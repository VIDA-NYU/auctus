import codecs
import json
import logging
import math
import numpy
import os
import pandas
import pkg_resources
import prometheus_client
import random
from sklearn.cluster import KMeans
import subprocess

from .identify_types import identify_types
from datamart_core.common import Type

logger = logging.getLogger(__name__)


MAX_SIZE = 50_000_000


BUCKETS = [0.5, 1.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0, 300.0, 600.0]

PROM_PROFILE = prometheus_client.Histogram('profile_seconds',
                                           "Profile time",
                                           buckets=BUCKETS)
PROM_TYPES = prometheus_client.Histogram('profile_types_seconds',
                                         "Profile types time",
                                         buckets=BUCKETS)
PROM_SPATIAL = prometheus_client.Histogram('profile_spatial_seconds',
                                           "Profile spatial coverage time",
                                           buckets=BUCKETS)


def mean_stddev(array):
    total = 0
    for elem in array:
        try:
            total += float(elem)
        except ValueError:
            pass
    mean = total / len(array)if len(array) > 0 else 0
    total = 0
    for elem in array:
        try:
            elem = float(elem) - mean
        except ValueError:
            continue
        total += elem * elem
    stddev = math.sqrt(total / len(array)) if len(array) > 0 else 0

    return mean, stddev


N_RANGES = 3


def get_numerical_ranges(values):
    """
    Retrieve the numeral ranges given the input (timestamp, integer, or float).

    This performs K-Means clustering, returning a maximum of 3 ranges.
    """

    if not values:
        return []

    logger.info("Computing numerical ranges, %d values", len(values))

    clustering = KMeans(n_clusters=min(N_RANGES, len(values)),
                        random_state=0)
    clustering.fit(numpy.array(values).reshape(-1, 1))
    logger.info("K-Means clusters: %r", clustering.cluster_centers_)

    # Compute confidence intervals for each range
    ranges = []
    sizes = []
    for rg in range(N_RANGES):
        cluster = [values[i]
                   for i in range(len(values))
                   if clustering.labels_[i] == rg]
        if not cluster:
            continue
        cluster.sort()
        min_idx = int(0.05 * len(cluster))
        max_idx = int(0.95 * len(cluster))
        ranges.append([
            cluster[min_idx],
            cluster[max_idx],
        ])
        sizes.append(len(cluster))
    logger.info("Ranges: %r", ranges)
    logger.info("Sizes: %r", sizes)

    # Convert to Elasticsearch syntax
    ranges = [{'range': {'gte': rg[0], 'lte': rg[1]}}
              for rg in ranges]
    return ranges


def get_spatial_ranges(values):
    """
    Retrieve the spatial ranges (i.e. bounding boxes) given the input gps points.

    This performs K-Means clustering, returning a maximum of 3 ranges.
    """

    clustering = KMeans(n_clusters=min(N_RANGES, len(values)),
                        random_state=0)
    clustering.fit(values)
    logger.info("K-Means clusters: %r", clustering.cluster_centers_)

    # Compute confidence intervals for each range
    ranges = []
    sizes = []
    for rg in range(N_RANGES):
        cluster = [values[i]
                   for i in range(len(values))
                   if clustering.labels_[i] == rg]
        if not cluster:
            continue
        cluster.sort(key=lambda p: p[0])
        min_idx = int(0.05 * len(cluster))
        max_idx = int(0.95 * len(cluster))
        min_lat = cluster[min_idx][0]
        max_lat = cluster[max_idx][0]
        cluster.sort(key=lambda p: p[1])
        min_long = cluster[min_idx][1]
        max_long = cluster[max_idx][1]
        ranges.append([
            [min_long, max_lat],
            [max_long, min_lat],
        ])
        sizes.append(len(cluster))
    logger.info("Ranges: %r", ranges)
    logger.info("Sizes: %r", sizes)

    # TODO: Deal with clusters made of outliers

    # Convert to Elasticsearch syntax
    ranges = [{'range': {'type': 'envelope',
                         'coordinates': coords}}
              for coords in ranges]
    return ranges


def run_scdp(data):
    # Run SCDP
    logger.info("Running SCDP...")
    scdp = pkg_resources.resource_filename('datamart_profiler', 'scdp.jar')
    if isinstance(data, (str, bytes)):
        if os.path.isdir(data):
            data = os.path.join(data, 'main.csv')
        if not os.path.exists(data):
            raise ValueError("data file does not exist")
        proc = subprocess.Popen(['java', '-jar', scdp, data],
                                stdout=subprocess.PIPE,
                                stdin=subprocess.PIPE)
        stdout, _ = proc.communicate()
    else:
        proc = subprocess.Popen(['java', '-jar', scdp, '/dev/stdin'],
                                stdout=subprocess.PIPE,
                                stdin=subprocess.PIPE)
        data.to_csv(codecs.getwriter('utf-8')(proc.stdin))
        stdout, _ = proc.communicate()
    if proc.wait() != 0:
        logger.error("Error running SCDP: returned %d", proc.returncode)
        return {}
    else:
        try:
            return json.loads(stdout)
        except json.JSONDecodeError:
            logger.exception("Invalid output from SCDP")
            return {}


def normalize_latlong_column_name(name, *substrings):
    name = name.lower()
    for substr in substrings:
        idx = name.find(substr)
        if idx >= 0:
            name = name[:idx] + name[idx + len(substr):]
            break
    return name


def pair_latlong_columns(columns_lat, columns_long):
    # Normalize latitude column names
    normalized_lat = {}
    for i, (name, values_lat) in enumerate(columns_lat):
        name = normalize_latlong_column_name(name, 'latitude', 'lat')
        normalized_lat[name] = i

    # Go over normalized longitude column names and try to match
    pairs = []
    missed_long = []
    for name, values_long in columns_long:
        norm_name = normalize_latlong_column_name(name, 'longitude', 'long')
        if norm_name in normalized_lat:
            pairs.append((columns_lat[normalized_lat.pop(norm_name)],
                          (name, values_long)))
        else:
            missed_long.append(name)

    # Gather missed columns and log them
    missed_lat = [columns_lat[i][0] for i in sorted(normalized_lat.values())]
    if missed_lat:
        logger.warning("Unmatched latitude columns: %r", missed_lat)
    if missed_long:
        logger.warning("Unmatched longitude columns: %r", missed_long)

    return pairs


@PROM_PROFILE.time()
def process_dataset(data, metadata=None):
    """Compute all metafeatures from a dataset.

    :param metadata: The metadata provided by the discovery plugin (might be
        very limited).
    """
    if metadata is None:
        metadata = {}

    # FIXME: SCDP currently disabled
    # scdp_out = run_scdp(data)
    scdp_out = {}

    if isinstance(data, (str, bytes)):
        if not os.path.exists(data):
            raise ValueError("data file does not exist")

        # File size
        metadata['size'] = os.path.getsize(data)
        logger.info("File size: %r bytes", metadata['size'])

        # Sub-sample
        if metadata['size'] > MAX_SIZE:
            logger.info("Counting rows...")
            with open(data, 'rb') as fp:
                metadata['nb_rows'] = sum(1 for _ in fp)

            ratio = MAX_SIZE / metadata['size']
            logger.info("Loading dataframe, sample ratio=%r...", ratio)
            data = pandas.read_csv(
                data,
                dtype=str, na_filter=False,
                skiprows=lambda i: i != 0 and random.random() > ratio)
        else:
            logger.info("Loading dataframe...")
            data = pandas.read_csv(data,
                                   dtype=str, na_filter=False)

            metadata['nb_rows'] = data.shape[0]

        logger.info("Dataframe loaded, %d rows, %d columns",
                    data.shape[0], data.shape[1])
    else:
        if not isinstance(data, pandas.DataFrame):
            raise TypeError("data should be a filename or a pandas.DataFrame")
        metadata['nb_rows'] = len(data)

    # Get column dictionary
    columns = metadata.setdefault('columns', [])
    # Fix size if wrong
    if len(columns) != len(data.columns):
        logger.info("Setting column names from header")
        columns[:] = [{} for _ in range(len(data.columns))]
    else:
        logger.info("Keeping columns from discoverer")

    # Set column names
    for column_meta, name in zip(columns, data.columns):
        column_meta['name'] = name

    # Copy info from SCDP
    for column_meta, name in zip(columns, data.columns):
        column_meta.update(scdp_out.get(name, {}))

    # Lat / Long
    columns_lat = []
    columns_long = []

    with PROM_TYPES.time():
        for i, column_meta in enumerate(columns):
            logger.info("Processing column %d...", i)
            array = data.iloc[:, i]
            # Identify types
            structural_type, semantic_types_dict = \
                identify_types(array, column_meta['name'])
            # Set structural type
            column_meta['structural_type'] = structural_type
            # Add semantic types to the ones already present
            sem_types = column_meta.setdefault('semantic_types', [])
            for sem_type in semantic_types_dict:
                if sem_type not in sem_types:
                    sem_types.append(sem_type)

            # Compute ranges for numerical/spatial data
            if structural_type in (Type.INTEGER, Type.FLOAT):
                column_meta['mean'], column_meta['stddev'] = mean_stddev(array)

                # Get numerical ranges
                numerical_values = []
                for e in array:
                    try:
                        numerical_values.append(float(e))
                    except ValueError:
                        numerical_values.append(None)

                # Get lat/long columns
                if Type.LATITUDE in semantic_types_dict:
                    columns_lat.append(
                        (column_meta['name'], numerical_values)
                    )
                elif Type.LONGITUDE in semantic_types_dict:
                    columns_long.append(
                        (column_meta['name'], numerical_values)
                    )
                else:
                    column_meta['coverage'] = get_numerical_ranges(
                        [x for x in numerical_values if x is not None]
                    )

            # Compute ranges for temporal data
            if Type.DATE_TIME in semantic_types_dict:
                timestamps = numpy.empty(
                    len(semantic_types_dict[Type.DATE_TIME]),
                    dtype='float32',
                )
                timestamps_for_range = []
                for j, dt in enumerate(
                        semantic_types_dict[Type.DATE_TIME]):
                    timestamps[j] = dt.timestamp()
                    timestamps_for_range.append(
                        dt.replace(minute=0, second=0).timestamp()
                    )
                column_meta['mean'], column_meta['stddev'] = \
                    mean_stddev(timestamps)

                # Get temporal ranges
                column_meta['coverage'] = \
                    get_numerical_ranges(timestamps_for_range)

    # Lat / Long
    logger.info("Computing spatial coverage...")
    with PROM_SPATIAL.time():
        spatial_coverage = []
        pairs = pair_latlong_columns(columns_lat, columns_long)
        for (name_lat, values_lat), (name_long, values_long) in pairs:
            values = []
            for i in range(len(values_lat)):
                if values_lat[i] and values_long[i]:  # Ignore None and 0
                    values.append((values_lat[i], values_long[i]))

            if len(values) > 1:
                logger.info("Computing spatial ranges %r,%r (%d rows)",
                            name_lat, name_long, len(values))
                spatial_ranges = get_spatial_ranges(values)
                if spatial_ranges:
                    spatial_coverage.append({"lat": name_lat,
                                             "lon": name_long,
                                             "ranges": spatial_ranges})

    if spatial_coverage:
        metadata['spatial_coverage'] = spatial_coverage

    # Return it -- it will be inserted into Elasticsearch, and published to the
    # feed and the waiting on-demand searches
    return metadata
