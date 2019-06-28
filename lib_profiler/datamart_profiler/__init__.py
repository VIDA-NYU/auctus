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
    logger.info("Ranges: %r", ranges)

    # Convert to Elasticsearch syntax
    ranges = [{'range': {'gte': rg[0], 'lte': rg[1]}}
              for rg in ranges]
    return ranges


def get_spatial_ranges(values):
    """
    Retrieve the spatial ranges (i.e. bounding boxes) given the input gps points.

    This performs K-Means clustering, returning a maximum of 3 ranges.
    """

    logger.info("Computing spatial ranges, %d values", len(values))

    clustering = KMeans(n_clusters=min(N_RANGES, len(values)),
                        random_state=0)
    clustering.fit(values)
    logger.info("K-Means clusters: %r", clustering.cluster_centers_)

    # Compute confidence intervals for each range
    ranges = []
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
    logger.info("Ranges: %r", ranges)

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


@PROM_PROFILE.time()
def process_dataset(
        data,
        dataset_id=None,
        metadata=None,
        lazo_client=None,
        search=False):
    """Compute all metafeatures from a dataset.

    :param data: path to dataset
    :param dataset_id: id of the dataset
    :param metadata: The metadata provided by the discovery plugin (might be
        very limited).
    :param lazo_client: client for the Lazo Index Server
    :param search: True if this method is being called during the search
        operation (and not for indexing).
    """
    if metadata is None:
        metadata = {}

    # FIXME: SCDP currently disabled
    # scdp_out = run_scdp(data)
    scdp_out = {}

    data_path = None
    if isinstance(data, (str, bytes)):
        if not os.path.exists(data):
            raise ValueError("data file does not exist")

        # saving path
        if isinstance(data, str):
            data_path = data

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

    # Lat / Lon
    column_lat = []
    column_lon = []

    # Textual columns
    column_textual = []

    # Identify types
    logger.info("Identifying types...")
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

                # Get lat/lon columns
                if Type.LATITUDE in semantic_types_dict:
                    column_lat.append(
                        (column_meta['name'], numerical_values)
                    )
                elif Type.LONGITUDE in semantic_types_dict:
                    column_lon.append(
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

            if structural_type == Type.TEXT and \
                    Type.DATE_TIME not in semantic_types_dict:
                column_textual.append(column_meta['name'])

    # Textual columns
    if lazo_client and column_textual:
        # Indexing with lazo
        if not search:
            logger.info("Indexing textual data with Lazo...")
            try:
                if data_path:
                    # if we have the path, send the path
                    lazo_client.index_data_path(
                        data_path,
                        dataset_id,
                        column_textual
                    )
                else:
                    # if path is not available, send the data instead
                    for column_name in column_textual:
                        lazo_client.index_data(
                            data[column_name].values.tolist(),
                            dataset_id,
                            column_name
                        )
            except Exception as e:
                logger.warning('Error indexing textual attributes from %s', dataset_id)
                logger.warning(str(e))
        # Generating Lazo sketches for the search
        else:
            logger.info("Generating Lazo sketches...")
            try:
                if data_path:
                    # if we have the path, send the path
                    lazo_sketches = lazo_client.get_lazo_sketch_from_data_path(
                        data_path,
                        "",
                        column_textual
                    )
                else:
                    # if path is not available, send the data instead
                    lazo_sketches = []
                    for column_name in column_textual:
                        lazo_sketches.append(
                            lazo_client.get_lazo_sketch_from_data(
                                data[column_name].values.tolist(),
                                "",
                                column_name
                            )
                        )
                ## saving sketches into metadata
                metadata_lazo = []
                for i in range(len(column_textual)):
                    n_permutations, hash_values, cardinality =\
                        lazo_sketches[i]
                    metadata_lazo.append(dict(
                        name=column_textual[i],
                        n_permutations=n_permutations,
                        hash_values=list(hash_values),
                        cardinality=cardinality
                    ))
                metadata['lazo'] = metadata_lazo
            except Exception as e:
                logger.warning('Error getting Lazo sketches textual attributes from %s', dataset_id)
                logger.warning(str(e))

    # Lat / Lon
    logger.info("Computing spatial coverage...")
    with PROM_SPATIAL.time():
        spatial_coverage = []
        i_lat = i_lon = 0
        while i_lat < len(column_lat) and i_lon < len(column_lon):
            name_lat = column_lat[i_lat][0]
            name_lon = column_lon[i_lon][0]

            values_lat = column_lat[i_lat][1]
            values_lon = column_lon[i_lon][1]
            values = []
            for i in range(len(values_lat)):
                if values_lat[i] is not None and values_lon[i] is not None:
                    values.append((values_lat[i], values_lon[i]))

            if len(values) > 1:
                spatial_ranges = get_spatial_ranges(values)
                if spatial_ranges:
                    spatial_coverage.append({"lat": name_lat,
                                             "lon": name_lon,
                                             "ranges": spatial_ranges})

            i_lat += 1
            i_lon += 1

    if spatial_coverage:
        metadata['spatial_coverage'] = spatial_coverage

    # Return it -- it will be inserted into Elasticsearch, and published to the
    # feed and the waiting on-demand searches
    return metadata
