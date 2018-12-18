import codecs
import hdbscan
import json
import logging
import math
import numpy
import os
import pandas
import pkg_resources
import subprocess

from .identify_types import identify_types


logger = logging.getLogger(__name__)


MAX_SIZE = 50_000_000


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


def get_numerical_ranges(values):
    """
    Retrieve the numeral ranges given the input (timestamp, integer, or float).
    """

    if not values:
        return []

    values = sorted(values)

    range_diffs = []
    for i in range(1, len(values)):
        diff = values[i] - values[i-1]
        diff != 0 and range_diffs.append(diff)

    avg_range_diff, std_dev_range_diff = mean_stddev(range_diffs)

    ranges = []
    current_min = values[0]
    current_max = values[0]

    for i in range(1, len(values)):
        if (values[i] - values[i-1]) > avg_range_diff + 3*std_dev_range_diff:
            ranges.append({"range": {"gte": current_min, "lte": current_max}})
            current_min = values[i]
            current_max = values[i]
            continue
        current_max = values[i]
    ranges.append({"range": {"gte": current_min, "lte": current_max}})

    return ranges


def get_spatial_ranges(values):
    """
    Retrieve the spatial ranges (i.e. bounding boxes) given the input gps points.
    It uses HDBSCAN for finding finer spatial ranges.
    """

    clustering = hdbscan.HDBSCAN(min_cluster_size=10).fit(values)

    clusters = {}
    for i in range(len(values)):
        label = clustering.labels_[i]
        if label < 0:
            continue
        if label not in clusters:
            clusters[label] = [[float("inf"), -float("inf")], [float("inf"), -float("inf")]]
        clusters[label][0][0] = min(clusters[label][0][0], values[i][0])  # min lat
        clusters[label][0][1] = max(clusters[label][0][1], values[i][0])  # max lat

        clusters[label][1][0] = min(clusters[label][1][0], values[i][1])  # min lon
        clusters[label][1][1] = max(clusters[label][1][1], values[i][1])  # max lon

    ranges = []
    for v in clusters.values():
        if (v[0][0] != v[0][1]) and (v[1][0] != v[1][1]):
            ranges.append({"range": {"type": "envelope",
                                     "coordinates": [
                                         [v[1][0], v[0][1]],
                                         [v[1][1], v[0][0]]
                                     ]}})

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


def process_dataset(data, metadata=None):
    """Compute all metafeatures from a dataset.

    :param metadata: The metadata provided by the discovery plugin (might be
        very limited).
    """
    if metadata is None:
        metadata = {}

    scdp_out = run_scdp(data)

    if isinstance(data, (str, bytes)):
        if os.path.isdir(data):
            data = os.path.join(data, 'main.csv')
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

            ratio = metadata['size'] / MAX_SIZE
            logger.info("Loading dataframe, sample ratio=%r...", ratio)
            data = pandas.read_csv(data,
                                   dtype=str, na_filter=False,
                                   skiprows=lambda i: i != 0 and i > ratio)
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

    # Identify types
    logger.info("Identifying types...")
    for i, column_meta in enumerate(columns):
        array = data.iloc[:, i]
        structural_type, semantic_types_dict = \
            identify_types(array, column_meta['name'])
        # Set structural type
        column_meta['structural_type'] = structural_type
        # Add semantic types to the ones already present
        sem_types = column_meta.setdefault('semantic_types', [])
        for sem_type in semantic_types_dict:
            if sem_type not in sem_types:
                sem_types.append(sem_type)

        if structural_type in ('http://schema.org/Integer',
                               'http://schema.org/Float'):
            column_meta['mean'], column_meta['stddev'] = mean_stddev(array)

            # Get numerical ranges
            # logger.warning(" Column Name: " + column_meta['name'])
            numerical_values = []
            for e in array:
                try:
                    numerical_values.append(float(e))
                except ValueError:
                    numerical_values.append(None)

            # Get lat/lon columns
            if 'https://schema.org/latitude' in semantic_types_dict:
                column_lat.append(
                    (column_meta['name'], numerical_values)
                )
            elif 'https://schema.org/longitude' in semantic_types_dict:
                column_lon.append(
                    (column_meta['name'], numerical_values)
                )
            else:
                column_meta['coverage'] = get_numerical_ranges(
                    [x for x in numerical_values if x is not None]
                )

        if 'http://schema.org/DateTime' in semantic_types_dict:
            timestamps = numpy.empty(
                len(semantic_types_dict['http://schema.org/DateTime']),
                dtype='float32',
            )
            timestamps_for_range = []
            for j, dt in enumerate(
                    semantic_types_dict['http://schema.org/DateTime']):
                timestamps[j] = dt.timestamp()
                timestamps_for_range.append(
                    dt.replace(minute=0, second=0).timestamp()
                )
            column_meta['mean'], column_meta['stddev'] = \
                mean_stddev(timestamps)

            # Get temporal ranges
            column_meta['coverage'] = \
                get_numerical_ranges(timestamps_for_range)

    # Lat / Lon
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
                values.append([values_lat[i], values_lon[i]])

        spatial_coverage.append({"lat": name_lat,
                                 "lon": name_lon,
                                 "ranges": get_spatial_ranges(values)})

        i_lat += 1
        i_lon += 1

    if spatial_coverage:
        metadata['spatial_coverage'] = spatial_coverage

    # TODO: Compute histogram

    # Return it -- it will be inserted into Elasticsearch, and published to the
    # feed and the waiting on-demand searches
    return metadata
