import codecs
import collections
import contextlib
import csv
from datetime import datetime
import itertools
import logging
import math
import numpy
import opentelemetry.trace
import os
import pandas
from pandas.errors import EmptyDataError
import prometheus_client
import string
import time
import random
import re
import warnings

from .numerical import mean_stddev, get_numerical_ranges
from .profile_types import identify_types, determine_dataset_type
from .spatial import LatLongColumn, Geohasher, nominatim_resolve_all, \
    pair_latlong_columns, get_spatial_ranges, parse_wkt_column
from .temporal import get_temporal_resolution
from . import types


logger = logging.getLogger(__name__)
tracer = opentelemetry.trace.get_tracer(__name__)


RANDOM_SEED = 89

MAX_SIZE = 5000000  # 5 MB
SAMPLE_ROWS = 20

MAX_UNCLEAN_ADDRESSES = 0.20  # 20%


MAX_SKIPPED_ROWS = 6
"""Maximum number of rows to discard at the top of the file"""

HEADER_CONSISTENT_ROWS = 4
"""Stop throwing out lines when that many in a row have same number of columns
"""

MAX_GEOHASHES = 100


BUCKETS = [
    1.0, 2.0, 4.0, 7.0, 12.0, 20.0, 32.0, 52.0, 80.0, 120.0, 190.0,
    300.0, 480.0, 720.0, 1200.0, 1800.0, 3600.0, 5760.0, 7200.0,
    float('inf'),
]

PROM_PROFILE = prometheus_client.Histogram(
    'profile_seconds', "Profile time",
    buckets=BUCKETS,
)
PROM_TYPES = prometheus_client.Histogram(
    'profile_types_seconds', "Profile types time",
    buckets=BUCKETS,
)
PROM_SPATIAL = prometheus_client.Histogram(
    'profile_spatial_seconds', "Profile spatial coverage time",
    buckets=BUCKETS,
)
PROM_LAZO = prometheus_client.Histogram(
    'profile_lazo_seconds', "Profile time with Lazo, time",
    buckets=BUCKETS,
)


_re_word_split = re.compile(r'\W+')


csv.field_size_limit(2097152)  # Default 131072


def truncate_string(s, limit=140):
    """Truncate a string, replacing characters over the limit with "...".
    """
    if len(s) <= limit:
        return s
    else:
        # Try to find a space
        space = s.rfind(' ', limit - 20, limit - 3)
        if space == -1:
            return s[:limit - 3] + "..."
        else:
            return s[:space] + "..."


DELIMITERS = set(string.punctuation) | set(string.whitespace)
UPPER = set(string.ascii_uppercase)
LOWER = set(string.ascii_lowercase)


def expand_attribute_name(name):
    """Expand an attribute names to keywords derived from it.
    """
    name = name.replace('_', ' ').replace('-', ' ')

    word = []
    for c in name:
        if c in DELIMITERS:
            if word:
                yield ''.join(word)
                word = []
            continue

        if word:
            if (
                (word[-1] in string.digits) != (c in string.digits)
                or (word[-1] in LOWER and c in UPPER)
            ):
                yield ''.join(word)
                word = []

        word.append(c)

    yield ''.join(word)


def _lazo_retry(func):
    from lazo_index_service.errors import LazoError
    try:
        return func()
    except LazoError:
        pass
    return func()


def count_rows_to_skip(file):
    """Count non-data rows at the top, such as titles etc.
    """
    # Check whether this is a binary file
    read_sample = file.read(4)
    binary = not isinstance(read_sample, str)
    file.seek(0, 0)

    # Decode CSV
    if binary:
        codec_reader = codecs.getreader('utf-8')(file)
        reader = csv.reader(codec_reader)
    else:
        reader = csv.reader(file)

    # Read rows until the number of items stabilizes
    run_start = 0
    run_cols = None
    run_len = 0
    try:
        for i, row in enumerate(itertools.islice(reader, MAX_SKIPPED_ROWS + HEADER_CONSISTENT_ROWS)):
            if i >= MAX_SKIPPED_ROWS + HEADER_CONSISTENT_ROWS:
                raise ValueError("Can't find consistent CSV data in file")
            if len(row) == run_cols:
                # Number of columns matches with run
                run_len += 1
                if run_len == HEADER_CONSISTENT_ROWS:
                    # 4 rows with the same size, assume we're good
                    return run_start
            else:
                # Number of columns doesn't match, start new run
                run_start = i
                run_cols = len(row)
                run_len = 1

        # Reached the end of the file
        return run_start
    finally:
        file.seek(0, 0)


def load_data(data, load_max_size=None, indexes=True):
    metadata = {}

    if isinstance(data, pandas.DataFrame):
        if load_max_size is not None:
            warnings.warn(
                "load_max_size is set but ignored since the data was already "
                + "loaded and provided as a DataFrame",
                UserWarning,
            )

        # Turn indexes into regular columns
        if (
            indexes and (
                data.index.dtype != numpy.int64
                or not pandas.Index(numpy.arange(len(data))).equals(data.index)
            )
        ):
            data = data.reset_index()

        metadata['nb_rows'] = len(data)
        # Change to object dtype first and do fillna() to work around bug
        # https://github.com/pandas-dev/pandas/issues/25353 (nan as str 'nan')
        data = data.astype(object).fillna('').astype(str)

        column_names = data.columns
    else:
        if not load_max_size:
            load_max_size = MAX_SIZE

        column_names = None  # Avoids a warning
        with contextlib.ExitStack() as stack:
            if isinstance(data, (str, bytes)):
                if not os.path.exists(data):
                    raise ValueError("data file does not exist")

                # File size
                metadata['size'] = os.path.getsize(data)
                logger.info("File size: %r bytes", metadata['size'])

                data = stack.enter_context(open(data, 'rb'))
            elif hasattr(data, 'read'):
                # Get size by seeking to the end
                data.seek(0, 2)
                metadata['size'] = data.tell()
                data.seek(0, 0)
            else:
                raise TypeError("data should be a filename, a file object, or "
                                "a pandas.DataFrame")

            # Read column names
            read_sample = data.read(4)
            data.seek(0, 0)
            if isinstance(read_sample, str):
                reader = csv.reader(data)
                try:
                    column_names = next(reader)
                except StopIteration:
                    column_names = None
                del reader
            else:
                codec_reader = codecs.getreader('utf-8')(data)
                reader = csv.reader(codec_reader)
                try:
                    column_names = next(reader)
                except StopIteration:
                    column_names = None
                del reader
                del codec_reader
            data.seek(0, 0)

            # Load the data
            if metadata['size'] > load_max_size:
                logger.info("Counting rows...")
                metadata['nb_rows'] = sum(1 for _ in data)
                if metadata['nb_rows'] > 0:
                    metadata['average_row_size'] = (
                        metadata['size'] / metadata['nb_rows']
                    )
                data.seek(0, 0)

                # Sub-sample
                ratio = load_max_size / metadata['size']
                logger.info("Loading dataframe, sample ratio=%r...", ratio)
                rand = random.Random(RANDOM_SEED)
                selected_rows = set(rand.sample(
                    range(1, metadata['nb_rows']),
                    math.ceil(ratio * (metadata['nb_rows'] - 1)),
                ))
                selected_rows.add(0)  # Always get the header
                data = pandas.read_csv(
                    data,
                    dtype=str, na_filter=False,
                    skiprows=lambda i: i not in selected_rows,
                )
            else:
                logger.info("Loading dataframe...")
                data = pandas.read_csv(data,
                                       dtype=str, na_filter=False)

                metadata['nb_rows'] = data.shape[0]
                if metadata['nb_rows'] > 0:
                    metadata['average_row_size'] = (
                        metadata['size'] / metadata['nb_rows']
                    )

            logger.info("Dataframe loaded, %d rows, %d columns",
                        data.shape[0], data.shape[1])

    return data, metadata, column_names


def process_column(
    array, column_meta,
    *,
    manual=None,
    plots=True,
    coverage=True,
    geo_data=None,
    nominatim=None,
):
    # Identify types
    with tracer.start_as_current_span('profile/identify_types'):
        structural_type, semantic_types_dict, additional_meta = \
            identify_types(array, column_meta['name'], geo_data, manual)
    logger.info(
        "Column type %s [%s]",
        structural_type,
        ', '.join(semantic_types_dict),
    )

    # Set structural type
    column_meta['structural_type'] = structural_type
    # Add semantic types to the ones already present
    sem_types = column_meta.setdefault('semantic_types', [])
    for sem_type in semantic_types_dict:
        if sem_type not in sem_types:
            sem_types.append(sem_type)
    # Insert additional metadata
    column_meta.update(additional_meta)

    # Resolved values are returned so they can be used again to compute spatial
    # coverage
    resolved = {}

    # Compute ranges for numerical data
    if (
        structural_type in (types.INTEGER, types.FLOAT)
        and (coverage or plots)
    ):
        # Get numerical values needed for either ranges or plot
        with tracer.start_as_current_span('profile/parse_numerical_values'):
            numerical_values = []
            for e in array:
                try:
                    e = float(e)
                except ValueError:
                    pass
                else:
                    if -3.4e38 < e < 3.4e38:  # Overflows in ES
                        numerical_values.append(e)

        # Compute ranges from numerical values
        if coverage:
            with tracer.start_as_current_span('profile/numerical_ranges'):
                column_meta['mean'], column_meta['stddev'] = \
                    mean_stddev(numerical_values)

                ranges = get_numerical_ranges(numerical_values)
                if ranges:
                    column_meta['coverage'] = ranges

        # Compute histogram from numerical values
        if plots:
            with tracer.start_as_current_span('profile/numerical_plot'):
                counts, edges = numpy.histogram(
                    numerical_values,
                    bins=10,
                )
                counts = [int(i) for i in counts]
                edges = [float(f) for f in edges]
                column_meta['plot'] = {
                    "type": "histogram_numerical",
                    "data": [
                        {
                            "count": count,
                            "bin_start": edges[i],
                            "bin_end": edges[i + 1],
                        }
                        for i, count in enumerate(counts)
                    ]
                }

    if types.DATE_TIME in semantic_types_dict:
        datetimes = semantic_types_dict[types.DATE_TIME]
        resolved['datetimes'] = datetimes
        timestamps = numpy.empty(
            len(datetimes),
            dtype='float32',
        )
        for j, dt in enumerate(datetimes):
            timestamps[j] = dt.timestamp()
        resolved['timestamps'] = timestamps

        # Compute histogram from temporal values
        if plots and 'plot' not in column_meta:
            with tracer.start_as_current_span('profile/temporal_plot'):
                counts, edges = numpy.histogram(timestamps, bins=10)
                counts = [int(i) for i in counts]
                column_meta['plot'] = {
                    "type": "histogram_temporal",
                    "data": [
                        {
                            "count": count,
                            "date_start": datetime.utcfromtimestamp(
                                float(edges[i]),
                            ).isoformat(),
                            "date_end": datetime.utcfromtimestamp(
                                float(edges[i + 1]),
                            ).isoformat(),
                        }
                        for i, count in enumerate(counts)
                    ]
                }

    # Compute histogram from categorical values
    if plots and types.CATEGORICAL in semantic_types_dict:
        with tracer.start_as_current_span('profile/categorical_plot'):
            counter = collections.Counter()
            for value in array:
                if not value:
                    continue
                counter[value] += 1
            counts = counter.most_common(5)
            counts = sorted(counts)
            column_meta['plot'] = {
                "type": "histogram_categorical",
                "data": [
                    {
                        "bin": value,
                        "count": count,
                    }
                    for value, count in counts
                ]
            }

    # Compute histogram from textual values
    if (
        plots and types.TEXT in semantic_types_dict and
        'plot' not in column_meta
    ):
        with tracer.start_as_current_span('profile/textual_plot'):
            counter = collections.Counter()
            for value in array:
                for word in _re_word_split.split(value):
                    word = word.lower()
                    if word:
                        counter[word] += 1
            counts = counter.most_common(5)
            column_meta['plot'] = {
                "type": "histogram_text",
                "data": [
                    {
                        "bin": value,
                        "count": count,
                    }
                    for value, count in counts
                ]
            }

    # Resolve addresses into coordinates
    if (
        nominatim is not None and
        structural_type == types.TEXT and
        types.TEXT in semantic_types_dict and
        types.ADMIN not in semantic_types_dict
    ):
        with tracer.start_as_current_span('profile/nominatim'):
            locations, non_empty = nominatim_resolve_all(
                nominatim,
                array,
            )
        if non_empty > 0:
            unclean_ratio = 1.0 - len(locations) / non_empty
            if unclean_ratio <= MAX_UNCLEAN_ADDRESSES:
                resolved['addresses'] = locations
                if types.ADDRESS not in column_meta['semantic_types']:
                    column_meta['semantic_types'].append(types.ADDRESS)

    # Set level of administrative areas
    if types.ADMIN in semantic_types_dict:
        level, areas = semantic_types_dict[types.ADMIN]
        if level is not None:
            column_meta['admin_area_level'] = level
        resolved['admin_areas'] = areas

    return resolved


@PROM_LAZO.time()
def lazo_index_data(
    data,
    dataset_id,
    columns_textual, column_textual_names,
    lazo_client,
):
    logger.info("Indexing textual data with Lazo...")
    start = time.perf_counter()
    for idx, name in zip(columns_textual, column_textual_names):
        def call_lazo():
            lazo_client.index_data(
                data.iloc[:, idx].values.tolist(),
                dataset_id,
                name,
            )

        _lazo_retry(call_lazo)
    logger.info(
        "Indexing with Lazo took %.2fs seconds",
        time.perf_counter() - start,
    )


@PROM_LAZO.time()
def get_lazo_data_sketch(
    data,
    columns_textual, column_textual_names,
    lazo_client,
):
    logger.info("Sketching textual data with Lazo...")
    start = time.perf_counter()
    lazo_sketches = []
    for idx, name in zip(columns_textual, column_textual_names):
        def call_lazo():
            return lazo_client.get_lazo_sketch_from_data(
                data.iloc[:, idx].values.tolist(),
                "",
                name,
            )

        lazo_sketches.append(_lazo_retry(call_lazo))
    logger.info(
        "Sketching with Lazo took %.2fs seconds",
        time.perf_counter() - start,
    )
    return lazo_sketches


@PROM_PROFILE.time()
def process_dataset(data, dataset_id=None, metadata=None,
                    lazo_client=None, nominatim=None, geo_data=None,
                    search=False, include_sample=False,
                    coverage=True, plots=False, indexes=True,
                    load_max_size=None,
                    **kwargs):
    """Compute all metafeatures from a dataset.

    :param data: path to dataset, or file object, or DataFrame
    :param dataset_id: id of the dataset
    :param metadata: The metadata provided by the discovery plugin (might be
        very limited).
    :param lazo_client: client for the Lazo Index Server
    :param nominatim: URL of the Nominatim server
    :param geo_data: ``True`` or a datamart_geo.GeoData instance to use to
        resolve named administrative territorial entities
    :param search: True if this method is being called during the search
        operation (and not for indexing).
    :param include_sample: Set to True to include a few random rows to the
        result. Useful to present to a user.
    :param coverage: Whether to compute data ranges
    :param plots: Whether to compute plots
    :param indexes: Whether to include indexes. If True (the default), the
        input is a DataFrame, and it has index(es) different from the default
        range, they will appear in the result with the columns.
    :param load_max_size: Target size of the data to be analyzed. The data will
        be randomly sampled if it is bigger. Defaults to `MAX_SIZE`, currently
        5 MB. This is different from the sample data included in the result.
    :return: JSON structure (dict)
    """
    if 'sample_size' in kwargs:
        warnings.warn(
            "Argument 'sample_size' is deprecated, use 'load_max_size'",
            DeprecationWarning,
        )
        load_max_size = kwargs.pop('sample_size')
    if kwargs:
        raise TypeError(
            "process_dataset() got unexpected keyword argument %r" %
            next(iter(kwargs))
        )

    if geo_data is True:
        from datamart_geo import GeoData

        geo_data = GeoData.from_local_cache()

    if metadata is None:
        metadata = {}

    # Load or prepare data for processing
    try:
        data, file_metadata, column_names = load_data(
            data,
            load_max_size=load_max_size,
            indexes=indexes,
        )
    except EmptyDataError:
        logger.warning("Dataframe is empty!")
        metadata['nb_rows'] = 0
        metadata['nb_profiled_rows'] = 0
        metadata['columns'] = []
        metadata['types'] = []
        return metadata
    metadata.update(file_metadata)
    metadata['nb_profiled_rows'] = data.shape[0]
    metadata['nb_columns'] = data.shape[1]

    if 'columns' in metadata:
        columns = metadata['columns']
        logger.info("Using provided columns info")
        if len(columns) != len(data.columns):
            raise ValueError("Column metadata doesn't match number of columns")
        for column_meta, name in zip(columns, column_names):
            if 'name' in column_meta and column_meta['name'] != name:
                raise ValueError("Column names don't match")
            column_meta['name'] = name
    else:
        logger.info("Setting column names from header")
        columns = [{'name': name} for name in column_names]
        metadata['columns'] = columns

    if data.shape[0] == 0:
        logger.info("0 rows, returning early")
        metadata['types'] = []
        return metadata

    # Get manual updates from the user
    manual_columns = {}
    if 'manual_annotations' in metadata:
        if 'columns' in metadata['manual_annotations']:
            manual_columns = {
                col['name']: col
                for col in metadata['manual_annotations']['columns']
            }

    # Cache some values that have been resolved for type identification but are
    # useful for spatial coverage computation: admin areas and addresses
    # Having to resolve them once to see if they're valid and a second time to
    # build coverage information would be too slow
    resolved_columns = {}

    # Identify types
    logger.info("Identifying types, %d columns...", len(columns))
    with PROM_TYPES.time():
        with tracer.start_as_current_span('profile/columns'):
            for column_idx, column_meta in enumerate(columns):
                name = column_meta['name']
                with tracer.start_as_current_span('profile/column', attributes={'idx': column_idx, 'name': name}):
                    logger.info("Processing column %d %r...", column_idx, name)
                    array = data.iloc[:, column_idx]
                    if name in manual_columns:
                        manual = manual_columns[name]
                    else:
                        manual = None
                    # Process the column, updating the column_meta dict
                    resolved_columns[column_idx] = process_column(
                        array, column_meta,
                        manual=manual,
                        plots=plots,
                        coverage=coverage,
                        geo_data=geo_data,
                        nominatim=nominatim,
                    )

    # Textual columns
    columns_textual = [
        col_idx
        for col_idx, col in enumerate(columns)
        if (
            col['structural_type'] == types.TEXT
            and types.DATE_TIME not in col['semantic_types']
        )
    ]
    if lazo_client and columns_textual:
        with tracer.start_as_current_span('profile/categorical'):
            # Indexing with lazo
            column_textual_names = [columns[idx]['name'] for idx in columns_textual]
            if not search:
                try:
                    lazo_index_data(
                        data,
                        dataset_id,
                        columns_textual, column_textual_names,
                        lazo_client,
                    )
                except Exception:
                    logger.warning("Error indexing textual attributes from %s", dataset_id)
                    raise
            else:
                try:
                    lazo_sketches = get_lazo_data_sketch(
                        data,
                        columns_textual, column_textual_names,
                        lazo_client,
                    )
                except Exception:
                    logger.warning("Error getting Lazo sketches")
                    raise
                else:
                    # saving sketches into metadata
                    for sketch, idx in zip(lazo_sketches, columns_textual):
                        n_permutations, hash_values, cardinality = sketch
                        columns[idx]['lazo'] = dict(
                            n_permutations=n_permutations,
                            hash_values=list(hash_values),
                            cardinality=cardinality,
                        )

    # Pair lat & long columns
    columns_lat = [
        LatLongColumn(
            index=col_idx,
            name=col['name'],
            annot_pair=manual_columns.get(col['name'], {}).get('latlong_pair'),
        )
        for col_idx, col in enumerate(columns)
        if types.LATITUDE in col['semantic_types']
    ]
    columns_long = [
        LatLongColumn(
            index=col_idx,
            name=col['name'],
            annot_pair=manual_columns.get(col['name'], {}).get('latlong_pair'),
        )
        for col_idx, col in enumerate(columns)
        if types.LONGITUDE in col['semantic_types']
    ]
    latlong_pairs, (missed_lat, missed_long) = \
        pair_latlong_columns(columns_lat, columns_long)

    # Log missed columns
    if missed_lat:
        logger.warning("Unmatched latitude columns: %r", missed_lat)
    if missed_long:
        logger.warning("Unmatched longitude columns: %r", missed_long)

    # Remove semantic type from unpaired columns
    for col in columns:
        if col['name'] in missed_lat:
            col['semantic_types'].remove(types.LATITUDE)
        if col['name'] in missed_long:
            col['semantic_types'].remove(types.LONGITUDE)

    # Identify the overall dataset types (numerical, categorical, spatial, or temporal)
    dataset_types = collections.Counter()
    for column_meta in columns:
        dataset_type = determine_dataset_type(
            column_meta['structural_type'],
            column_meta['semantic_types'],
        )
        if dataset_type:
            dataset_types[dataset_type] += 1
    for key, d_type in [
        ('nb_spatial_columns', types.DATASET_SPATIAL),
        ('nb_temporal_columns', types.DATASET_TEMPORAL),
        ('nb_categorical_columns', types.DATASET_CATEGORICAL),
        ('nb_numerical_columns', types.DATASET_NUMERICAL),
    ]:
        if dataset_types[d_type]:
            metadata[key] = dataset_types[d_type]
    metadata['types'] = sorted(set(dataset_types))

    if coverage:
        logger.info("Computing spatial coverage...")
        spatial_coverage = []
        with PROM_SPATIAL.time():
            with tracer.start_as_current_span('profile/spatial_coverage'):
                # Compute sketches from lat/long pairs
                for col_lat, col_long in latlong_pairs:
                    lat_values = data.iloc[:, col_lat.index]
                    lat_values = pandas.to_numeric(lat_values, errors='coerce')
                    long_values = data.iloc[:, col_long.index]
                    long_values = pandas.to_numeric(long_values, errors='coerce')
                    mask = (
                        ~numpy.isnan(lat_values)
                        & ~numpy.isnan(long_values)
                        & (-90.0 < lat_values) & (lat_values < 90.0)
                        & (-180.0 < long_values) & (long_values < 180.0)
                    )

                    if mask.any():
                        lat_values = lat_values[mask]
                        long_values = long_values[mask]
                        values = numpy.array([lat_values, long_values]).T
                        logger.info(
                            "Computing spatial sketch lat=%r long=%r (%d rows)",
                            col_lat.name, col_long.name, len(values),
                        )
                        # Ranges
                        spatial_ranges = get_spatial_ranges(values)
                        # Geohashes
                        builder = Geohasher(number=MAX_GEOHASHES)
                        builder.add_points(values)
                        hashes = builder.get_hashes_json()

                        spatial_coverage.append({
                            'type': 'latlong',
                            'column_names': [col_lat.name, col_long.name],
                            'column_indexes': [
                                col_lat.index,
                                col_long.index,
                            ],
                            'geohashes4': hashes,
                            'ranges': spatial_ranges,
                            'number': len(values),
                        })

                # Compute sketches from WKT points
                for i, col in enumerate(columns):
                    if col['structural_type'] != types.GEO_POINT:
                        continue
                    latlong = col.get('point_format') == 'lat,long'
                    name = col['name']
                    values = parse_wkt_column(
                        data.iloc[:, i],
                        latlong=latlong,
                    )
                    total = numpy.sum(data.iloc[:, i].apply(lambda x: bool(x)))
                    if len(values) < 0.5 * total:
                        logger.warning(
                            "Most data points did not parse correctly as "
                            "point (%s) col=%d %r",
                            'lat,long' if latlong else 'long,lat',
                            i, col,
                        )
                    if values:
                        logger.info(
                            "Computing spatial sketches point=%r (%d rows)",
                            name, len(values),
                        )
                        # Ranges
                        spatial_ranges = get_spatial_ranges(values)
                        # Geohashes
                        builder = Geohasher(number=MAX_GEOHASHES)
                        builder.add_points(values)
                        hashes = builder.get_hashes_json()

                        spatial_coverage.append({
                            'type': 'point_latlong' if latlong else 'point',
                            'column_names': [name],
                            'column_indexes': [i],
                            'geohashes4': hashes,
                            'ranges': spatial_ranges,
                            'number': len(values),
                        })

                for idx, resolved in resolved_columns.items():
                    # Compute sketches from addresses
                    if 'addresses' in resolved:
                        locations = resolved['addresses']

                        name = columns[idx]['name']
                        logger.info(
                            "Computing spatial sketches address=%r (%d rows)",
                            name, len(locations),
                        )
                        # Ranges
                        spatial_ranges = get_spatial_ranges(locations)
                        # Geohashes
                        builder = Geohasher(number=MAX_GEOHASHES)
                        builder.add_points(locations)
                        hashes = builder.get_hashes_json()

                        spatial_coverage.append({
                            'type': 'address',
                            'column_names': [name],
                            'column_indexes': [idx],
                            'geohashes4': hashes,
                            'ranges': spatial_ranges,
                            'number': len(locations),
                        })

                    # Compute sketches from administrative areas
                    if 'admin_areas' in resolved:
                        areas = resolved['admin_areas']

                        name = columns[idx]['name']
                        logger.info(
                            "Computing spatial sketches admin_areas=%r (%d rows)",
                            name, len(areas),
                        )
                        cov = {
                            'type': 'admin',
                            'column_names': [name],
                            'column_indexes': [idx],
                        }

                        # Merge into a single range
                        merged = None
                        for area in areas:
                            if area is None:
                                continue
                            new = area.bounds
                            if new:
                                if merged is None:
                                    merged = new
                                else:
                                    merged = (
                                        min(merged[0], new[0]),
                                        max(merged[1], new[1]),
                                        min(merged[2], new[2]),
                                        max(merged[3], new[3]),
                                    )
                        if (
                            merged is not None
                            and merged[1] - merged[0] > 0.01
                            and merged[3] - merged[2] > 0.01
                        ):
                            logger.info("Computed bounding box")
                            cov['ranges'] = [
                                {
                                    'range': {
                                        'type': 'envelope',
                                        'coordinates': [
                                            [merged[0], merged[3]],
                                            [merged[1], merged[2]],
                                        ],
                                    },
                                },
                            ]
                        else:
                            logger.info("Couldn't build a bounding box")

                        # Compute geohashes
                        builder = Geohasher(number=MAX_GEOHASHES)
                        for area in areas:
                            if area is None or not area.bounds:
                                continue
                            builder.add_aab(area.bounds)
                        hashes = builder.get_hashes_json()
                        if hashes:
                            cov['geohashes4'] = hashes

                        # Count
                        cov['number'] = builder.total

                        if 'ranges' in cov or 'geohashes4' in cov:
                            spatial_coverage.append(cov)

        if spatial_coverage:
            metadata['spatial_coverage'] = spatial_coverage

        logger.info("Computing temporal coverage...")
        temporal_coverage = []

        with tracer.start_as_current_span('profile/temporal_coverage'):
            # Datetime columns
            for idx, col in enumerate(columns):
                if types.DATE_TIME not in col['semantic_types']:
                    continue
                datetimes = resolved_columns[idx]['datetimes']
                timestamps = resolved_columns[idx]['timestamps']
                logger.info(
                    "Computing temporal ranges datetime=%r (%d rows)",
                    col['name'], len(datetimes),
                )

                # Get temporal ranges
                ranges = get_numerical_ranges(timestamps)
                if not ranges:
                    continue

                # Get temporal resolution
                resolution = get_temporal_resolution(datetimes)

                temporal_coverage.append({
                    'type': 'datetime',
                    'column_names': [col['name']],
                    'column_indexes': [idx],
                    'column_types': [types.DATE_TIME],
                    'ranges': ranges,
                    'temporal_resolution': resolution,
                })

            # TODO: Times split over multiple columns

        if temporal_coverage:
            metadata['temporal_coverage'] = temporal_coverage

    # Attribute names
    attribute_keywords = []
    for col in columns:
        attribute_keywords.append(col['name'])
        kw = list(expand_attribute_name(col['name']))
        if kw != [col['name']]:
            attribute_keywords.extend(kw)
    metadata['attribute_keywords'] = attribute_keywords

    # Sample data
    if include_sample:
        with tracer.start_as_current_span('profile/sample'):
            rand = numpy.random.RandomState(RANDOM_SEED)
            choose_rows = rand.choice(
                len(data),
                min(SAMPLE_ROWS, len(data)),
                replace=False,
            )
            choose_rows.sort()  # Keep it in order
            sample = data.iloc[choose_rows]
            sample = sample.map(truncate_string)  # Truncate long values
            metadata['sample'] = sample.to_csv(index=False, lineterminator='\r\n')

    # Return it -- it will be inserted into Elasticsearch, and published to the
    # feed and the waiting on-demand searches
    return metadata
