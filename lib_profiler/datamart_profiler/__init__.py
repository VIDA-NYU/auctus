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
    mean = total / len(array)
    total = 0
    for elem in array:
        try:
            elem = float(elem) - mean
        except ValueError:
            continue
        total += elem * elem
    stddev = math.sqrt(total / len(array))

    return mean, stddev


def process_dataset(path, metadata):
    """Compute all metafeatures from a dataset.

    :param metadata: The metadata provided by the discovery plugin (might be
        very limited).
    """
    csv_file = os.path.join(path, 'main.csv')

    # File size
    metadata['size'] = os.path.getsize(csv_file)
    logger.info("File size: %r bytes", metadata['size'])

    # Run SCDP
    logger.info("Running SCDP...")
    scdp = pkg_resources.resource_filename('datamart_profiler', 'scdp.jar')
    cmd = ['java', '-jar', scdp, csv_file]
    proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stdin=subprocess.PIPE)
    stdout, _ = proc.communicate()
    if proc.wait() != 0:
        logger.error("Error running SCDP: returned %d", proc.returncode)
        scdp_out = {}
    else:
        try:
            scdp_out = json.loads(stdout)
        except json.JSONDecodeError:
            logger.exception("Invalid output from SCDP")
            scdp_out = {}

    # Sub-sample
    if metadata['size'] > MAX_SIZE:
        logger.info("Counting rows...")
        with open(csv_file, 'rb') as fp:
            metadata['nb_rows'] = sum(1 for _ in fp)

        ratio = metadata['size'] / MAX_SIZE
        logger.info("Loading dataframe, sample ratio=%r...", ratio)
        df = pandas.read_csv(csv_file,
                             dtype=str, na_filter=False,
                             skiprows=lambda i: i != 0 and i > ratio)
    else:
        logger.info("Loading dataframe...")
        df = pandas.read_csv(csv_file,
                             dtype=str, na_filter=False)

        metadata['nb_rows'] = df.shape[0]

    logger.info("Dataframe loaded, %d rows, %d columns",
                df.shape[0], df.shape[1])

    # Get column dictionary
    columns = metadata.setdefault('columns', [])
    # Fix size if wrong
    if len(columns) != len(df.columns):
        logger.info("Setting column names from header")
        columns[:] = [{} for _ in range(len(df.columns))]
    else:
        logger.info("Keeping columns from discoverer")

    # Set column names
    for column_meta, name in zip(columns, df.columns):
        column_meta['name'] = name

    # Copy info from SCDP
    for column_meta, name in zip(columns, df.columns):
        column_meta.update(scdp_out.get(name, {}))

    # Identify types
    logger.info("Identifying types...")
    for i, column_meta in enumerate(columns):
        array = df.iloc[:, i]
        structural_type, semantic_types_dict = identify_types(array)
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

        if 'http://schema.org/DateTime' in semantic_types_dict:
            timestamps = numpy.empty(
                len(semantic_types_dict['http://schema.org/DateTime']),
                dtype='float32',
            )
            for j, dt in enumerate(
                    semantic_types_dict['http://schema.org/DateTime']):
                timestamps[j] = dt.timestamp()
            column_meta['mean'], column_meta['stddev'] = \
                mean_stddev(timestamps)

    # TODO: Compute histogram

    # Return it -- it will be inserted into Elasticsearch, and published to the
    # feed and the waiting on-demand searches
    return metadata
