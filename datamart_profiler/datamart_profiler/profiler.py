import json
import math
import numpy
import os
import pandas
import subprocess

from .identify_types import identify_types


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


def handle_dataset(storage, metadata):
    """Compute all metafeatures from a dataset.

    :param metadata: The metadata provided by the discovery plugin (might be
        very limited).
    """
    csv_file = os.path.join(storage.path, 'main.csv')

    # Run SCDP
    cmd = ['java', '-jar', 'scdp.jar', csv_file]
    proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stdin=subprocess.PIPE)
    proc.stdin.close()
    scdp_out = json.load(proc.stdout)
    if proc.wait() != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)
    print(scdp_out)

    # File size
    metadata['size'] = os.path.getsize(csv_file)

    df = pandas.read_csv(os.path.join(storage.path, 'main.csv'),
                         dtype=str, na_filter=False)

    # Number of rows
    metadata['nb_rows'] = df.shape[0]

    # Get column dictionary
    columns = metadata.setdefault('columns', [])
    # Fix size if wrong
    if len(columns) != len(df.columns):
        columns[:] = [{} for _ in range(len(df.columns))]

    # Set column names
    for column_meta, name in zip(columns, df.columns):
        column_meta['name'] = name

    # Identify types
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
            timestamps = numpy.array(
                (dt.timestamp()
                for dt in semantic_types_dict['http://schema.org/DateTime']),
                dtype='float32')
            mean, stddev = mean_stddev(timestamps)

    # TODO: Compute histogram

    # Return it -- it will be inserted into Elasticsearch, and published to the
    # feed and the waiting on-demand searches
    return metadata
