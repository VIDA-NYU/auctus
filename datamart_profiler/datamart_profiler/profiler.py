import os
import pandas
import time

from .identify_types import identify_types


def handle_dataset(storage, metadata):
    """Compute all metafeatures from a dataset.

    :param metadata: The metadata provided by the discovery plugin (might be
        very limited).
    """
    time.sleep(3.03)

    # File size
    metadata['size'] = os.path.getsize(os.path.join(storage.path, 'main.csv'))

    df = pandas.read_csv(os.path.join(storage.path, 'main.csv'))

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
        structural_types, semantic_types_dict = identify_types(array)
        # Set structural type
        column_meta['structural_type'] = structural_types
        # Add semantic types to the ones already present
        sem_types = column_meta.setdefault('semantic_types', [])
        for sem_type in semantic_types_dict:
            if sem_type not in sem_types:
                sem_types.append(sem_type)

    # TODO: Compute histogram

    # Return it -- it will be inserted into Elasticsearch, and published to the
    # feed and the waiting on-demand searches
    return metadata
