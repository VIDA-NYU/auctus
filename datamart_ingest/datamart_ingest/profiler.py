import csv
import os
import time


def handle_dataset(storage, metadata):
    """Compute all metafeatures from a dataset.

    :param metadata: The metadata provided by the discovery plugin (might be
        very limited).
    """
    time.sleep(3.03)

    with open(os.path.join(storage.path, 'main.csv'), newline='') as fp:
        # Read header from first line
        header = next(iter(csv.reader(fp)))

        # Count rows
        nb_rows = 1 + sum(1 for _ in fp)

    # Update metadata
    metadata['nb_rows'] = nb_rows
    columns = metadata.setdefault('columns', [])
    if len(columns) != len(header):
        columns[:] = [{} for _ in range(len(header))]
    for dst, src in zip(columns, header):
        dst['name'] = src

    # Return it -- it will be inserted into Elasticsearch, and published to the
    # feed and the waiting on-demand searches
    return metadata
