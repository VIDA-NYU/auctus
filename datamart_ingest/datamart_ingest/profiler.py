import csv
import os
import time


def handle_dataset(storage, metadata):
    """Compute all metafeatures from a dataset.

    :param metadata: The metadata provided by the discovery plugin (might be
        very limited).
    """
    time.sleep(4)
    with open(os.path.join(storage.path, 'main.csv'), newline='') as fp:
        header = next(iter(csv.reader(fp)))

        nb_rows = 1 + sum(1 for _ in fp)
    return dict(
        metadata,
        nb_rows=nb_rows,
        columns=[dict(name=h) for h in header],
    )
