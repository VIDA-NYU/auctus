import csv
import os


def handle_dataset(storage, discovery_meta):
    with open(os.path.join(storage.path, 'main.csv'), newline='') as fp:
        header = next(iter(csv.reader(fp)))

        nb_rows = 1 + sum(1 for _ in fp)
    return dict(
        nb_rows=nb_rows,
        columns=[dict(name=h) for h in header],
    )
