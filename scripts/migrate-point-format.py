#!/usr/bin/env python3

"""This scripts updates the index for !166.

It adds the column "point_format" information (the default one, "long,lat").
"""

import json
import os
import shutil
import sys


def migrate(from_folder, to_folder):
    assert os.listdir(from_folder)
    assert not os.listdir(to_folder)

    datasets = []
    lazo = []
    for f in os.listdir(from_folder):
        if f.startswith('lazo.'):
            lazo.append(f)
        else:
            datasets.append(f)

    for i, dataset in enumerate(datasets):
        if i % 100 == 0:
            print("% 5d / %5d datasets processed" % (i, len(datasets)))

        with open(os.path.join(from_folder, dataset)) as fp:
            obj = json.load(fp)

        for column in obj['columns']:
            if (
                column['structural_type'] == 'http://schema.org/GeoCoordinates'
                and 'point_format' not in column
            ):
                column['point_format'] = 'long,lat'

        with open(os.path.join(to_folder, dataset), 'w') as fp:
            json.dump(obj, fp, sort_keys=True, indent=2)

    print("Copying lazo data...")
    for i, f in enumerate(lazo):
        if i % 1000 == 0:
            print("% 5d / %5d files copied" % (i, len(lazo)))
        shutil.copy2(
            os.path.join(from_folder, f),
            os.path.join(to_folder, f),
        )


if __name__ == '__main__':
    migrate(sys.argv[1], sys.argv[2])
