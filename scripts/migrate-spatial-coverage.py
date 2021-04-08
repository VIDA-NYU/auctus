#!/usr/bin/env python3

"""This scripts updates the index for !141.

It changes the format of 'spatial_coverage' (in 'datasets' index) and the
'spatial_coverage' index.
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

        if 'spatial_coverage' in obj:
            new_list = []
            for old_coverage in obj.pop('spatial_coverage'):
                # Parse old coverage
                new_coverage = {'ranges': old_coverage.pop('ranges')}
                if 'lat' in old_coverage or 'lon' in old_coverage:
                    new_coverage['type'] = 'latlong'
                    new_coverage['column_names'] = [
                        old_coverage.pop('lat'),
                        old_coverage.pop('lon'),
                    ]
                elif 'address' in old_coverage:
                    new_coverage['type'] = 'address'
                    new_coverage['column_names'] = [
                        old_coverage.pop('address'),
                    ]
                elif 'point' in old_coverage:
                    new_coverage['type'] = 'point'
                    new_coverage['column_names'] = [
                        old_coverage.pop('point'),
                    ]
                elif 'admin' in old_coverage:
                    new_coverage['type'] = 'admin'
                    new_coverage['column_names'] = [
                        old_coverage.pop('admin'),
                    ]
                else:
                    raise ValueError(
                        "Error: Unknown spatial coverage in file %s" % dataset
                    )
                if old_coverage:
                    raise ValueError("Error: Leftover keys in file %s" % dataset)

                # Add the column indexes
                indexes = {
                    col['name']: idx
                    for idx, col in enumerate(obj['columns'])
                }
                new_coverage['column_indexes'] = [
                    indexes[name]
                    for name in new_coverage['column_names']
                ]

                new_list.append(new_coverage)

            # Replace information
            obj['spatial_coverage'] = new_list

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
