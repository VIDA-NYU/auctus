#!/usr/bin/env python3

"""This scripts updates the index for !162.

It creates the 'temporal_coverage' (in 'datasets' index) and the
'temporal_coverage' index.
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

        if 'temporal_coverage' not in obj:
            temporal_coverage = []
            for idx, column in enumerate(obj['columns']):
                if 'http://schema.org/DateTime' in column['semantic_types']:
                    coverage = {
                        'type': 'datetime',
                        'column_names': [column['name']],
                        'column_indexes': [idx],
                        'column_types': ['http://schema.org/DateTime'],
                        'ranges': column.pop('coverage'),
                    }
                    column.pop('mean', None)
                    column.pop('stddev', None)
                    if 'temporal_resolution' in column:
                        coverage['temporal_resolution'] = \
                            column.pop('temporal_resolution')
                    temporal_coverage.append(coverage)

            if temporal_coverage:
                obj['temporal_coverage'] = temporal_coverage

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
