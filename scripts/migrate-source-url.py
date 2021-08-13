#!/usr/bin/env python3

"""This scripts adds the source_url for Socrata datasets.
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

        if obj['materialize']['identifier'] == 'datamart.socrata':
            if 'source_url' not in obj:
                obj['source_url'] = 'https://%s/_/_/%s' % (
                    obj['materialize']['socrata_domain'],
                    obj['materialize']['socrata_id'],
                )

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
