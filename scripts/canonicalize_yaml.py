#!/usr/bin/env python3

"""This script sorts YAML documents and objects to allow diffing.

It loads multiple YAML files, orders the documents by 'metadata/kind' and
'metadata/name', sorts the keys of each objects alphabetically, and dumps it
all to stdout.

In addition, it also sorts the 'env:' list/map.

Usage:
    find yaml -type f -print0 | xargs -0 python canonicalize_yaml.py
"""

import sys
import yaml


def sort_env(obj):
    if isinstance(obj, list):
        return [sort_env(i) for i in obj]
    elif isinstance(obj, dict):
        return {
            k: (
                sorted(v, key=lambda i: i['name']) if k == 'env'
                else sort_env(v)
            )
            for k, v in obj.items()
        }
    else:
        return obj


if __name__ == '__main__':
    objs = []
    for filename in sys.argv[1:]:
        with open(filename, 'r') as fp_in:
            objs.extend(yaml.safe_load_all(fp_in))

    objs = [sort_env(o) for o in objs]
    objs = sorted(objs, key=lambda o: (o['kind'], o['metadata']['name']))

    yaml.safe_dump_all(objs, sys.stdout, sort_keys=True)
