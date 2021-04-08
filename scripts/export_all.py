#!/usr/bin/env python3

"""This script exports the index to JSON files.

It is useful as backup, and to provide snapshots to users so they don't have to
profile everything to get a system going.

The exported folder can be loaded in using `import_all.py` (which will simply
load the JSON files) or `reprocess_all.py` (which will only read some fields,
and get the metadata by reprocessing the datasets).
"""

from functools import lru_cache
import logging
import json
import os

from datamart_core.common import PrefixedElasticsearch, encode_dataset_id


SIZE = 10000


@lru_cache(maxsize=10000000)
def _next_filename(pattern):
    number = 0
    while os.path.exists(pattern.format(number)):
        number += 1
    return [number]  # Return a list so we can mutate it


def unique_filename(pattern):
    """Return a file name with an incrementing number to make it unique.
    """
    number = _next_filename(pattern)
    fname = pattern.format(number[0])
    number[0] += 1
    return fname


def export():
    es = PrefixedElasticsearch()

    print("Dumping datasets", end='', flush=True)
    hits = es.scan(
        index='datamart',
        query={
            'query': {
                'match_all': {},
            },
        },
        size=SIZE,
    )
    for h in hits:
        # Use dataset ID as file name
        with open(encode_dataset_id(h['_id']), 'w') as fp:
            json.dump(h['_source'], fp, sort_keys=True, indent=2)

    print("Dumping Lazo data", end='', flush=True)
    hits = es.scan(
        index='lazo',
        query={
            'query': {
                'match_all': {},
            },
        },
        size=SIZE,
    )
    for h in hits:
        # Use "lazo." dataset_id ".NB" as file name
        dataset_id = h['_id'].split('__.__')[0]
        fname = unique_filename(
            'lazo.{0}.{{0}}'.format(encode_dataset_id(dataset_id))
        )
        with open(fname, 'w') as fp:
            json.dump(
                dict(h['_source'], _id=h['_id']),
                fp,
                sort_keys=True,
                indent=2,
            )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    export()
