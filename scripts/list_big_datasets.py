#!/usr/bin/env python3

"""This script lists datasets with a big size.
"""

import elasticsearch
import elasticsearch.helpers
import os


SIZE = 10000


def search():
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
    hits = elasticsearch.helpers.scan(
        es,
        index='datamart',
        query={
            'query': {
                'range': {
                    "size": {
                        "gt": 50_000_000,
                    },
                },
            },
        },
        _source=False,
        size=SIZE,
    )
    for h in hits:
        print(h['_id'])


if __name__ == '__main__':
    search()
