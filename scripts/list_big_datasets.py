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
                        "gt": 10_000_000_000,
                    },
                },
            },
        },
        _source='size',
        size=SIZE,
    )
    for h in hits:
        print("%s %.1f GB" % (h['_id'], h['_source']['size'] / 1_000_000_000.0))


if __name__ == '__main__':
    search()
