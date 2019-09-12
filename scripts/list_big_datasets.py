#!/usr/bin/env python3

"""This script lists datasets with a big size.
"""

import elasticsearch
import os


SIZE = 10000


def search():
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
    while True:
        hits = es.search(
            index='datamart',
            body={
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
        )['hits']['hits']
        for h in hits:
            print(h['_id'])
        if len(hits) != SIZE:
            break


if __name__ == '__main__':
    search()
