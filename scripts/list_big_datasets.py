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
    from_ = 0
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
            from_=from_,
            size=SIZE,
        )['hits']['hits']
        from_ += len(hits)
        for h in hits:
            print(h['_id'])
        if len(hits) != SIZE:
            break


if __name__ == '__main__':
    search()
