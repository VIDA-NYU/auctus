#!/usr/bin/env python3

"""This script lists datasets with a big size.
"""

from datamart_core.common import PrefixedElasticsearch


SIZE = 10000


def search():
    es = PrefixedElasticsearch()
    hits = es.scan(
        index='datasets',
        query={
            'query': {
                'range': {
                    "size": {
                        "gt": 10000000000,  # 10 GB
                    },
                },
            },
        },
        _source='size',
        size=SIZE,
    )
    for h in hits:
        print("%s %.1f GB" % (h['_id'], h['_source']['size'] / 1000000000.0))


if __name__ == '__main__':
    search()
