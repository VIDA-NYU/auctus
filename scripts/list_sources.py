#!/usr/bin/env python3

"""This script gives a summary of the dataset sources.
"""

import elasticsearch
import elasticsearch.helpers
import os


SIZE = 10000


def count():
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
    sources = {}
    hits = elasticsearch.helpers.scan(
        es,
        index='datamart',
        query={
            'query': {
                'match_all': {},
            },
        },
        _source='source',
        size=SIZE,
    )
    for h in hits:
        source = h['_source']['source']

        try:
            sources[source] += 1
        except KeyError:
            sources[source] = 1

    for identifier, count in sorted(sources.items(), key=lambda p: -p[1]):
        print('{: 6d} {}'.format(count, identifier))


if __name__ == '__main__':
    count()
