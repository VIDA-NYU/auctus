#!/usr/bin/env python3
import elasticsearch
import os
import sys


SIZE = 10000


def count():
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
    sources = {}
    while True:
        hits = es.search(
            index='datamart',
            body={
                'query': {
                    'match_all': {},
                },
            },
            size=SIZE,
        )['hits']['hits']
        for h in hits:
            identifier = h['_source']['materialize']['identifier']
            try:
                sources[identifier] += 1
            except KeyError:
                sources[identifier] = 1
        if len(hits) != SIZE:
            break
    for identifier, count in sorted(sources.items(), key=lambda p: -p[1]):
        print('{: 6d} {}'.format(count, identifier))


if __name__ == '__main__':
    count()
