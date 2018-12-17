#!/usr/bin/env python3
import elasticsearch
import os
import sys


SIZE = 10000


def clear(identifier):
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
    while True:
        hits = es.search(
            index='datamart',
            body={
                'query': {
                    'term': {
                        'materialize.identifier': identifier,
                    },
                },
            },
            size=SIZE,
        )['hits']['hits']
        for h in hits:
            es.delete('datamart', '_doc', h['_id'])
        if len(hits) != SIZE:
            break


if __name__ == '__main__':
    clear(sys.argv[1])
