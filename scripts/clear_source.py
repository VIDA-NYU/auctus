#!/usr/bin/env python3
import elasticsearch
import os
import sys


def clear(identifier):
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
    hits = es.search(
        index='datamart',
        body={
            'query': {
                'term': {
                    'materialize.identifier': identifier,
                }
            }
        },
    )['hits']['hits']
    for h in hits:
        es.delete('datamart', '_doc', h['_id'])


if __name__ == '__main__':
    clear(sys.argv[1])
