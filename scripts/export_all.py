#!/usr/bin/env python3

import elasticsearch
import logging
import json
import os

from datamart_core.common import encode_dataset_id


SIZE = 10000


def export():
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
    for index in ('datamart', 'lazo'):
        prefix = 'lazo.' if index == 'lazo' else ''
        while True:
            hits = es.search(
                index=index,
                body={
                    'query': {
                        'match_all': {},
                    },
                },
                size=SIZE,
            )['hits']['hits']
            for h in hits:
                with open(encode_dataset_id(prefix + h['_id']), 'w') as fp:
                    json.dump(h['_source'], fp, sort_keys=True, indent=2)
            print('.', end='', flush=True)
            if len(hits) != SIZE:
                break


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    export()
