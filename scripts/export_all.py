#!/usr/bin/env python3
import elasticsearch
import json
import os
import re


SIZE = 10000


re_non_path_safe = re.compile(r'[^A-Za-z0-9_.-]')


def encode_dataset_id(dataset_id):
    dataset_id = dataset_id.replace('_', '__')
    dataset_id = re_non_path_safe.sub(lambda m: '_%X' % ord(m.group(0)),
                                      dataset_id)
    return dataset_id


def export():
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
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
            with open(encode_dataset_id(h['_id']), 'w') as fp:
                json.dump(h['_source'], fp, sort_keys=True, indent=2)
        if len(hits) != SIZE:
            break


if __name__ == '__main__':
    export()
