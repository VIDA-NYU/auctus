#!/usr/bin/env python3
import elasticsearch
import json
import os
import sys


def decode_dataset_id(dataset_id):
    dataset_id = list(dataset_id)
    i = 0
    while i < len(dataset_id):
        if dataset_id[i] == '_':
            if dataset_id[i + 1] == '_':
                del dataset_id[i + 1]
            else:
                char_hex = dataset_id[i + 1:i + 3]
                dataset_id[i + 1:i + 3] = []
                char_hex = ''.join(dataset_id[i + 1: i + 3])
                dataset_id[i] = chr(int(char_hex, 16))
        i += 1
    return dataset_id


def import_all(folder):
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        with open(path, 'r') as fp:
            obj = json.load(fp)
        es.index(
            'datamart',
            '_doc',
            obj,
            id=name,
        )
        print('.', end='', flush=True)


if __name__ == '__main__':
    import_all(sys.argv[1])
