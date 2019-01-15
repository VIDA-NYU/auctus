#!/usr/bin/env python3
import elasticsearch
import json
import os
import sys


def import_json(dataset_id, fp):
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
    obj = json.load(fp)
    es.index(
        'datamart',
        '_doc',
        obj,
        id=dataset_id,
    )


if __name__ == '__main__':
    import_json(sys.argv[1], sys.stdin)
