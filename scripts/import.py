#!/usr/bin/env python3
import elasticsearch
import json
import logging
import os
import sys

from datamart_core.common import add_dataset_to_index


def import_json(dataset_id, fp):
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
    obj = json.load(fp)
    add_dataset_to_index(es, dataset_id, obj)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    import_json(sys.argv[1], sys.stdin)
