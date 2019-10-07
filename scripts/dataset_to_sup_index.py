#!/usr/bin/env python3

import elasticsearch
import elasticsearch.helpers
import logging
import os

from datamart_core.common import add_dataset_to_sup_index


def create_indices():
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )

    query = {
        'query': {
            'match_all': {}
        }
    }

    hits = elasticsearch.helpers.scan(
        es,
        index='datamart',
        query=query,
        size=100,
    )
    for hit in hits:
        add_dataset_to_sup_index(es, hit['_id'], hit['_source'])


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    create_indices()
