#!/usr/bin/env python3

"""This script deletes all the datasets in the index from a specific source.
"""

import elasticsearch
import elasticsearch.helpers
import lazo_index_service
import logging
import os
import sys

from datamart_core.common import delete_dataset_from_index


SIZE = 10000


def clear(source):
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
    lazo_client = lazo_index_service.LazoIndexClient(
        host=os.environ['LAZO_SERVER_HOST'],
        port=int(os.environ['LAZO_SERVER_PORT'])
    )
    hits = elasticsearch.helpers.scan(
        es,
        index='datamart,pending',
        query={
            'query': {
                'term': {
                    'source': source,
                },
            },
        },
        _source=False,
        size=SIZE,
    )
    for h in hits:
        delete_dataset_from_index(es, h['_id'], lazo_client)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    clear(sys.argv[1])
