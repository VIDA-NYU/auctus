#!/usr/bin/env python3
import elasticsearch
import lazo_index_service
import logging
import os
import sys

from datamart_core.common import delete_dataset_from_index


SIZE = 10000


def delete(datasets):
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
    lazo_client = lazo_index_service.LazoIndexClient(
        host=os.environ['LAZO_SERVER_HOST'],
        port=int(os.environ['LAZO_SERVER_PORT'])
    )
    for dataset in datasets:
        delete_dataset_from_index(es, lazo_client, dataset)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    delete(sys.argv[1:])
