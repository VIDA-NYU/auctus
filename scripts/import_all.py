#!/usr/bin/env python3

"""This script imports an exported index.

It simply loads the data from the JSON files. If you want to reprocess them
instead, use `reprocess_all.py`, which will only read name, description, and
date, and obtain the rest of the metadata via profiling.
"""

import asyncio
import elasticsearch
import json
import lazo_index_service
import logging
import os
import sys
import time

from datamart_core.common import PrefixedElasticsearch, add_dataset_to_index, \
    delete_dataset_from_index, add_dataset_to_lazo_storage, decode_dataset_id


RETRY_DELAYS = [10, 15, 30, 45, 60, 80, 100, 120, 140, 0]  # 10 attempts


async def import_all(folder):
    es = PrefixedElasticsearch()
    if 'LAZO_SERVER_HOST' in os.environ:
        lazo_client = lazo_index_service.LazoIndexClient(
            host=os.environ['LAZO_SERVER_HOST'],
            port=int(os.environ['LAZO_SERVER_PORT'])
        )
    else:
        lazo_client = None

    dataset_docs = []
    lazo_docs = []
    for name in os.listdir(folder):
        if name.startswith('lazo.'):
            lazo_docs.append(name)
        else:
            dataset_docs.append(name)

    for i, name in enumerate(dataset_docs):
        if i % 50 == 0:
            print(
                "\nImporting to Elasticsearch, %d/%d" % (i, len(dataset_docs)),
                flush=True,
            )
        path = os.path.join(folder, name)
        with open(path, 'r') as fp:
            obj = json.load(fp)

        dataset_id = decode_dataset_id(name)
        for i, delay in enumerate(RETRY_DELAYS):
            try:
                delete_dataset_from_index(es, dataset_id, lazo_client)
                add_dataset_to_index(es, dataset_id, obj)
                break
            except elasticsearch.TransportError:
                print('X', end='', flush=True)
                if i == len(RETRY_DELAYS) - 1:
                    raise
                time.sleep(delay)
        print('.', end='', flush=True)

    for i, name in enumerate(lazo_docs):
        if i % 500 == 0:
            print(
                "\nImporting to Lazo, %d/%d" % (i, len(lazo_docs)),
                flush=True,
            )
        path = os.path.join(folder, name)
        with open(path, 'r') as fp:
            obj = json.load(fp)

        dataset_id = decode_dataset_id(name[5:]).rsplit('.', 1)[0]
        lazo_es_id = obj.pop('_id')
        assert lazo_es_id.split('__.__')[0] == dataset_id
        for i, delay in enumerate(RETRY_DELAYS):
            try:
                add_dataset_to_lazo_storage(es, lazo_es_id, obj)
                break
            except elasticsearch.TransportError:
                print('X', end='', flush=True)
                if i == len(RETRY_DELAYS) - 1:
                    raise
                time.sleep(delay)
        if i % 10 == 0:
            print('.', end='', flush=True)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('elasticsearch').setLevel(logging.ERROR)
    logging.getLogger('datamart_core.common').setLevel(logging.WARNING)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(loop.create_task(
        import_all(sys.argv[1])
    ))
