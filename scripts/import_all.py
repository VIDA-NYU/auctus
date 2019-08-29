#!/usr/bin/env python3

import aio_pika
import asyncio
import elasticsearch
import json
import lazo_index_service
import logging
import os
import sys
import time

from datamart_core.common import add_dataset_to_index, \
    delete_dataset_from_index, add_dataset_to_lazo_storage, json2msg, \
    decode_dataset_id


async def import_all(folder):
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
    amqp_conn = await aio_pika.connect_robust(
        host=os.environ['AMQP_HOST'],
        login=os.environ['AMQP_USER'],
        password=os.environ['AMQP_PASSWORD'],
    )
    amqp_chan = await amqp_conn.channel()
    amqp_datasets_exchange = await amqp_chan.declare_exchange(
        'datasets',
        aio_pika.ExchangeType.TOPIC,
    )
    if 'LAZO_SERVER_HOST' in os.environ:
        lazo_client = lazo_index_service.LazoIndexClient(
            host=os.environ['LAZO_SERVER_HOST'],
            port=int(os.environ['LAZO_SERVER_PORT'])
        )
    else:
        lazo_client = None

    print("Importing Elasticsearch data")
    for name in os.listdir(folder):
        if name.startswith('lazo.'):
            continue
        path = os.path.join(folder, name)
        with open(path, 'r') as fp:
            obj = json.load(fp)

        dataset_id = decode_dataset_id(name)
        try:
            delete_dataset_from_index(es, dataset_id, lazo_client)
            add_dataset_to_index(es, dataset_id, obj)
        except elasticsearch.TransportError:
            print('X', end='', flush=True)
            time.sleep(10)  # If writing can't keep up, needs a real break
            delete_dataset_from_index(es, dataset_id, lazo_client)
            add_dataset_to_index(es, dataset_id, obj)
        await amqp_datasets_exchange.publish(
            json2msg(dict(obj, id=dataset_id)),
            dataset_id,
        )
        print('.', end='', flush=True)

    print("Importing Lazo data")
    for name in os.listdir(folder):
        if not name.startswith('lazo.'):
            continue
        path = os.path.join(folder, name)
        with open(path, 'r') as fp:
            obj = json.load(fp)

        id = decode_dataset_id(name[5:])
        try:
            add_dataset_to_lazo_storage(es, id, obj)
        except elasticsearch.TransportError:
            print('X', end='', flush=True)
            time.sleep(10)  # If writing can't keep up, needs a real break
            add_dataset_to_lazo_storage(es, id, obj)
        print('.', end='', flush=True)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(loop.create_task(
        import_all(sys.argv[1])
    ))
