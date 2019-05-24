#!/usr/bin/env python3
import aio_pika
import asyncio
import elasticsearch
import json
import os
import sys
import time

from datamart_core.common import json2msg


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

    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        with open(path, 'r') as fp:
            obj = json.load(fp)
        try:
            es.index(
                'datamart',
                '_doc',
                obj,
                id=name,
            )
        except elasticsearch.TransportError:
            print('X', end='', flush=True)
            time.sleep(10)  # If writing can't keep up, needs a real break
            es.index(
                'datamart',
                '_doc',
                obj,
                id=name,
            )
        await amqp_datasets_exchange.publish(
            json2msg(dict(obj, id=name)),
            name,
        )
        print('.', end='', flush=True)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(loop.create_task(
        import_all(sys.argv[1])
    ))
