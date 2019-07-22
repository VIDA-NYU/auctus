#!/usr/bin/env python3
import logging

import aio_pika
import asyncio
import json
import os
import sys

from datamart_core.common import json2msg, decode_dataset_id


async def import_all(folder):
    amqp_conn = await aio_pika.connect_robust(
        host=os.environ['AMQP_HOST'],
        login=os.environ['AMQP_USER'],
        password=os.environ['AMQP_PASSWORD'],
    )
    amqp_chan = await amqp_conn.channel()
    amqp_profile_exchange = await amqp_chan.declare_exchange(
        'profile',
        aio_pika.ExchangeType.FANOUT,
    )

    for name in os.listdir(folder):
        dataset_id = decode_dataset_id(name)
        path = os.path.join(folder, name)
        with open(path, 'r') as fp:
            obj = json.load(fp)
        metadata = dict(name=obj['name'],
                        description=obj['description'],
                        materialize=obj['materialize'])
        if obj.get('description'):
            metadata['description'] = obj['description']
        await amqp_profile_exchange.publish(
            json2msg(dict(id=dataset_id, metadata=metadata)),
            '',
        )
        print('.', end='', flush=True)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(loop.create_task(
        import_all(sys.argv[1])
    ))
