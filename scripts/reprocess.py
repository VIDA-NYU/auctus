#!/usr/bin/env python3

"""This script reprocesses datasets that are already in the index.

This is generally not necessary. You can use freshen_old_index.py to reprocess
datasets that were profiled by old versions of the profiler.
"""

import aio_pika
import asyncio
import elasticsearch
import logging
import os
import sys

from datamart_core.common import PrefixedElasticsearch, json2msg


logger = logging.getLogger(__name__)


async def freshen(datasets, priority):
    es = PrefixedElasticsearch()

    amqp_conn = await aio_pika.connect_robust(
        host=os.environ['AMQP_HOST'],
        port=int(os.environ['AMQP_PORT']),
        login=os.environ['AMQP_USER'],
        password=os.environ['AMQP_PASSWORD'],
    )
    amqp_chan = await amqp_conn.channel()
    amqp_profile_exchange = await amqp_chan.declare_exchange(
        'profile',
        aio_pika.ExchangeType.FANOUT,
    )

    for dataset_id in datasets:
        try:
            obj = es.get('datasets', dataset_id)['_source']
        except elasticsearch.NotFoundError:
            obj = es.get('pending', dataset_id)['_source']['metadata']
            dataset_version = None
        else:
            dataset_version = obj['version']

        logger.info("Reprocessing %s, version=%r",
                    dataset_id, dataset_version)
        metadata = dict(name=obj['name'],
                        materialize=obj['materialize'],
                        source=obj.get('source', 'unknown'))
        if obj.get('description'):
            metadata['description'] = obj['description']
        if obj.get('date'):
            metadata['date'] = obj['date']
        if obj.get('manual_annotations'):
            metadata['manual_annotations'] = obj['manual_annotations']
        await amqp_profile_exchange.publish(
            json2msg(
                dict(id=dataset_id, metadata=metadata),
                priority=priority,
            ),
            '',
        )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    args = sys.argv[1:]

    priority = 0
    if args and args[0] == '--prio2':
        priority = 2
        args = args[1:]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(loop.create_task(
        freshen(args, priority)
    ))
