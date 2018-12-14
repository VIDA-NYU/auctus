import aio_pika
import asyncio
import itertools
import json
import logging
import os
import sys
import time


logger = logging.getLogger(__name__)


def log_future(future, message="Exception in background task",
               should_never_exit=False):
    def log(future):
        try:
            future.result()
        except Exception:
            logger.exception(message)
        if should_never_exit:
            logger.critical("Critical task died, exiting")
            asyncio.get_event_loop().stop()
            sys.exit(1)
    future.add_done_callback(log)


class Coordinator(object):
    def __init__(self, es):
        self.elasticsearch = es
        self.recent_discoveries = []

        # Retry a few times, in case the Elasticsearch container is not yet up
        for i in itertools.count():
            try:
                if not es.indices.exists('datamart'):
                    logger.info("Creating 'datamart' index in Elasticsearch")
                    es.indices.create(
                        'datamart',
                        {
                            'mappings': {
                                '_doc': {
                                    'properties': {
                                        # 'columns' is a nested field, we want
                                        # to query individual columns
                                        'columns': {
                                            'type': 'nested',
                                            'properties': {
                                                'semantic_types': {
                                                    'type': 'keyword',
                                                    'index': True,
                                                },
                                                # we want to query individual numerical ranges
                                                'coverage': {
                                                    'type': 'nested',
                                                    'properties': {
                                                        'range': {
                                                            'type': 'double_range'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'spatial_coverage': {
                                            'type': 'nested',
                                            'properties': {
                                                'lat': {
                                                    'type': 'text'
                                                },
                                                'lon': {
                                                    'type': 'text'
                                                },
                                                # we want to query individual spatial ranges
                                                'ranges': {
                                                    'type': 'nested',
                                                    'properties': {
                                                        'range': {
                                                            'type': 'geo_shape'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'license': {
                                            'type': 'keyword',
                                            'index': True,
                                        },
                                    },
                                },
                            },
                        },
                    )
            except Exception:
                logger.warning("Can't connect to Elasticsearch, retrying...")
                if i == 5:
                    raise
                else:
                    time.sleep(5)
            else:
                break

        log_future(asyncio.get_event_loop().create_task(self._amqp()),
                   should_never_exit=True)

    async def _amqp(self):
        connection = await aio_pika.connect_robust(
            host=os.environ['AMQP_HOST'],
            login=os.environ['AMQP_USER'],
            password=os.environ['AMQP_PASSWORD'],
        )
        self.channel = await connection.channel()
        await self.channel.set_qos(prefetch_count=1)

        # Register to profiling exchange
        self.profile_exchange = await self.channel.declare_exchange(
            'profile',
            aio_pika.ExchangeType.FANOUT,
        )
        self.profile_queue = await self.channel.declare_queue(exclusive=True)
        await self.profile_queue.bind(self.profile_exchange)

        # Register to datasets exchange
        datasets_exchange = await self.channel.declare_exchange(
            'datasets',
            aio_pika.ExchangeType.TOPIC)
        self.datasets_queue = await self.channel.declare_queue(exclusive=True)
        await self.datasets_queue.bind(datasets_exchange, '#')

        # Register to queries exchange
        queries_exchange = await self.channel.declare_exchange(
            'queries',
            aio_pika.ExchangeType.FANOUT)
        self.queries_queue = await self.channel.declare_queue(exclusive=True)
        await self.queries_queue.bind(queries_exchange)

        await asyncio.gather(
            asyncio.get_event_loop().create_task(self._consume_profile()),
            asyncio.get_event_loop().create_task(self._consume_datasets()),
            asyncio.get_event_loop().create_task(self._consume_queries()),
        )

    async def _consume_profile(self):
        # Consume profiling messages
        async for message in self.profile_queue.iterator(no_ack=True):
            obj = json.loads(message.body.decode('utf-8'))
            dataset_id = obj['id']
            metadata = obj.get('metadata', {})
            materialize = metadata.get('materialize', {})
            logger.info("Got profile message: %r", dataset_id)
            storage = obj['storage']['path']
            for i in range(len(self.recent_discoveries)):
                if self.recent_discoveries[i]['id'] == dataset_id:
                    break
            else:
                self.recent_discoveries.insert(
                    0,
                    dict(id=dataset_id, storage=storage,
                         discoverer=materialize.get('identifier', '(unknown)'),
                         discovered=materialize.get('date', '???')),
                )
                del self.recent_discoveries[15:]

    async def _consume_datasets(self):
        # Consume dataset messages
        async for message in self.datasets_queue.iterator(no_ack=True):
            obj = json.loads(message.body.decode('utf-8'))
            dataset_id = obj['id']
            materialize = obj.get('materialize', {})
            logger.info("Got dataset message: %r", dataset_id)
            for i in range(len(self.recent_discoveries)):
                if self.recent_discoveries[i]['id'] == dataset_id:
                    self.recent_discoveries[i].pop('storage', None)
                    self.recent_discoveries[i]['profiled'] = obj.get('date',
                                                                     '???')
                    break
            else:
                self.recent_discoveries.insert(
                    0,
                    dict(id=dataset_id,
                         discoverer=materialize.get('identifier', '(unknown)'),
                         discovered=materialize.get('date', '???'),
                         profiled=obj.get('date', '???')),
                )
                del self.recent_discoveries[15:]

    async def _consume_queries(self):
        # Consume queries messages
        async for message in self.queries_queue.iterator(no_ack=True):
            obj = json.loads(message.body.decode('utf-8'))
            logger.info("Got query message")
            # TODO: Store recent queries
