import aio_pika
import asyncio
import json
import logging
import os


logger = logging.getLogger(__name__)


def log_future(future, message="Exception in background task"):
    def log(future):
        try:
            future.result()
        except Exception:
            logger.exception(message)
    future.add_done_callback(log)


class Coordinator(object):
    def __init__(self, es):
        self.elasticsearch = es
        self.recent_discoveries = []  # (dataset_id, storage, ingested: bool)

        log_future(asyncio.get_event_loop().create_task(self._amqp()))

    async def _amqp(self):
        connection = await aio_pika.connect_robust(
            host=os.environ['AMQP_HOST'],
            login=os.environ['AMQP_USER'],
            password=os.environ['AMQP_PASSWORD'],
        )
        self.channel = await connection.channel()
        await self.channel.set_qos(prefetch_count=1)

        # Register to ingest exchange
        self.ingest_exchange = await self.channel.declare_exchange(
            'ingest',
            aio_pika.ExchangeType.FANOUT,
        )
        self.ingest_queue = await self.channel.declare_queue(exclusive=True)
        await self.ingest_queue.bind(self.ingest_exchange)

        # Register to datasets exchange
        datasets_exchange = await self.channel.declare_exchange(
            'datasets',
            aio_pika.ExchangeType.TOPIC)
        self.datasets_queue = await self.channel.declare_queue(exclusive=True)
        await self.datasets_queue.bind(datasets_exchange)

        # Register to queries exchange
        queries_exchange = await self.channel.declare_exchange(
            'queries',
            aio_pika.ExchangeType.FANOUT)
        self.queries_queue = await self.channel.declare_queue(exclusive=True)
        await self.queries_queue.bind(queries_exchange)

        log_future(
            asyncio.get_event_loop().create_task(self._consume_ingest()))
        log_future(
            asyncio.get_event_loop().create_task(self._consume_datasets()))
        log_future(
            asyncio.get_event_loop().create_task(self._consume_queries()))

    async def _consume_ingest(self):
        # Consume ingest messages
        async for message in self.ingest_queue:
            obj = json.loads(message.body.decode('utf-8'))
            dataset_id = obj['id']
            storage = obj['storage']['path']
            for i in range(len(self.recent_discoveries)):
                if self.recent_discoveries[i][0] == dataset_id:
                    break
            else:
                self.recent_discoveries.insert(0, (dataset_id, storage, False))
                del self.recent_discoveries[15:]

    async def _consume_datasets(self):
        # Consume dataset messages
        async for message in self.datasets_queue:
            obj = json.loads(message.body.decode('utf-8'))
            dataset_id = obj['id']
            for i in range(len(self.recent_discoveries)):
                if self.recent_discoveries[i][0] == dataset_id:
                    self.recent_discoveries[i][1] = None
                    self.recent_discoveries[i][2] = True
                    break
            else:
                self.recent_discoveries.insert(0, (dataset_id, None, True))
                del self.recent_discoveries[15:]

    async def _consume_queries(self):
        # Consume queries messages
        async for message in self.queries_queue:
            obj = json.loads(message.body.decode('utf-8'))
            # TODO: Store recent queries
