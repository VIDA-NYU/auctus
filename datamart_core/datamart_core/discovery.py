import aio_pika
import asyncio
from datetime import datetime
import json
import logging
import os
import uuid

from .common import block_run


logger = logging.getLogger(__name__)


class _HandleQueryPublisher(object):
    def __init__(self, discoverer, reply_to):
        self.discoverer = discoverer
        self.reply_to = reply_to

    def __call__(self, storage, discovery_meta):
        self.discoverer.record_dataset(storage, discovery_meta,
                                       bind=self.reply_to)


class Discoverer(object):
    _async = False

    def __init__(self, identifier, concurrent=1):
        self.work_tickets = asyncio.Semaphore(concurrent)
        self.identifier = identifier
        self.loop = asyncio.get_event_loop()
        self.loop.create_task(self._run())

    async def _amqp_setup(self):
        # Setup the queries exchange
        exchange = await self.channel.declare_exchange(
            'queries',
            aio_pika.ExchangeType.FANOUT)

        # Declare our discoverer's query queue
        self.query_queue = await self.channel.declare_queue(
            'queries.%s' % self.identifier,
            auto_delete=True)
        await self.query_queue.bind(exchange)

        # Declare our discoverer's materialization queue
        self.materialize_queue = await self.channel.declare_queue(
            'materializes.%s' % self.identifier,
            auto_delete=True)

        # Setup the datasets exchange
        self.datasets_exchange = await self.channel.declare_exchange(
            'datasets',
            aio_pika.ExchangeType.TOPIC)

    async def _run(self):
        connection = await aio_pika.connect_robust(
            host=os.environ['AMQP_HOST'],
            login=os.environ['AMQP_USER'],
            password=os.environ['AMQP_PASSWORD'],
        )
        self.channel = await connection.channel()
        await self.channel.set_qos(prefetch_count=1)

        await self._amqp_setup()

        # Start ingestion process
        self._call(self.main_loop)

        # FIXME: The semaphore is only acquired when a message is received,
        # which means we might block while holding it. But if I acquire it
        # before, we can only be listening to one queue at a time.
        if hasattr(self, 'handle_query'):
            self.loop.create_task(self._consume_queries())
        self.loop.create_task(self._consume_materializes())

    async def _consume_queries(self):
        async for message in self.query_queue:
            await self.work_tickets.acquire()
            obj = json.loads(message.body)

            # Let the requester know that we are working on it
            await self.channel.default_exchange.publish(
                aio_pika.Message(json.dumps(dict(
                    work_started=self.identifier,
                ))),
                message.reply_to,
            )

            # Call handle_query
            logger.info("Handling query")
            future = self._call(self.handle_query, obj,
                                _HandleQueryPublisher(self.channel,
                                                      message.reply_to))
            future.add_done_callback(
                self._handle_query_callback(message)
            )

            await self.work_tickets.acquire()

    def _handle_query_callback(self, message):
        async def coro(future):
            try:
                future.result()
            except Exception:
                logger.exception("Error handling query")
                # Ack anyway, retrying would probably fail again
                # The message only gets re-queued if this process gets killed
                message.ack()
            else:
                # Let the requester know that we are done working on this
                await self.channel.default_exchange.publish(
                    aio_pika.Message(json.dumps(dict(
                        work_done=self.identifier,
                    ))),
                    message.reply_to,
                )

                message.ack()
                logger.info("Query handled successfully")

        def callback(future):
            self.work_tickets.release()
            self.loop.create_task(coro(future))

        return callback

    async def _consume_materializes(self):
        async for message in self.materialize_queue:
            await self.work_tickets.acquire()
            obj = json.loads(message.body)
            discovery_meta = obj['discovery']

            # Call handle_materialize
            logger.info("Handling materialization")
            future = self._call(self.handle_materialize,
                                discovery_meta)
            future.add_done_callback(
                self._handle_materialize_callback(message)
            )

            await self.work_tickets.acquire()

    def _handle_materialize_callback(self, message):
        async def coro(future):
            try:
                storage = future.result()
            except Exception:
                logger.exception("Error materializing")
                # Ack anyway, retrying would probably fail again
                # The message only gets re-queued if this process gets killed
                message.ack()

                await self.channel.default_exchange.publish(
                    aio_pika.Message(json.dumps(dict(
                        success=False,
                    ))),
                    message.reply_to,
                )
            else:
                await self.channel.default_exchange.publish(
                    aio_pika.Message(json.dumps(dict(
                        success=True,
                        storage=storage.to_json(),
                    )))
                )

        def callback(future):
            self.work_tickets.release()
            self.loop.create_task(coro(future))

        return callback

    def _call(self, method, *args):
        if self._async:
            return self.loop.create_task(
                method(*args),
            )
        else:
            return self.loop.run_in_executor(
                None,
                method,
                *args,
            )

    async def main_loop(self):
        pass

    # def handle_query(self, query)

    async def handle_materialize(self):
        raise NotImplementedError

    async def _record_dataset(self, storage, discovery_meta,
                              bind=None):
        dataset_id = uuid.uuid4().hex

        # Bind the requester's reply queue to the datasets exchange, with the
        # right routing_key, so that he receives the ingestion result for the
        # dataset
        if bind is not None:
            reply_queue = await self.channel.declare_queue(bind, passive=True)
            await reply_queue.bind(self.datasets_exchange, dataset_id)

        # Publish this dataset to the ingestion queue
        discovery_meta = dict(discovery_meta,
                              date=datetime.utcnow().isoformat() + 'Z')
        await self.channel.default_exchange.publish(
            aio_pika.Message(json.dumps(dict(
                id=dataset_id,
                storage=storage.to_json(),
                discovery_meta=discovery_meta,
            ))),
            'ingest',
        )
        return dataset_id

    def record_dataset(self, storage, discovery_meta, bind=None):
        coro = self._record_dataset(storage, discovery_meta, bind)
        if self._async:
            return self.loop.create_task(coro)
        else:
            return block_run(self.loop, coro)


class AsyncDiscoverer(Discoverer):
    _async = True

    def main_loop(self):
        pass
