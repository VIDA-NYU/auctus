import aio_pika
import asyncio
from datetime import datetime
import elasticsearch
import itertools
import logging
import os
import shutil
import time

from datamart_core.common import Storage, log_future, json2msg, msg2json
from .profiler import handle_dataset


logger = logging.getLogger(__name__)


MAX_CONCURRENT = 2


class Ingester(object):
    def __init__(self):
        self.work_tickets = asyncio.Semaphore(MAX_CONCURRENT)
        self.es = elasticsearch.Elasticsearch(
            os.environ['ELASTICSEARCH_HOSTS'].split(',')
        )
        self.channel = None

        self.loop = asyncio.get_event_loop()
        log_future(self.loop.create_task(self._run()), logger,
                   should_never_exit=True)

        # Retry a few times, in case the Elasticsearch container is not yet up
        for i in itertools.count():
            try:
                if not self.es.indices.exists('datamart'):
                    logger.info("Creating 'datamart' index in Elasticsearch")
                    self.es.indices.create(
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
                                            },
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

    async def _amqp_setup(self):
        # Setup the datasets exchange
        self.datasets_exchange = await self.channel.declare_exchange(
            'datasets',
            aio_pika.ExchangeType.TOPIC)

        # Setup the ingest exchange
        self.ingest_exchange = await self.channel.declare_exchange(
            'ingest',
            aio_pika.ExchangeType.FANOUT,
        )

        # Declare the ingestion queue
        self.ingest_queue = await self.channel.declare_queue('ingest')
        await self.ingest_queue.bind(self.ingest_exchange)

    async def _run(self):
        connection = await aio_pika.connect_robust(
            host=os.environ['AMQP_HOST'],
            login=os.environ['AMQP_USER'],
            password=os.environ['AMQP_PASSWORD'],
        )
        self.channel = await connection.channel()
        await self.channel.set_qos(prefetch_count=1)

        await self._amqp_setup()

        # Consume ingestion queue
        await self.work_tickets.acquire()
        async for message in self.ingest_queue:
            obj = msg2json(message)
            dataset_id = obj['id']
            storage = Storage(obj['storage'])
            metadata = obj['metadata']
            materialize = metadata.pop('materialize', {})

            # Call handle_dataset
            logger.info("Handling dataset %r from %r",
                        dataset_id, materialize.get('identifier', '(unknown)'))
            future = self.loop.run_in_executor(
                None,
                handle_dataset,
                storage,
                metadata,
            )

            future.add_done_callback(
                self.handle_dataset_callback(
                    message, dataset_id, materialize, storage,
                )
            )
            await self.work_tickets.acquire()

    def handle_dataset_callback(self, message, dataset_id, materialize,
                                storage):
        async def coro(future):
            try:
                metadata = future.result()
            except Exception:
                logger.exception("Error handling dataset %r", dataset_id)
                # Ack anyway, retrying would probably fail again
                # The message only gets re-queued if this process gets killed
                message.ack()
            else:
                # Insert results in Elasticsearch
                body = dict(metadata,
                            materialize=materialize,
                            date=datetime.utcnow().isoformat() + 'Z')
                self.es.index(
                    'datamart',
                    '_doc',
                    body,
                    id=dataset_id,
                )
                # Publish to RabbitMQ
                await self.datasets_exchange.publish(
                    json2msg(dict(body, id=dataset_id)),
                    dataset_id,
                )

                message.ack()
                logger.info("Dataset %r processed successfully", dataset_id)

        def callback(future):
            self.work_tickets.release()
            if os.path.isdir(storage.path):
                shutil.rmtree(storage.path)
            else:
                os.remove(storage.path)
            log_future(self.loop.create_task(coro(future)), logger)

        return callback


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")
    Ingester()
    asyncio.get_event_loop().run_forever()


if __name__ == '__main__':
    main()
