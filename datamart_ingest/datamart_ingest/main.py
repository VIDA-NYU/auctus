import aio_pika
import asyncio
from datetime import datetime
import elasticsearch
import json
import logging
import os

from datamart_core.common import Storage
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

        asyncio.get_event_loop().create_task(self._run())

    async def _amqp_setup(self):
        # Setup the exchange
        self.datasets_exchange = await self.channel.declare_exchange(
            'datasets',
            aio_pika.ExchangeType.TOPIC)

        # Declare ingestion queue
        self.ingest_queue = await self.channel.declare_queue('ingest')

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
            obj = json.loads(message.body)
            dataset_id = obj['id']
            storage = Storage(obj['storage'])
            discovery_meta = obj['discovery']

            # Call handle_dataset
            logger.info("Handling dataset %r from %r",
                        dataset_id, discovery_meta.get('discoverer'))
            future = asyncio.get_event_loop().run_in_executor(
                handle_dataset,
                storage,
                discovery_meta,
            )

            future.add_done_callback(
                self.handle_dataset_callback(
                    message, dataset_id, discovery_meta,
                )
            )
            await self.work_tickets.acquire()

    def handle_dataset_callback(self, message, dataset_id, discovery_meta):
        async def coro(future):
            try:
                ingest_meta = future.result()
            except Exception:
                logger.exception("Error handling dataset %r", dataset_id)
                # Ack anyway, retrying would probably fail again
                # The message only gets re-queued if this process gets killed
                message.ack()
            else:
                # Insert results in Elasticsearch
                body = dict(ingest_meta,
                            discovery=discovery_meta,
                            date=datetime.utcnow().isoformat() + 'Z')
                self.es.index(
                    'datamart',
                    '_doc',
                    body,
                    id=dataset_id,
                )
                # Publish to RabbitMQ
                await self.datasets_exchange.publish(
                    aio_pika.Message(json.dumps(dict(body, id=dataset_id))),
                    dataset_id,
                )

                message.ack()
                logger.info("Dataset %r processed successfully", dataset_id)

        def callback(future):
            self.work_tickets.release()
            asyncio.get_event_loop().create_task(coro(future))

        return callback


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")
    Ingester()
    asyncio.get_event_loop().run_forever()


if __name__ == '__main__':
    main()
