import aio_pika
import asyncio
from datetime import datetime
import elasticsearch
import itertools
import lazo_index_service
import logging
import os
import prometheus_client
import time
import xlrd

from datamart_core.common import log_future, json2msg, msg2json
from datamart_core.materialize import get_dataset
from datamart_materialize.excel import xls_to_csv
from datamart_profiler import process_dataset


logger = logging.getLogger(__name__)


MAX_CONCURRENT = 2


def materialize_and_process_dataset(dataset_id, metadata, lazo_client):
    with get_dataset(metadata, dataset_id) as dataset_path:
        materialize = metadata.pop('materialize')

        # Check for Excel file format
        try:
            xlrd.open_workbook(dataset_path)
        except xlrd.XLRDError:
            pass
        else:
            logger.info("This is an Excel file")
            materialize.setdefault('convert', []).append({'identifier': 'xls'})
            os.rename(dataset_path, dataset_path + '.xls')
            with open(dataset_path, 'w', newline='') as dst:
                xls_to_csv(dataset_path + '.xls', dst)

        # Profile
        start = time.perf_counter()
        metadata = process_dataset(
            data=dataset_path,
            metadata=metadata,
            lazo_client=lazo_client,
            dataset_id=dataset_id
        )
        logger.info("Profiling took %.2fs", time.perf_counter() - start)

        metadata['materialize'] = materialize
        return metadata


class Profiler(object):
    def __init__(self):
        self.work_tickets = asyncio.Semaphore(MAX_CONCURRENT)
        self.es = elasticsearch.Elasticsearch(
            os.environ['ELASTICSEARCH_HOSTS'].split(',')
        )
        self.lazo_client = lazo_index_service.LazoIndexClient(
            host=os.environ['LAZO_SERVER_HOST'],
            port=int(os.environ['LAZO_SERVER_PORT'])
        )
        self.channel = None

        self.loop = asyncio.get_event_loop()
        log_future(self.loop.create_task(self._run()), logger,
                   should_never_exit=True)

        # Retry a few times, in case the Elasticsearch container is not yet up
        for i in itertools.count():
            try:
                if not self.es.indices.exists('datamart'):
                    raise RuntimeError("'datamart' index does not exist")
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

        # Setup the profiling exchange
        self.profile_exchange = await self.channel.declare_exchange(
            'profile',
            aio_pika.ExchangeType.FANOUT,
        )

        # Declare the profiling queue
        self.profile_queue = await self.channel.declare_queue(
            'profile',
            arguments={'x-max-priority': 3},
        )
        await self.profile_queue.bind(self.profile_exchange)

        # Declare the failed queue
        self.failed_queue = await self.channel.declare_queue('failed_profile')

    async def _run(self):
        connection = await aio_pika.connect_robust(
            host=os.environ['AMQP_HOST'],
            login=os.environ['AMQP_USER'],
            password=os.environ['AMQP_PASSWORD'],
        )
        self.channel = await connection.channel()
        await self.channel.set_qos(prefetch_count=1)

        await self._amqp_setup()

        # Consume profiling queue
        await self.work_tickets.acquire()
        async for message in self.profile_queue:
            obj = msg2json(message)
            dataset_id = obj['id']
            metadata = obj['metadata']
            materialize = metadata.get('materialize', {})

            logger.info("Processing dataset %r from %r",
                        dataset_id, materialize.get('identifier'))

            future = self.loop.run_in_executor(
                None,
                materialize_and_process_dataset,
                dataset_id,
                metadata,
                self.lazo_client
            )

            future.add_done_callback(
                self.process_dataset_callback(
                    message, dataset_id,
                )
            )

            await self.work_tickets.acquire()

    def process_dataset_callback(self, message, dataset_id):
        async def coro(future):
            try:
                try:
                    metadata = future.result()
                except Exception:
                    logger.exception("Error processing dataset %r", dataset_id)
                    # Move message to failed queue
                    await self.channel.default_exchange.publish(
                        aio_pika.Message(message.body),
                        self.failed_queue.name,
                    )
                    # Ack anyway, retrying would probably fail again
                    message.ack()
                else:
                    # Insert results in Elasticsearch
                    body = dict(metadata,
                                date=datetime.utcnow().isoformat() + 'Z')
                    self.es.index(
                        'datamart',
                        '_doc',
                        body,
                        id=dataset_id,
                    )  # failed

                    # Publish to RabbitMQ
                    await self.datasets_exchange.publish(
                        json2msg(dict(body, id=dataset_id)),
                        dataset_id,
                    )

                    message.ack()
                    logger.info("Dataset %r processed successfully",
                                dataset_id)
            except Exception:
                message.nack()
                raise

        def callback(future):
            self.work_tickets.release()
            log_future(self.loop.create_task(coro(future)), logger)

        return callback


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")
    prometheus_client.start_http_server(8000)
    Profiler()
    asyncio.get_event_loop().run_forever()


if __name__ == '__main__':
    main()
