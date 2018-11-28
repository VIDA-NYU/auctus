import aio_pika
import asyncio
from datetime import datetime
import elasticsearch
from elasticsearch import helpers
import itertools
import logging
import os
import shutil
import time

from datamart_core.common import Storage, log_future, json2msg, msg2json
from .profiler import handle_dataset


logger = logging.getLogger(__name__)


MAX_CONCURRENT = 2


class Profiler(object):
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
                    raise RuntimeError("'datamart' index does not exist")
            except Exception:
                logger.warning("Can't connect to Elasticsearch, retrying...")
                if i == 5:
                    raise
                else:
                    time.sleep(5)
            else:
                break

        # Put mapping for datetime index
        logger.info("Creating datetime index...")
        if not self.es.indices.exists('datamart_datetime_index'):
            self.es.indices.create(index='datamart_datetime_index')

            mapping = '''
            {
              "properties": {
                "name": {
                  "type": "text"
                },
                "id" : {
                  "type": "text"
                },
                "time_frame": {
                  "type": "long_range"
                }
              }
            }'''

            try:
                self.es.indices.put_mapping(
                    index='datamart_datetime_index',
                    body=mapping,
                    doc_type='_doc'
                )
            except Exception as e:
                logger.warning("Not able to create datetime index: " + e)

    def generator_es_datetime_doc(self, column_name, data_id, ranges):
        """
        Generator for adding datetime ranges in bulk to elasticsearch.
        """

        for range_ in ranges:
            yield {
                "_op_type": "index",
                "_index": "datamart_datetime_index",
                "_type": "_doc",
                "name": column_name,
                "id": data_id,
                "time_frame": {
                    "gte": int(range_[0]),
                    "lte": int(range_[1])
                }
            }

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
        self.profile_queue = await self.channel.declare_queue('profile')
        await self.profile_queue.bind(self.profile_exchange)

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
                metadata, temporal_index = future.result()
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

                # Add temporal indices, if any
                for column_name in dict(temporal_index):

                    try:
                        helpers.bulk(
                            self.es,
                            self.generator_es_datetime_doc(
                                column_name,
                                dataset_id,
                                temporal_index[column_name]
                            )
                        )
                    except Exception as e:
                        logger.warning("Not able to add ranges to datetime index: {0}".format(e))

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
    Profiler()
    asyncio.get_event_loop().run_forever()


if __name__ == '__main__':
    main()
