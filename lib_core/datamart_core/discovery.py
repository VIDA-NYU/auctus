import aio_pika
import asyncio
import contextlib
from datetime import datetime
import lazo_index_service
import logging
import os
import sentry_sdk
import sys
import uuid

from .common import PrefixedElasticsearch, block_run, json2msg, \
    encode_dataset_id, delete_dataset_from_index, strip_html
from .objectstore import get_object_store


logger = logging.getLogger(__name__)


class Discoverer(object):
    """Base class for discoverer plugins.
    """
    _async = False

    def __init__(self, identifier):
        self.identifier = identifier
        self.loop = asyncio.get_event_loop()

    async def _amqp_setup(self):
        # Setup the profiling exchange
        self.profile_exchange = await self.channel.declare_exchange(
            'profile',
            aio_pika.ExchangeType.FANOUT,
        )

        # Declare the profiling queue
        profile_queue = await self.channel.declare_queue(
            'profile',
            arguments={'x-max-priority': 3},
        )
        await profile_queue.bind(self.profile_exchange)

    async def run(self):
        self.elasticsearch = PrefixedElasticsearch()
        self.lazo_client = lazo_index_service.LazoIndexClient(
            host=os.environ['LAZO_SERVER_HOST'],
            port=int(os.environ['LAZO_SERVER_PORT'])
        )

        connection = await aio_pika.connect_robust(
            host=os.environ['AMQP_HOST'],
            port=int(os.environ['AMQP_PORT']),
            login=os.environ['AMQP_USER'],
            password=os.environ['AMQP_PASSWORD'],
        )
        self.channel = await connection.channel()
        await self.channel.set_qos(prefetch_count=1)

        await self._amqp_setup()

        # Start profiling process
        try:
            await self._call(self.discover_datasets)
        except Exception as e:
            sentry_sdk.capture_exception(e)
            logger.exception("Exception in discoverer %s", self.identifier)
            sys.exit(1)

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

    def discover_datasets(self):
        pass

    async def _a_record_dataset(self, materialize, metadata,
                                dataset_id=None):
        if dataset_id is None:
            dataset_id = uuid.uuid4().hex
        dataset_id = self.identifier + '.' + dataset_id

        # Publish this dataset to the profiling queue
        metadata = dict(metadata,
                        materialize=dict(
                            materialize,
                            identifier=self.identifier,
                            date=datetime.utcnow().isoformat() + 'Z'))
        await self.profile_exchange.publish(
            json2msg(
                dict(
                    id=dataset_id,
                    metadata=metadata,
                ),
            ),
            '',
        )
        logger.info("Discovered %s", dataset_id)
        return dataset_id

    def record_dataset(self, materialize, metadata,
                       dataset_id=None):
        """Publish a found dataset.

        The dataset will be profiled if necessary and recorded in the index.
        """
        if 'name' not in metadata:
            metadata['name'] = dataset_id
        if 'source' not in metadata:
            metadata['source'] = self.identifier
        if 'description' in metadata:
            metadata['description'] = strip_html(metadata['description'])
        coro = self._a_record_dataset(materialize, metadata,
                                      dataset_id=dataset_id)
        if self._async:
            return self.loop.create_task(coro)
        else:
            return block_run(self.loop, coro)

    @contextlib.contextmanager
    def write_to_shared_storage(self, dataset_id):
        """Write a file to persistent storage.

        This is useful if there is no way to materialize this dataset again in
        the future, and you need to store it to refer to it. Materialization
        won't occur for datasets that are in shared storage already.
        """
        # TODO: Add a mechanism to clean datasets from storage
        object_store = get_object_store()
        key = encode_dataset_id(self.identifier + '.' + dataset_id)
        with object_store.open('datasets', key, 'wb') as fp:
            yield fp

    def delete_dataset(self, *, full_id=None, dataset_id=None):
        """Delete a dataset that is no longer present in the source.
        """
        if (full_id is not None) == (dataset_id is not None):
            raise TypeError("Pass only one of 'id' and 'full_id'")

        if full_id is None:
            full_id = self.identifier + '.' + dataset_id

        delete_dataset_from_index(
            self.elasticsearch,
            full_id,
            self.lazo_client,
        )

        # And the stored datasets
        object_store = get_object_store()
        object_store.delete('datasets', encode_dataset_id(full_id))


class AsyncDiscoverer(Discoverer):
    """Async variant of `Discoverer`.
    """
    _async = True

    async def discover_datasets(self):
        pass
