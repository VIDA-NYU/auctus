import asyncio
import aiohttp
from datetime import datetime
import elasticsearch
import logging
import os

from .utils import block_run


logger = logging.getLogger(__name__)


class Storage(object):
    def __init__(self, obj):
        self.path = obj['path']
        self.max_size_bytes = obj.get('max_size_bytes')

    def __repr__(self):
        return '<Storage %r%s>' % (
            self.path,
            ' max_size_bytes=%r' % self.max_size_bytes
            if self.max_size_bytes else ''
        )


class DiscovererHandler(object):
    def __init__(self, obj, identifier, concurrent):
        if isinstance(obj, SimpleDiscoverer):
            self._async = False
        elif isinstance(obj, AsyncDiscoverer):
            self._async = True
        else:
            raise TypeError("Discoverer is not a SimpleDiscoverer nor an "
                            "AsyncDiscoverer")
        self._obj = obj
        self._identifier = identifier
        self.elasticsearch = elasticsearch.Elasticsearch(
            os.environ['ELASTICSEARCH_HOSTS'].split(',')
        )
        self.coordinator = os.environ['COORDINATOR_URL'].rstrip('/')
        self._work = asyncio.Semaphore(concurrent)
        self.http_session = aiohttp.ClientSession()
        asyncio.get_event_loop().create_task(self._request_work())
        if self._async:
            asyncio.get_event_loop().create_task(self._obj.run())
        else:
            asyncio.get_event_loop().run_in_executor(None, self._obj.run)

    def _call(self, method, *args):
        if self._async:
            return asyncio.get_event_loop().create_task(
                method(*args),
            )
        else:
            return asyncio.get_event_loop().run_in_executor(
                None,
                method,
                *args,
            )

    async def _request_work(self):
        url = self.coordinator + '/poll/discovery'
        while True:
            await self._work.acquire()
            async with self.http_session.get(url) as resp:
                obj = await resp.json()
            if 'query' in obj:
                future = self._call(self._obj.handle_ondemand_query,
                                    obj['query'])
            elif 'materialize' in obj:
                future = self._call(self._obj.handle_materialization,
                                    obj['materialize'])
            else:
                logger.error("Got unknown request from coordinator")
                self._work.release()
                await asyncio.sleep(5)
                continue
            future.add_done_callback(self._work_done)

    def _work_done(self, future):
        self._work.release()

    async def dataset_found(self, dataset_meta):
        dataset_meta = dict(dataset_meta,
                            kind='dataset',
                            date=datetime.utcnow().isoformat() + 'Z')
        dataset_id = self.elasticsearch.index(
            'datamart',
            '_doc',
            dataset_meta,
        )
        body = {'id': dataset_id, 'meta': dataset_meta}
        url = self.coordinator + '/dataset_discovered'
        async with self.http_session.post(url, json=body) as resp:
            obj = await resp.json()
        return obj['dataset_id']

    def dataset_found_blocking(self, dataset_meta):
        return block_run(self.dataset_found(dataset_meta))

    async def create_shared_storage(self):
        url = self.coordinator + '/allocate_dataset'
        async with self.http_session.get(url) as resp:
            obj = await resp.json()
        return Storage(obj)

    def create_shared_storage_blocking(self):
        return block_run(self.create_shared_storage())

    async def dataset_downloaded(self, dataset_id, storage):
        url = self.coordinator + '/dataset_downloaded'
        async with self.http_session.post(url, json={
                    'dataset_id': dataset_id,
                    'storage_path': storage.path,
                }):
            pass

    def dataset_downloaded_blocking(self, dataset_id, storage):
        return block_run(self.dataset_downloaded(dataset_id, storage))


class SimpleDiscoverer(object):
    """Base class for a discovery plugin.

    A discovery plugin is in charge of the following:

    * Crawl the web looking for datasets, inserting dataset records in
      Elasticsearch, possibly materializing them on disk
    * Materialize a previously recorded dataset
    * React to a user query to perform on-demand crawling (optional)
    """
    def __init__(self, identifier, concurrent=1):
        self._handler = DiscovererHandler(self, identifier, concurrent)

    def handle_ondemand_query(self, query):
        """Query from a user, implement this to perform on-demand search.

        You can leave this alone if your discovery plugin doesn't do this.

        Called in a thread pool.
        """

    def dataset_found(self, dataset_meta):
        """Record that a dataset has been found.
        """
        return self._handler.dataset_found_blocking(dataset_meta)

    def handle_materialization(self, meta):
        """Materialization request.

        A dataset we previously found or downloaded is needed again. This
        method should fetch it from its original location, if possible.

        Called in a thread pool.
        """
        raise NotImplementedError

    def create_shared_storage(self):
        """Call this to get a folder where to write a dataset.
        """
        return self._handler.create_shared_storage_blocking()

    def dataset_downloaded(self, dataset_id, storage):
        """Record a dataset, after it's been acquired.
        """
        return self._handler.dataset_downloaded_blocking(dataset_id, storage)

    def run(self):
        """Entrypoint for the discovery plugin.

        Crawl, poll, search, and call `dataset_found()` and
        `dataset_downloaded()` to record found datasets.

        If this method is not implemented, the plugin will simply wait for
        requests; `handle_ondemand_query()` and `handle_materialization()` will
        be called when needed in a thread pool.
        """


class AsyncDiscoverer(object):
    """Base class for an asynchronous discovery plugin.

    A discovery plugin is in charge of the following:

    * Crawl the web looking for datasets, inserting dataset records in
      Elasticsearch, possibly materializing them on disk
    * Materialize a previously recorded dataset
    * React to a user query to perform on-demand crawling (optional)
    """
    def __init__(self, identifier, concurrent=1):
        self._handler = DiscovererHandler(self, identifier, concurrent)

    async def handle_ondemand_query(self, query):
        """Query from a user, implement this to perform on-demand search.

        You can leave this alone if your discovery plugin doesn't do this.
        """

    def dataset_found(self, dataset_meta):
        """Record that a dataset has been found.
        """
        return self._handler.dataset_found(dataset_meta)

    async def handle_materialization(self, meta):
        """Materialization request.

        A dataset we previously found or downloaded is needed again. This
        method should fetch it from its original location, if possible.
        """
        raise NotImplementedError

    def create_shared_storage(self):
        """Call this to get a folder where to write a dataset.
        """
        return self._handler.create_shared_storage()

    def dataset_downloaded(self, dataset_id, storage):
        """Record a dataset, after it's been acquired.
        """
        return self._handler.dataset_downloaded(dataset_id, storage)
