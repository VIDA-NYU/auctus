from datetime import datetime
import logging

from .common import Async, BaseHandler, WriteStorage, block_run


logger = logging.getLogger(__name__)


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

    def handle_materialization(self, dataset_id, meta):
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


class AsyncDiscoverer(Async):
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

    async def handle_materialization(self, dataset_id, meta):
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


class DiscovererHandler(BaseHandler):
    BASE_PLUGIN_CLASSES = (SimpleDiscoverer, AsyncDiscoverer)
    POLL_PATH = '/poll/discovery'

    def work_received(self, obj):
        if 'query' in obj:
            return self._call(self._obj.handle_ondemand_query,
                              obj['query'])
        elif 'materialize' in obj:
            return self._call(self._obj.handle_materialization,
                              obj['materialize']['id'],
                              obj['materialize']['meta'])

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
        async with self.post('/dataset_discovered', body) as resp:
            obj = await resp.json()
        return obj['dataset_id']

    def dataset_found_blocking(self, dataset_meta):
        return block_run(self.dataset_found(dataset_meta))

    async def create_shared_storage(self):
        async with self.get('/allocate_dataset') as resp:
            obj = await resp.json()
        return WriteStorage(obj)

    def create_shared_storage_blocking(self):
        return block_run(self.create_shared_storage())

    async def dataset_downloaded(self, dataset_id, storage):
        async with self.post('/dataset_downloaded', {
                'dataset_id': dataset_id,
                'storage_path': storage.path}):
            pass

    def dataset_downloaded_blocking(self, dataset_id, storage):
        return block_run(self.dataset_downloaded(dataset_id, storage))
