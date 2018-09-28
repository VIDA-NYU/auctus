import asyncio
from datetime import datetime
import elasticsearch
import json
import logging
import os

from .rabbitmq import RabbitMQ


logger = logging.getLogger(__name__)


class DiscovererHandler(RabbitMQ):
    def __init__(self, obj, identifier):
        super(DiscovererHandler, self).__init__()
        self.elasticsearch = elasticsearch.Elasticsearch(
            os.environ['ELASTICSEARCH_HOSTS'].split(',')
        )
        self._obj = obj
        self._identifier = identifier

    def on_channel_open(self):
        pass  # TODO: Listen for on-demand queries

    async def dataset_found(self, dataset_meta):
        dataset_meta = dict(dataset_meta,
                            kind='dataset',
                            date=datetime.utcnow().isoformat() + 'Z')
        dataset_id = self.elasticsearch.index(
            'datamart',
            '_doc',
            dataset_meta,
        )
        body = json.dumps({'id': dataset_id, 'meta': dataset_meta})
        self._amqp_channel.basic_publish('discovered', '', body)

    def create_dataset_storage(self):
        TODO send

    def dataset_downloaded(self, storage, dataset_meta):
        TODO send


class BaseDiscoverer(object):
    """Base class for a discovery plugin.

    A discovery plugin is in charge of the following:

    * Crawl the web looking for datasets, inserting dataset records in
      Elasticsearch, possibly materializing them on disk
    * Materialize a previously recorded dataset
    * React to a user query to perform on-demand crawling (optional)
    """
    def __init__(self, identifier):
        self._handler = DiscovererHandler(self, identifier)

    def handle_ondemand_query(self, query):
        """Query from a user, implement this to perform on-demand search.

        You can leave this alone if your discovery plugin doesn't do this.
        """
        raise NotImplementedError

    def dataset_found(self, dataset_meta):
        """Record that a dataset has been found.
        """
        return self._handler.dataset_found(dataset_meta)

    def handle_materialization(self, meta):
        """Materialization request.

        A dataset we previously found or downloaded is needed again. This
        method should fetch it from its original location, if possible.
        """
        raise NotImplementedError

    def create_dataset_storage(self):
        """Call this to get a folder where to write a dataset.
        """
        return self._handler.create_dataset_storage()

    def dataset_downloaded(self, storage, dataset_meta):
        """Record a dataset, after it's been acquired.
        """
        return self._handler.dataset_downloaded(storage, dataset_meta)
