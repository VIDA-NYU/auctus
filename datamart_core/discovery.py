import json
import os

class DiscovererHandler(object):
    def __init__(self, obj, identifier):
        self._obj = obj
        self._identifier = identifier
        self._config = json.loads(os.environ['DATAMART_CONFIG'])

    def do_health(self):
        TODO recv

    def dataset_found(self, dataset_meta):
        TODO send

    def create_dataset_storage(self):
        TODO send

    def dataset_downloaded(self, storage, dataset_meta):
        TODO send


class BaseDiscoverer(object):
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
