import logging
import os
import uuid


logger = logging.getLogger(__name__)


class Coordinator(object):
    def __init__(self):
        self.discoverers = {}
        self.ingesters = {}
        self.storage = {}
        self.storage_r = {}
        self.recent_discoveries = []

    def add_discoverer(self, identifier, obj):
        self.discoverers.setdefault(identifier, set()).add(obj)

    def remove_discoverer(self, identifier, obj):
        s = self.discoverers[identifier]
        s.discard(obj)
        if not s:
            del self.discoverers[identifier]

    def add_ingester(self, identifier, obj):
        self.ingesters.setdefault(identifier, set()).add(obj)

    def remove_ingester(self, identifier, obj):
        s = self.ingesters[identifier]
        s.discard(obj)
        if not s:
            del self.ingesters[identifier]

    def discovered(self, identifier, dataset_id, dataset_meta):
        logger.info("Dataset discovered: %r (%r)", dataset_id, identifier)
        self.recent_discoveries.insert(0, dataset_id)
        del self.recent_discoveries[15:]

    def downloaded(self, identifier, dataset_id, storage_path):
        logger.info("Dataset downloaded: %r %r (%r)", dataset_id, storage_path,
                    identifier)
        self.storage[storage_path] = dataset_id, []
        self.storage_r[dataset_id] = storage_path
        # TODO: Notify ingesters

    def ingested(self, identifier, dataset_id, ingest_id, ingest_meta):
        logger.info("Dataset ingested: %r %r (%r)", dataset_id, ingest_id,
                    identifier)
        if dataset_id in self.storage_r:
            self.storage[self.storage_r[dataset_id]][1].append(identifier)

    def allocate_shared(self, identifier):
        name = str(uuid.uuid4())
        path = '/datasets/%s' % name
        self.storage[path] = None
        os.mkdir(path)
        logger.info("Dataset storage requested (%r): %r", identifier, path)
        return path
