import logging
import os
import uuid


logger = logging.getLogger(__name__)


class Coordinator(object):
    def __init__(self):
        self.discoverers = {}
        self.ingesters = {}

    def add_discoverer(self, identifier, obj):
        self.discoverers.setdefault(identifier, set()).add(obj)

    def remove_discoverer(self, identifier, obj):
        self.discoverers[identifier].discard(obj)

    def add_ingester(self, identifier, obj):
        self.ingesters.setdefault(identifier, set()).add(obj)

    def remove_ingester(self, identifier, obj):
        self.ingesters[identifier].discard(obj)

    def discovered(self, identifier, dataset_id, dataset_meta):
        logger.info("Dataset discovered: %r (%r)", dataset_id, identifier)
        pass  # TODO

    def downloaded(self, identifier, dataset_id, storage_path):
        logger.info("Dataset downloaded: %r %r (%r)", dataset_id, storage_path,
                    identifier)
        pass  # TODO

    def ingested(self, identifier, dataset_id, ingest_id, ingest_meta):
        logger.info("Dataset ingested: %r %r (%r)", dataset_id, ingest_id,
                    identifier)
        pass  # TODO

    def allocate_shared(self, identifier):
        name = str(uuid.uuid4())
        path = '/datasets/%s' % name
        os.mkdir(path)
        logger.info("Dataset storage requested (%r): %r", identifier, path)
        return path
