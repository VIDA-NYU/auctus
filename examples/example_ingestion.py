import asyncio
import logging
import os

from datamart_core import SimpleIngester


logger = logging.getLogger(__name__)


class ExampleIngester(SimpleIngester):
    def handle_ingest(self, storage, dataset_id, dataset_meta):
        logger.info("INGESTING DATASET %r", dataset_id)

        # Compute size in bytes
        size = 0
        for root, dirs, files in os.walk(storage.path):
            for name in files:
                size += os.path.getsize(os.path.join(root, name))

        # Record metadata
        ingest_meta = {
            'size_bytes': size,
            'interesting': True,
            'foobar_score': 0.42,
            'keywords': ['interesting', 'data'],
        }
        self.record_metadata(dataset_id, ingest_meta)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    ExampleIngester('org.datadrivendiscovery.example.ingester')
    asyncio.get_event_loop().run_forever()
