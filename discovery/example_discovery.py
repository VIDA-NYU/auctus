import asyncio
import logging
import os
import time

from datamart_core import Discoverer


logger = logging.getLogger(__name__)


class ExampleDiscoverer(Discoverer):
    """Example discovery plugin.
    """
    data = ('name,country\n'
            'Remi,France\n'
            'Heiko,Australia\n'
            'Fernando,Brazil\n'
            'Juliana,USA\n')
    metadata = {
        'is_example': True,
        'name': "Example CSV",
    }

    def main_loop(self):
        # State-of-the-art search process
        logger.info("Searching...")
        time.sleep(5)

        # Download dataset to storage
        storage = self.create_storage()
        with open(os.path.join(storage.path, 'main.csv'), 'w') as fp:
            fp.write(self.data)

        # We found a dataset
        self.record_dataset(
            storage,  # Path in shared cache
            dict(filename='nyu.zip'),  # Materialization information
            self.metadata,  # Metadata (profiler will update it)
            dataset_id='example_nyu',
        )

    def handle_materialize(self, materialize):
        if materialize['filename'] == 'nyu.zip':
            storage = self.create_storage()
            with open(os.path.join(storage.path, 'main.csv'), 'w') as fp:
                fp.write(self.data)
            return storage


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")
    ExampleDiscoverer('datamart.example.example_discoverer')
    asyncio.get_event_loop().run_forever()
