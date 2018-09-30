import asyncio
import logging
import os
import time

from datamart_core import SimpleDiscoverer


logger = logging.getLogger(__name__)


class ExampleDiscoverer(SimpleDiscoverer):
    """Example discovery plugin.
    """
    data = ('name,country\n'
            'Remi,France\n'
            'Heiko,Australia\n'
            'Fernando,Brazil\n'
            'Juliana,USA\n')
    meta = {
        'filename': 'nyu.zip',
        'is_example': True,
    }

    def do_dataset_download(self, dataset_id):
        """Example download: just write hard-coded CSV to disk.
        """
        # Download dataset to storage
        storage = self.create_shared_storage()
        with open(os.path.join(storage.path, 'countries.csv'), 'w') as fp:
            fp.write(self.data)

        # Record it
        self.dataset_downloaded(dataset_id, storage)

    def run(self):
        # State-of-the-art search process
        time.sleep(5)

        # We found a dataset
        dataset_id = self.dataset_found(self.meta)

        # Download it
        self.do_dataset_download(dataset_id)

    def handle_materialization(self, dataset_id, meta):
        if meta['filename'] == 'nyu.zip':
            self.do_dataset_download(dataset_id)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    ExampleDiscoverer('org.datadrivendiscovery.example.discoverer')
    asyncio.get_event_loop().run_forever()
