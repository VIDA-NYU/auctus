import asyncio
import logging
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
    meta = {
        'filename': 'nyu.zip',
        'is_example': True,
    }

    def run(self):
        # State-of-the-art search process
        time.sleep(5)

        # We found a dataset
        dataset_id = self.record_dataset('storage path here',
                                         self.meta)

        # TODO: Download it

    def handle_materialization(self, dataset_id, meta):
        if meta['filename'] == 'nyu.zip':
            pass  # TODO


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    ExampleDiscoverer('org.datadrivendiscovery.example.discoverer')
    asyncio.get_event_loop().run_forever()
