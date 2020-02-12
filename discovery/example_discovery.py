import asyncio
import logging
import os
import time

from datamart_core import Discoverer


logger = logging.getLogger(__name__)


class ExampleDiscoverer(Discoverer):
    """Example discovery plugin.
    """
    data = (b'name,country\n'
            b'Remi,France\n'
            b'Heiko,Australia\n'
            b'Fernando,Brazil\n'
            b'Juliana,USA\n')
    metadata = {
        'is_example': True,
        'name': "Example CSV",
    }
    dataset_id = 'example_nyu'

    def main_loop(self):
        # State-of-the-art search process
        logger.info("Searching...")
        time.sleep(5)

        # Write file to shared storage
        with self.write_to_shared_storage(self.dataset_id) as f_dst:
            f_dst.write(self.data)

        # We found a dataset
        self.record_dataset(
            dict(filename='nyu.csv'),  # Materialization information
            self.metadata,  # Metadata (profiler will update it)
            dataset_id=self.dataset_id,
        )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")
    ExampleDiscoverer('datamart.example')
    asyncio.get_event_loop().run_forever()
