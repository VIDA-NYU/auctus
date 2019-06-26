import asyncio
import logging

from datamart_core import Discoverer


logger = logging.getLogger(__name__)


class TestDiscoverer(Discoverer):
    """Discovery plugin for the test suite.
    """
    def main_loop(self):
        self.record_dataset(
            dict(direct_url='http://172.0.44.1:7000/basic.csv'),
            {'description': "This is a very simple CSV with people"},
            dataset_id='basic',
        )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")
    TestDiscoverer('datamart.test')
    asyncio.get_event_loop().run_forever()
