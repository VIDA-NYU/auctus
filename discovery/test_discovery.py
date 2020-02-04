import asyncio
from http.server import HTTPServer, SimpleHTTPRequestHandler
import logging
import os
import shutil
import threading

from datamart_core import Discoverer


logger = logging.getLogger(__name__)


class TestDiscoverer(Discoverer):
    """Discovery plugin for the test suite.
    """
    def main_loop(self):
        # Put this one on disk
        with self.write_to_shared_storage('geo') as dirname:
            shutil.copy2('geo.csv', os.path.join(dirname, 'main.csv'))
        self.record_dataset(
            dict(),
            {
                # Omit name, should be set to 'geo' automatically
                'description': "Another simple CSV with places",
            },
            dataset_id='geo',
        )

        # Put this one on disk
        with self.write_to_shared_storage('agg') as dirname:
            shutil.copy2('agg.csv', os.path.join(dirname, 'main.csv'))
        self.record_dataset(
            dict(),
            {
                # Omit name, should be set to 'agg' automatically
                'description': "Simple CSV with ids and salaries to test"
                               " aggregation for numerical attributes",
            },
            dataset_id='agg',
        )

        # Use URL for this one
        self.record_dataset(
            dict(direct_url='http://test_discoverer:7000/lazo.csv'),
            {
                # Omit name, should be set to 'lazo' automatically
                'description': "Simple CSV with states and years"
                               " to test the Lazo index service",
            },
            dataset_id='lazo',
        )

        # Put this one on disk
        with self.write_to_shared_storage('daily') as dirname:
            shutil.copy2('daily.csv', os.path.join(dirname, 'main.csv'))
        self.record_dataset(
            dict(),
            {
                'name': 'daily',
                'description': "Temporal dataset with daily resolution",
            },
            dataset_id='daily',
        )

        # Use URL for this one
        self.record_dataset(
            dict(direct_url='http://test_discoverer:7000/hourly.csv'),
            {
                # Omit name, should be set to 'hourly' automatically
                'description': "Temporal dataset with hourly resolution",
            },
            dataset_id='hourly',
        )

        # Needs to be last, CI waits for it to test

        # Use URL for this one
        self.record_dataset(
            dict(direct_url='http://test_discoverer:7000/basic.csv'),
            {
                'name': "basic",
                'description': "This is a very simple CSV with people",
            },
            dataset_id='basic',
        )


def server():
    httpd = HTTPServer(('0.0.0.0', 7000), SimpleHTTPRequestHandler)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()


if __name__ == '__main__':
    os.chdir('tests/data')
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")

    # Start a web server
    server_thread = threading.Thread(target=server)
    server_thread.setDaemon(True)
    server_thread.start()

    TestDiscoverer('datamart.test')
    asyncio.get_event_loop().run_forever()
