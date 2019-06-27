import asyncio
from http.server import HTTPServer, SimpleHTTPRequestHandler
import logging
import os
import threading

from datamart_core import Discoverer


logger = logging.getLogger(__name__)


class TestDiscoverer(Discoverer):
    """Discovery plugin for the test suite.
    """
    def main_loop(self):
        self.record_dataset(
            dict(direct_url='http://test_discoverer:7000/basic.csv'),
            {'description': "This is a very simple CSV with people"},
            dataset_id='basic',
        )


def server():
    with HTTPServer(('0.0.0.0', 7000), SimpleHTTPRequestHandler) as httpd:
        httpd.serve_forever()


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
