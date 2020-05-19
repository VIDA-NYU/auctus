import asyncio
from datetime import datetime, timedelta
import elasticsearch.helpers
import logging
import os
import requests
import sqlite3
import tempfile
import time

from datamart_core import Discoverer
from datamart_core.common import setup_logging


logger = logging.getLogger(__name__)


class UazIndicatorsDiscoverer(Discoverer):
    CHECK_INTERVAL = timedelta(days=1)

    def main_loop(self):
        while True:
            now = datetime.utcnow()

            try:
                self.get_data()
            except Exception:
                logger.exception("Error getting datasets")

            sleep_until = now + self.CHECK_INTERVAL
            logger.info("Sleeping until %s", sleep_until.isoformat())
            while datetime.utcnow() < sleep_until:
                time.sleep((sleep_until - datetime.utcnow()).total_seconds())

    def get_data(self):
        # Get current E-Tag
        try:
            info = self.elasticsearch.get(
                'pending',
                self.identifier,
            )['_source']
        except elasticsearch.NotFoundError:
            etag = None
        else:
            etag = info['etag']

        # Do HTTP request
        headers = {
            'User-Agent': 'Auctus/%s' % os.environ['DATAMART_VERSION'],
        }
        if etag:
            headers['If-None-Match'] = etag
        response = requests.get(
            'http://vanga.sista.arizona.edu/delphi_data/delphi.db',
            headers=headers,
            stream=True,
        )
        response.raise_for_status()
        if response.status_code == 304:
            logger.info("File hasn't changed")
            return

        with tempfile.NamedTemporaryFile(suffix='.sqlite3') as tmp:
            for chunk in response.iter_content(4096):
                if chunk:  # filter out keep-alive chunks
                    tmp.write(chunk)

            self.discover_indicators(tmp.name)
            self.discover_dssat(tmp.name)

        self.elasticsearch.index(
            'pending',
            {'etag': response.headers.get('ETag')},
            id=self.identifier,
        )

    def discover_indicators(self, filename):
        pass  # TODO

    def discover_dssat(self, filename):
        pass  # TODO


if __name__ == '__main__':
    setup_logging()
    UazIndicatorsDiscoverer('datamart.uaz-indicators')
    asyncio.get_event_loop().run_forever()
