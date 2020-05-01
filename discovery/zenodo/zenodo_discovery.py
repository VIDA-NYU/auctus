import asyncio
from datetime import datetime, timedelta
import logging
import time

from datamart_core import Discoverer
from datamart_core.common import setup_logging


logger = logging.getLogger(__name__)


class ZenodoDiscoverer(Discoverer):
    CHECK_INTERVAL = timedelta(days=1)

    def main_loop(self):
        while True:
            now = datetime.utcnow()

            try:
                self.get_datasets()
            except Exception:
                logger.exception("Error getting datasets")

            sleep_until = now + self.CHECK_INTERVAL
            logger.info("Sleeping until %s", sleep_until.isoformat())
            while datetime.utcnow() < sleep_until:
                time.sleep((sleep_until - datetime.utcnow()).total_seconds())

    def get_datasets(self):
        TODO


if __name__ == '__main__':
    setup_logging()
    ZenodoDiscoverer('datamart.zenodo')
    asyncio.get_event_loop().run_forever()
