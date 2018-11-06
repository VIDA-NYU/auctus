import asyncio
import logging

from datamart_core import Discoverer


logger = logging.getLogger(__name__)


class SocrataDiscoverer(Discoverer):
    def main_loop(self):
        pass  # TODO


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")
    SocrataDiscoverer('datamart.socrata_discoverer')
    asyncio.get_event_loop().run_forever()
