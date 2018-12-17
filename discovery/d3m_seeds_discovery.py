import asyncio
import json
import logging
import os

from datamart_core import Discoverer


logger = logging.getLogger(__name__)


class D3MSeedsDiscoverer(Discoverer):
    """Discoverer that just "finds" the D3M datasets, *slowly*.
    """
    def main_loop(self):
        datasets = os.listdir('/d3m_seed_datasets')
        logger.info("Got %d folders to go through...", len(datasets))

        for name in datasets:
            dir_name = os.path.join(
                '/d3m_seed_datasets',
                name,
                name + '_dataset',
            )
            csv_name = os.path.join(dir_name, 'tables', 'learningData.csv')
            if not os.path.exists(csv_name):
                logger.info("Doesn't exist: %r", csv_name)
                continue
            logger.info("Discovering: %r", csv_name)

            json_name = os.path.join(dir_name, 'datasetDoc.json')
            with open(json_name) as fp:
                doc = json.load(fp)['about']
            metadata = {}
            if 'datasetName' in doc:
                metadata['name'] = doc['datasetName']
            if 'description' in doc:
                metadata['description'] = doc['description']
            if 'license' in doc:
                metadata['license'] = doc['license']

            # Write symlink to shared storage
            with self.write_to_shared_storage(name) as dirname:
                os.symlink(csv_name, os.path.join(dirname, 'main.csv'))

            self.record_dataset(dict(d3m_name=name),
                                metadata,
                                dataset_id=name)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")
    D3MSeedsDiscoverer('datamart.d3m')
    asyncio.get_event_loop().run_forever()
