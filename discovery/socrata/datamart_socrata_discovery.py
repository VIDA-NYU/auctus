import asyncio
from datetime import datetime, timedelta
import elasticsearch
import json
import logging
import os
import requests
import sodapy
import time

from datamart_core import Discoverer


logger = logging.getLogger(__name__)


class SocrataDiscoverer(Discoverer):
    DEFAULT_DOMAINS = [
        {'url': 'data.cityofnewyork.us'},
    ]
    CHECK_INTERVAL = timedelta(days=1)

    def __init__(self, *args, **kwargs):
        super(SocrataDiscoverer, self).__init__(*args, **kwargs)

        if os.path.exists('socrata.json'):
            with open('socrata.conf') as fp:
                self.domains = json.load(fp)
            logger.info("Loaded %d domains from socrata.conf",
                        len(self.domains))
        else:
            self.domains = self.DEFAULT_DOMAINS
            logger.info("Using default domains")

        self.last_update = {}

    def main_loop(self):
        while True:
            sleep_until = None
            now = datetime.utcnow()
            for domain in self.domains:
                last_update = self.last_update.get(domain['url'])
                interval = domain.get('check_interval', self.CHECK_INTERVAL)
                if last_update is None or last_update + interval < now:
                    try:
                        self.process_domain(domain)
                    except Exception:
                        logger.exception("Error processing %s", domain['url'])
                    self.last_update[domain['url']] = now
                    if sleep_until is None or sleep_until > now + interval:
                        sleep_until = now + interval

            while datetime.utcnow() < sleep_until:
                time.sleep((sleep_until - datetime.utcnow()).total_seconds())

    def process_domain(self, domain):
        logger.info("Processing %s...", domain['url'])
        socrata = sodapy.Socrata(domain['url'],
                                 **domain.get('auth', {'app_token': None}))
        datasets = socrata.datasets()
        logger.info("Found %d datasets", len(datasets))
        for dataset in datasets:
            try:
                self.process_dataset(domain, dataset)
            except Exception:
                logger.exception("Error processing dataset %s",
                                 dataset['resource']['id'])

    def process_dataset(self, domain, dataset):
        # Get metadata
        resource = dataset['resource']
        id = resource['id']

        # Get record from Elasticsearch
        try:
            hit = self.elasticsearch.get(
                'datamart', '_doc',
                '%s.%s' % (self.identifier, id),
                _source=['materialize.socrata_updated'])
        except elasticsearch.NotFoundError:
            pass
        else:
            updated = hit['_source']['materialize']['socrata_updated']
            if resource['updatedAt'] <= updated:
                logger.info("Dataset has not changed: %s", id)
                return

        # Read metadata
        metadata = dict(
            name=resource.get('name', id),
        )
        if resource.get('description'):
            metadata['description'] = resource['description']
        direct_url = (
            'https://data.cityofnewyork.us/api/views/'
            '{dataset_id}/rows.csv?accessType=DOWNLOAD'.format(
                dataset_id=id)
        )

        # Download dataset
        logging.info("Downloading dataset %s (%s)",
                     id, resource.get('name', '<no name>'))
        storage = self.create_storage()
        with open(os.path.join(storage.path, 'main.csv'), 'wb') as dest:
            response = requests.get(direct_url, stream=True)
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=4096):
                if chunk:  # filter out keep-alive chunks
                    dest.write(chunk)

        self.record_dataset(storage,
                            dict(
                                socrata_id=id,
                                socrata_domain=domain['url'],
                                socrata_updated=resource['updatedAt'],
                                direct_url=direct_url,
                            ),
                            metadata,
                            dataset_id=id)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")
    SocrataDiscoverer('datamart.socrata_discoverer')
    asyncio.get_event_loop().run_forever()
