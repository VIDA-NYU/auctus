import asyncio
import elasticsearch
import json
import logging
import re
import sentry_sdk
import sodapy

from datamart_core import Discoverer
from datamart_core.common import setup_logging


logger = logging.getLogger(__name__)


re_non_id_safe = re.compile(r'[^a-z0-9-]+')


def encode_domain(url):
    domain = re_non_id_safe.sub('-', url.lower())
    return domain


class SocrataDiscoverer(Discoverer):
    def __init__(self, *args, **kwargs):
        super(SocrataDiscoverer, self).__init__(*args, **kwargs)

        with open('socrata.json') as fp:
            self.domains = json.load(fp)
        logger.info("Loaded %d domains from socrata.json",
                    len(self.domains))

        self.last_update = {}

    def discover_datasets(self):
        for domain in self.domains:
            try:
                self.process_domain(domain)
            except Exception as e:
                sentry_sdk.capture_exception(e)
                logger.exception("Error processing %s", domain['url'])

    def process_domain(self, domain):
        logger.info("Processing %s...", domain['url'])
        socrata = sodapy.Socrata(domain['url'],
                                 **domain.get('auth', {'app_token': None}))
        datasets = socrata.datasets()
        logger.info("Found %d datasets", len(datasets))
        if not datasets:
            return
        seen = set()
        for dataset in datasets:
            try:
                valid = self.process_dataset(domain, dataset)
            except Exception as e:
                sentry_sdk.capture_exception(e)
                logger.exception("Error processing dataset %s",
                                 dataset['resource']['id'])
            else:
                assert isinstance(valid, bool)
                if valid:
                    seen.add(dataset['resource']['id'])

        logger.info("Discovered %d/%d datasets", len(seen), len(datasets))

        # Clean up the datasets we didn't see
        deleted = 0
        size = 10000
        query = {
            'query': {
                'bool': {
                    'must': [
                        {
                            'term': {
                                'materialize.identifier': self.identifier,
                            },
                        },
                        {
                            'term': {
                                'materialize.socrata_domain.keyword': domain['url'],
                            },
                        },
                    ],
                },
            }
        }
        hits = self.elasticsearch.scan(
            index='datasets,pending',
            query=query,
            size=size,
            _source=['materialize.socrata_id'],
        )
        for h in hits:
            if h['_source']['materialize']['socrata_id'] not in seen:
                self.delete_dataset(full_id=h['_id'])
                deleted += 1

        if deleted:
            logger.info("Deleted %d missing datasets", deleted)

    def process_dataset(self, domain, dataset):
        # Get metadata
        resource = dataset['resource']
        id = resource['id']

        encoded_domain = encode_domain(domain['url'])
        dataset_id = '{}.{}'.format(encoded_domain, id)

        # Check type
        # api, calendar, chart, datalens, dataset, federated_href, file,
        # filter, form, href, link, map, measure, story, visualization
        if resource['type'] != 'dataset':
            logger.info("Skipping %s, type %s", id, resource['type'])
            return False

        # Get record from Elasticsearch
        hit = None
        try:
            hit = self.elasticsearch.get(
                'pending',
                '%s.%s' % (self.identifier, dataset_id),
                _source=['materialize.socrata_updated'],
            )['_source']
        except elasticsearch.NotFoundError:
            try:
                hit = self.elasticsearch.get(
                    'datasets',
                    '%s.%s' % (self.identifier, dataset_id),
                    _source=['materialize.socrata_updated'],
                )['_source']
            except elasticsearch.NotFoundError:
                pass

        if hit is not None:
            updated = hit['materialize']['socrata_updated']
            if resource['updatedAt'] <= updated:
                logger.info("Dataset has not changed: %s", id)
                return True

        # Read metadata
        metadata = dict(
            name=resource.get('name', id),
            source=domain['url'],
        )
        if resource.get('description'):
            metadata['description'] = resource['description']
        if 'link' in dataset:
            metadata['source_url'] = dataset['link']
        direct_url = (
            'https://{domain}/api/views/{dataset_id}/rows.csv'
            '?accessType=DOWNLOAD'.format(domain=domain['url'], dataset_id=id)
        )

        # Discover this dataset
        self.record_dataset(dict(socrata_id=id,
                                 socrata_domain=domain['url'],
                                 socrata_updated=resource['updatedAt'],
                                 direct_url=direct_url),
                            metadata,
                            dataset_id=dataset_id)
        return True


if __name__ == '__main__':
    setup_logging()
    asyncio.get_event_loop().run_until_complete(
        SocrataDiscoverer('datamart.socrata').run()
    )
