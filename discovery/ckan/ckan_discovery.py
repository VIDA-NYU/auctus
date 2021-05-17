import asyncio
import elasticsearch
import json
import logging
import requests
import sentry_sdk
from urllib.parse import urlencode

from datamart_core import Discoverer
from datamart_core.common import setup_logging


logger = logging.getLogger(__name__)


class CkanDiscoverer(Discoverer):
    EXTENSIONS = ('.csv', '.xls', '.xlsx')
    FILE_TYPES = ['CSV', 'XLS', 'XLSX']

    def __init__(self, *args, **kwargs):
        super(CkanDiscoverer, self).__init__(*args, **kwargs)
        with open('ckan.json') as fp:
            self.domains = json.load(fp)
        assert isinstance(self.domains, list)
        for domain in self.domains:
            assert isinstance(domain['url'], str)
            assert domain.keys() <= {'url', 'keyword_query'}
        logger.info("Loaded %d domains from ckan.json: %d",
                    len(self.domains))

    def discover_datasets(self):
        for domain in self.domains:
            try:
                self.get_datasets(domain)
            except Exception as e:
                sentry_sdk.capture_exception(e)
                logger.exception("Error processing %s", domain['url'])

    def get_datasets(self, domain):
        PAGE_SIZE = 100

        seen = set()
        kw = dict(
            fq='res_format:({0})'.format(' OR '.join(self.FILE_TYPES)),
            rows=PAGE_SIZE,
        )
        if 'keyword_query' in domain:
            kw['q'] = domain['keyword_query']
        start = 0
        while True:
            url = 'https://{0}/api/3/action/package_search?{1}'.format(
                domain['url'],
                urlencode(dict(kw, start=start)),
            )
            logger.info("Getting %s", url)
            headers = {'Accept': 'application/json'}
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            results = response.json()['result']['results']

            for record in results:
                self.process_package(domain, record)
                seen.add(record['id'])

            if len(results) < PAGE_SIZE:
                break
            else:
                start += len(results)

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
                                'materialize.ckan_domain.keyword': domain['url'],
                            },
                        },
                    ],
                },
            },
        }
        hits = self.elasticsearch.scan(
            index='datasets,pending',
            query=query,
            size=size,
            _source=['materialize.ckan_package_id'],
        )
        for h in hits:
            if h['_source']['materialize']['ckan_package_id'] not in seen:
                self.delete_dataset(full_id=h['_id'])
                deleted += 1

        if deleted:
            logger.info("Deleted %d missing datasets", deleted)

    def process_package(self, domain, package):
        # Get metadata common for the whole deposit
        package_metadata = dict(
            name=package['title'],
            source=domain['url'],
            source_url='https://{0}/dataset/{1}'.format(
                domain['url'], package['name'],
            ),
        )
        if 'license_title' in package:
            package_metadata['license'] = package['license_title']
        if package.get('notes'):
            package_metadata['description'] = package['notes']

        logger.info("Processing package %s %r", package['id'], package['title'])

        # Process each file
        for resource in package['resources']:
            if not resource['format'] in self.FILE_TYPES:
                continue

            dataset_id = '%s.%s' % (package['id'], resource['id'])

            modified = resource['metadata_modified']

            # See if we've ingested this file
            try:
                hit = self.elasticsearch.get(
                    'datasets',
                    '%s.%s' % (self.identifier, dataset_id),
                    _source=['materialize'],
                )['_source']
            except elasticsearch.NotFoundError:
                pass
            else:
                if hit['materialize']['ckan_record_updated'] == modified:
                    logger.info("Dataset already in index")
                    return
            try:
                hit = self.elasticsearch.get(
                    'pending',
                    '%s.%s' % (self.identifier, dataset_id),
                    _source=['status', 'metadata.materialize'],
                )['_source']
            except elasticsearch.NotFoundError:
                pass
            else:
                if (
                    hit['metadata']['materialize']['ckan_record_updated']
                    == modified
                ):
                    logger.info(
                        "Dataset already in pending index, status=%s",
                        hit.get('status'),
                    )
                    return

            logger.info("Resource %s", resource['name'])

            file_metadata = dict(
                package_metadata,
                name='%s - %s' % (
                    package_metadata['name'], resource['name'],
                ),
                size=resource['size'],
            )
            if resource.get('description'):
                file_metadata['description'] = resource['description']
                if 'description' in package_metadata:
                    file_metadata['description'] += (
                        '\n\n'
                        + package_metadata['description']
                    )
            direct_url = resource['download_url'] or resource.get('url')
            if not direct_url:
                raise KeyError('download_url or url')

            # Discover this dataset
            self.record_dataset(
                dict(
                    ckan_domain=domain['url'],
                    ckan_package_id=package['id'],
                    ckan_resource_id=resource['id'],
                    ckan_record_updated=modified,
                    direct_url=direct_url,
                ),
                file_metadata,
                dataset_id=dataset_id,
            )


if __name__ == '__main__':
    setup_logging()
    asyncio.get_event_loop().run_until_complete(
        CkanDiscoverer('datamart.ckan').run()
    )
