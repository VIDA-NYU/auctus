import asyncio
import elasticsearch
import json
import logging
import requests
import time
from urllib.parse import urlencode

from datamart_core import Discoverer
from datamart_core.common import setup_logging


logger = logging.getLogger(__name__)


class ZenodoDiscoverer(Discoverer):
    EXTENSIONS = ('.xls', '.xlsx', '.csv', '.sav')
    FILE_TYPES = ['csv', 'xlsx', 'sav']

    def __init__(self, *args, **kwargs):
        super(ZenodoDiscoverer, self).__init__(*args, **kwargs)
        with open('zenodo.json') as fp:
            obj = json.load(fp)
        self.keyword_query = obj.pop('keyword_query', '')
        if obj:
            logger.warning("Unknown keys in configuration: %s",
                           ', '.join(obj))
        logger.info("Loaded keyword from zenodo.json: %s",
                    self.keyword_query)

    def discover_datasets(self):
        seen = set()
        url = (
            'https://zenodo.org/api/records/'
            '?' + urlencode(
                dict(
                    page=1,
                    size=200,
                    q=self.keyword_query,
                    file_type=self.FILE_TYPES,
                    type='dataset',
                ),
                doseq=True
            )
        )
        while url:
            logger.info("Getting %s", url)
            headers = {'Accept': 'application/json'}
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            obj = response.json()

            for record in obj:
                self.process_record(record)
                seen.add(record['id'])

            if 'next' in response.links:
                url = response.links['next']['url']
                time.sleep(2)
            else:
                url = None

        # Clean up the datasets we didn't see
        deleted = 0
        size = 10000
        query = {
            'query': {
                'term': {
                    'materialize.identifier': self.identifier,
                },
            },
        }
        hits = self.elasticsearch.scan(
            index='datasets,pending',
            query=query,
            size=size,
            _source=['materialize.zenodo_record_id'],
        )
        for h in hits:
            if h['_source']['materialize']['zenodo_record_id'] not in seen:
                self.delete_dataset(full_id=h['_id'])
                deleted += 1

        if deleted:
            logger.info("Deleted %d missing datasets", deleted)

    def process_record(self, record):
        # Get metadata common for the whole deposit
        record_metadata = dict(
            name=record['title'],
            source='zenodo.org',
            source_url='https://zenodo.org/record/%d' % record['id'],
        )
        if 'license' in record['metadata']:
            record_metadata['license'] = record['metadata']['license']
        description = ''
        if record['metadata'].get('description'):
            description += record['metadata']['description']
        if record['metadata'].get('keywords'):
            description += '\n\n' + ', '.join(record['metadata']['keywords'])
        if description:
            record_metadata['description'] = description

        logger.info("Processing record %s %r", record['id'], record['title'])

        # Process each file
        for file in record['files']:
            if not file['filename'].lower().endswith(self.EXTENSIONS):
                continue

            dataset_id = '%s.%s' % (record['id'], file['id'])

            # See if we've ingested this file
            try:
                self.elasticsearch.get(
                    'datasets',
                    '%s.%s' % (self.identifier, dataset_id),
                    _source=False,
                )
            except elasticsearch.NotFoundError:
                pass
            else:
                logger.info("Dataset already in index")
                return

            try:
                hit = self.elasticsearch.get(
                    'pending',
                    '%s.%s' % (self.identifier, dataset_id),
                    _source=['status'],
                )['_source']
            except elasticsearch.NotFoundError:
                pass
            else:
                logger.info(
                    "Dataset already in pending index, status=%s",
                    hit.get('status'),
                )
                return

            logger.info("File %s", file['filename'])

            file_metadata = dict(
                record_metadata,
                name='%s - %s' % (
                    record_metadata['name'], file['filename'],
                ),
                size=file['filesize'],
            )
            direct_url = file['links']['download']

            # Discover this dataset
            self.record_dataset(
                dict(
                    zenodo_record_id=record['id'],
                    zenodo_file_id=file['id'],
                    zenodo_record_updated=record['modified'],
                    direct_url=direct_url,
                ),
                file_metadata,
                dataset_id=dataset_id,
            )


if __name__ == '__main__':
    setup_logging()
    asyncio.get_event_loop().run_until_complete(
        ZenodoDiscoverer('datamart.zenodo').run()
    )
