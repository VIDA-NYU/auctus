import asyncio
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import elasticsearch
import elasticsearch.helpers
import hashlib
import logging
import os
import re
import requests
import tempfile
import time
import urllib.parse
import zipfile

from datamart_core import Discoverer
from datamart_core.common import setup_logging

logger = logging.getLogger(__name__)


LIST_URL = 'https://data.worldbank.org/indicator'


_re_non_id = re.compile(r'[^a-z0-9]+')


class WorldBankDiscoverer(Discoverer):
    CHECK_INTERVAL = timedelta(days=1)

    def main_loop(self):
        while True:
            now = datetime.utcnow()

            self.process_list()

            sleep_until = now + self.CHECK_INTERVAL
            logger.info("Sleeping until %s", sleep_until.isoformat())
            while datetime.utcnow() < sleep_until:
                time.sleep((sleep_until - datetime.utcnow()).total_seconds())

    def process_list(self):
        # Scrape the page with the list
        logger.info("Getting list...")
        list_page = requests.get(
            'https://data.worldbank.org/indicator',
            headers={'User-agent': 'Auctus'},
        )
        list_page.raise_for_status()
        list_soup = BeautifulSoup(list_page.content, 'html5lib')

        seen = set()

        # Each indicator is in a <section> with an <h3> that links to details
        for section in list_soup.find_all('section'):
            titles = section.find_all('h3')
            if len(titles) != 1:
                continue
            title = titles[0]
            link = title.find('a')
            if not link:
                continue
            details_url = link.attrs['href']
            details_url = urllib.parse.urljoin(list_page.url, details_url)
            self.process_indicator(link.text, details_url)
            seen.add(details_url)

        # Clean up the datasets we didn't see
        deleted = 0
        size = 10000
        query = {
            'query': {
                'term': {
                    'materialize.identifier': self.identifier,
                },
            }
        }
        hits = elasticsearch.helpers.scan(
            self.elasticsearch,
            index='datamart,pending',
            query=query,
            size=size,
            _source=['materialize.worldbank_url'],
        )
        for h in hits:
            if h['_source']['materialize']['worldbank_url'] not in seen:
                self.delete_dataset(full_id=h['_id'])
                deleted += 1

        if deleted:
            logger.info("Deleted %d missing datasets", deleted)

    def process_indicator(self, indicator_name, details_url):
        logger.info("Processing indicator %r", indicator_name)

        # Scrape the page with an indicator's details
        details_page = requests.get(
            details_url,
            headers={'User-agent': 'Auctus'},
        )
        details_page.raise_for_status()
        details_soup = BeautifulSoup(details_page.content, 'html5lib')

        # Find the link labeled "CSV"
        data_url = None
        for link in details_soup.find_all('a'):
            if link.text != 'CSV':
                continue
            data_url = link.attrs['href']
            data_url = urllib.parse.urljoin(details_page.url, data_url)
            break
        if not data_url:
            logging.error("No CSV link on %s", details_url)
            return

        dataset_id = indicator_name.lower()
        dataset_id = _re_non_id.sub('-', dataset_id)

        # There's no ETag and Last-Modified is always the current time, so we
        # have no choice but to check the contents

        # Get record from Elasticsearch
        previous_csv_hash = None
        try:
            hit = self.elasticsearch.get(
                'datamart',
                '%s.%s' % (self.identifier, dataset_id),
                _source=['materialize.csv_hash'],
            )['_source']
        except elasticsearch.NotFoundError:
            pass
        else:
            previous_csv_hash = hit['materialize']['csv_hash']

        # Download the ZIP file
        with tempfile.NamedTemporaryFile('wb', suffix='.zip') as dl_file:
            with requests.get(data_url, stream=True) as resp:
                resp.raise_for_status()
                for chunk in resp.iter_content(4096):
                    dl_file.write(chunk)
                dl_file.flush()
                logger.info("ZIP downloaded, %d bytes", dl_file.tell())

            dl_zip = zipfile.ZipFile(dl_file.name)

            # Find the CSV
            csv_name, = [
                name for name in dl_zip.namelist()
                if not name.lower().startswith('metadata_')
                and name.lower().endswith('.csv')
            ]

            # Hash the CSV
            h = hashlib.sha1()
            with dl_zip.open(csv_name, 'r') as dl_csv:
                for chunk in iter(dl_csv.read, b''):
                    h.update(chunk)
            dl_csv_hash = h.hexdigest()

            # If unchanged, pass
            if previous_csv_hash == dl_csv_hash:
                logger.info("CSV unchanged, skipping")
                return

            # Write the CSV to storage
            with dl_zip.open(csv_name, 'r') as dl_csv:
                with self.write_to_shared_storage(dataset_id) as tmp:
                    with open(os.path.join(tmp, 'main.csv'), 'wb') as fp:
                        for chunk in iter(dl_csv.read, b''):
                            fp.write(chunk)

        self.record_dataset(
            dict(
                worldbank_name=indicator_name,
                worldbank_url=details_url,
                csv_hash=dl_csv_hash,
            ),
            dict(
                name="World Bank - %s" % indicator_name,
                source="World Bank",
            ),
            dataset_id,
        )


if __name__ == '__main__':
    setup_logging()
    WorldBankDiscoverer('datamart.worldbank')
    asyncio.get_event_loop().run_forever()
