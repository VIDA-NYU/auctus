import asyncio
import csv
import uuid
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
    NAMESPACE = uuid.UUID('d20b45e8-b0d5-4b17-a6c4-e8399afc2afa')  # Random

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
        logger.info("Downloading file (etag=%r)", etag)
        response = requests.get(
            'http://vanga.sista.arizona.edu/delphi_data/delphi.db',
            headers=headers,
            stream=True,
        )
        response.raise_for_status()
        if response.status_code == 304:
            logger.info("File hasn't changed")
            return
        logger.info(
            "Got response, length=%s",
            response.headers.get('content-length', "unset"),
        )

        with tempfile.NamedTemporaryFile(suffix='.sqlite3') as tmp:
            for chunk in response.iter_content(4096):
                if chunk:  # filter out keep-alive chunks
                    tmp.write(chunk)
            tmp.flush()

            self.discover_indicators(tmp.name)
            self.discover_dssat(tmp.name)

        self.elasticsearch.index(
            'pending',
            {'etag': response.headers.get('ETag')},
            id=self.identifier,
        )

    def discover_indicators(self, filename):
        conn = sqlite3.connect(filename)
        indicators = conn.execute('''\
            SELECT DISTINCT Source, Unit, Variable FROM indicator;
        ''')
        for source, unit, variable_name in indicators:
            name = "%s (%s) from %s" % (variable_name, unit, source)
            dataset_id = uuid.uuid5(self.NAMESPACE, name).hex
            cursor = conn.execute(
                '''\
                    SELECT Country, State, County, Year, Month, Value
                    FROM indicator
                    WHERE Source=? AND Unit=? AND Variable=?;
                ''',
                (source, unit, variable_name),
            )
            with self.write_to_shared_storage(dataset_id) as tmp:
                with open(os.path.join(tmp, 'main.csv'), 'w') as fp:
                    writer = csv.writer(fp)
                    writer.writerow([
                        'Country', 'State', 'County',
                        'Year', 'Month',
                        'Value',
                    ])
                    for row in cursor:
                        writer.writerow([
                            e if e is not None else ''
                            for e in row
                        ])
            self.record_dataset(
                dict(
                    uaz_indicators_source=source,
                    uaz_indicators_variable=variable_name,
                    uaz_indicators_unit=unit,
                ),
                dict(
                    name=name,
                    unit=unit,
                    source='%s (UAZ)' % source,
                ),
                dataset_id=dataset_id,
            )
            cursor.close()

    def discover_dssat(self, filename):
        pass  # TODO


if __name__ == '__main__':
    setup_logging()
    UazIndicatorsDiscoverer('datamart.uaz-indicators')
    asyncio.get_event_loop().run_forever()
