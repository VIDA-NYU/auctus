import csv
import logging
import os
import requests
import time

from . import UnconfiguredMaterializer, DatasetTooBig


logger = logging.getLogger(__name__)


def get_all(endpoint, token, delay=0.5, **params):
    """Handles NOAA API pagination.
    """
    headers = {'token': token,
               'Accept': 'application/json'}
    results = []
    failed = 0
    pages = 0
    while True:
        try:
            r = requests.get(
                'https://www.ncdc.noaa.gov/cdo-web/api/v2' +
                endpoint,
                headers=headers,
                params=dict(params, limit='1000', offset=len(results)),
            )
            r.raise_for_status()
        except requests.HTTPError as e:
            failed += 1
            logger.warning("Request failed (%d): %r", failed, e)
            if failed == 10:
                raise
            time.sleep(2)
            continue
        time.sleep(delay)
        failed = 0
        obj = r.json()
        if not obj:
            # FIXME: Does this mean no data?
            return results
        pages += 1
        results.extend(obj['results'])
        resultset = obj['metadata']['resultset']
        count = int(resultset['count'])
        if len(results) >= count:
            break

    return results


class NoaaMaterializer(object):
    DELAY = 0.5

    def download(self, materialize, writer, size_limit=None):
        try:
            token = os.environ['NOAA_TOKEN']
        except KeyError:
            raise UnconfiguredMaterializer("$NOAA_TOKEN is not set")

        data = get_all('/data', token, self.DELAY,
                       datasetid=materialize['noaa_dataset_id'],
                       datatypeid=materialize['noaa_datatype_id'],
                       locationid=materialize['noaa_city_id'],
                       startdate=materialize['noaa_start'],
                       enddate=materialize['noaa_end'])

        with writer.open_file('w') as fp:
            writer = csv.writer(fp)
            writer.writerow(['date', materialize['noaa_datatype_id']])
            # Sort by date
            data = sorted(data, key=lambda v: v['date'])
            # Write the data, one row per date, averaged across stations
            time = None
            values = []
            for row in data:
                if row['date'] != time:
                    if time is not None:
                        writer.writerow([time, sum(values) / len(values)])
                        if size_limit is not None and fp.tell() > size_limit:
                            raise DatasetTooBig(limit=size_limit)
                    time = row['date']
                    values = []
                values.append(row['value'])
            if time is not None:
                writer.writerow([time, sum(values) / len(values)])


noaa_materializer = NoaaMaterializer()
