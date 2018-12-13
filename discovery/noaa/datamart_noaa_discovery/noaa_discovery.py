import asyncio
import csv
from datetime import datetime, timedelta
from io import TextIOWrapper
import logging
import os
import pkg_resources
import requests
import time

from datamart_core import Discoverer


logger = logging.getLogger(__name__)


def YearIterator(start, end, years):
    start = datetime.strptime(start, '%Y-%m-%d').year
    end = datetime.strptime(end, '%Y-%m-%d').year
    start = start - (start % years)
    for i in range(start, end + 1):
        yield '%d-01-01' % i, '%d-12-31' % i


def get_all(endpoint, delay=0.5, **params):
    """Handles NOAA API pagination.
    """
    headers = {'token': os.environ['NOAA_TOKEN'],
               'Accept': 'application/json'}
    results = []
    failed = 0
    pages = 0
    while True:
        try:
            r = requests.get('https://www.ncdc.noaa.gov/cdo-web/api/v2' + endpoint,
                             headers=headers,
                             params=dict(params,
                                         limit='1000',
                                         offset=len(results)))
            r.raise_for_status()
        except requests.HTTPError as e:
            logger.warning("Request failed: %r", e)
            failed += 1
            if failed == 10:
                raise
            time.sleep(2)
            continue
        time.sleep(delay)
        failed = 0
        obj = r.json()
        pages += 1
        results.extend(obj['results'])
        resultset = obj['metadata']['resultset']
        count = int(resultset['count'])
        if len(results) >= count:
            break

    logger.info("Downloaded %d pages of data (%d rows)", pages, len(results))
    return results


class NoaaDiscoverer(Discoverer):
    DATASETS = [
        ('GSOM', "Monthly summaries"),
        ('GHCND', "Daily summaries"),
    ]
    DATATYPES = [
        ('TAVG', "average temperature"),
        ('AWND', "average wind speed"),
        # ('PRCP', "precipitation"),
    ]
    CITIES = [
        {'id': 'CITY:US360019',
         'name': "New York, NY US",
         'latitude': 49.230001434521334,
         'longitude': -74.01151465721249},
    ]

    DELAY = 0.5

    CHECK_INTERVAL = timedelta(hours=12)

    def __init__(self, *args, **kwargs):
        super(NoaaDiscoverer, self).__init__(*args, **kwargs)

        with pkg_resources.resource_stream('datamart_noaa_discovery',
                                           'noaa_cities.csv') as bio:
            tio = TextIOWrapper(bio, encoding='utf-8', newline='')
            csvfile = csv.DictReader(tio)
            assert csvfile.fieldnames == ['id', 'name',
                                          'latitude', 'longitude']
            self.CITIES = list(csvfile)
            for city in self.CITIES:
                city['latitude'] = float(city['latitude'])
                city['longitude'] = float(city['longitude'])
        logger.info("Loaded %d cities", len(self.CITIES))

    def main_loop(self):
        while True:
            for dataset, dataset_name in self.DATASETS:
                for datatype, datatype_name in self.DATATYPES:
                    logger.info("Processing dataset %s (%s), datatype %s "
                                "(%s)...",
                                dataset, dataset_name,
                                datatype, datatype_name)
                    for city_dict in self.CITIES:
                        logger.info("Getting city %s (%s)...",
                                    city_dict['name'], city_dict['id'])
                        try:
                            self.discover_data(dataset, dataset_name,
                                               datatype, datatype_name,
                                               city_dict)
                        except Exception:
                            logger.exception("Error handling dataset=%r "
                                             "datatype=%r city_id=%r",
                                             dataset, datatype,
                                             city_dict['id'])

            sleep_until = datetime.utcnow() + self.CHECK_INTERVAL
            while datetime.utcnow() < sleep_until:
                time.sleep((sleep_until - datetime.utcnow()).total_seconds())

    def discover_data(self, dataset, dataset_name, datatype, datatype_name,
                      city_dict):
        # Find available range
        time.sleep(self.DELAY)
        stations = get_all('/stations', self.DELAY,
                           datasetid=dataset, datatypeid=datatype)
        mindate = min(s['mindate'] for s in stations)
        maxdate = max(s['maxdate'] for s in stations)
        logger.info("Available range is %s - %s", mindate, maxdate)

        # Download dataset
        # TODO: Record dataset without materializing it?
        self.download_data(dataset, dataset_name,
                           datatype, datatype_name,
                           city_dict,
                           mindate, maxdate)

    def download_data(self, dataset, dataset_name, datatype, datatype_name,
                      city_dict, start, end):
        logger.info("Downloading data %s (%s) for %s",
                    datatype, datatype_name, city_dict['name'])

        storage = self.create_storage()
        with open(os.path.join(storage.path, 'main.csv'), 'w') as dest:
            writer = csv.writer(dest)
            writer.writerow(['date', datatype])

            for r_start, r_end in YearIterator(start, end,
                                               10 if dataset == 'GSOM' else 1):
                logger.info("%s - %s...", r_start, r_end)
                data = get_all('/data', self.DELAY,
                               datasetid=dataset, datatypeid=datatype,
                               locationid=city_dict['id'],
                               startdate=r_start, enddate=r_end)
                # Sort by date
                data = sorted(data, key=lambda v: v['date'])
                # Write the data, one row per date, averaged across stations
                time = None
                values = []
                for row in data:
                    if row['date'] != time:
                        if time is not None:
                            writer.writerow([time, sum(values) / len(values)])
                        time = row['date']
                        values = []
                    values.append(row['value'])
                if time is not None:
                    writer.writerow([time, sum(values) / len(values)])

        description = "NOAA {} of {} for {}".format(
            dataset_name, datatype_name, city_dict['name']),

        self.record_dataset(
            storage,
            dict(
                noaa_dataset_id=dataset,
                noaa_datatype_id=datatype,
                noaa_station_id=city_dict['id'],
                noaa_start=start,
                noaa_end=end,
            ),
            dict(
                name=description,
                description=description,
                latitude=city_dict['latitude'],
                longitude=city_dict['longitude'],
            ),
            dataset_id='{}.{}.{}'.format(dataset, datatype,
                                         city_dict['id']))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")
    NoaaDiscoverer('datamart.noaa_discoverer')
    asyncio.get_event_loop().run_forever()
