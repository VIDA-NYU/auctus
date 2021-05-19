import asyncio
import csv
from datetime import datetime
from io import TextIOWrapper
import logging
import os
import pkg_resources
import requests
import time

from datamart_core import Discoverer
from datamart_core.common import setup_logging


logger = logging.getLogger(__name__)


def iterate_years(start, end, years):
    start = datetime.strptime(start, '%Y-%m-%d').year
    end = datetime.strptime(end, '%Y-%m-%d').year
    start = start - (start % years)
    end = end + years - 1 - (end % years)
    for i in range(start, end + 1, years):
        yield '%d-01-01' % i, '%d-12-31' % (i + years - 1)


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
            r = requests.get(
                'https://www.ncdc.noaa_discovery.gov/cdo-web/api/v2' +
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


class NoaaDiscoverer(Discoverer):
    DATASET_NAMES = {
        'GSOM': "Monthly summaries",
        'GHCND': "Daily summaries",
    }
    DATASETS = list(DATASET_NAMES)
    DATATYPE_NAMES = {
        'TAVG': "average temperature",
        'AWND': "average wind speed",
        # 'PRCP': "precipitation",
    }
    DATATYPES = list(DATATYPE_NAMES)
    CITIES = [
        {'id': 'CITY:US360019',
         'name': "New York, NY US",
         'latitude': 49.230001434521334,
         'longitude': -74.01151465721249},
    ]

    DELAY = 0.5

    def __init__(self, *args, **kwargs):
        super(NoaaDiscoverer, self).__init__(*args, **kwargs)

        with pkg_resources.resource_stream('noaa_discovery',
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

    def discover_datasets(self):
        for dataset in self.DATASETS:
            for datatype in self.DATATYPES:
                logger.info("Processing dataset %s (%s), datatype %s "
                            "(%s)...",
                            dataset, self.DATASET_NAMES[dataset],
                            datatype, self.DATATYPE_NAMES[datatype])
                for city_dict in self.CITIES:
                    logger.info("Getting city %s (%s)...",
                                city_dict['name'], city_dict['id'])
                    try:
                        self.discover_data(dataset, datatype, city_dict)
                    except Exception:
                        logger.exception("Error handling dataset=%r "
                                         "datatype=%r city_id=%r",
                                         dataset, datatype,
                                         city_dict['id'])

    def discover_data(self, dataset, datatype, city_dict):
        # Find available range
        time.sleep(self.DELAY)
        stations = get_all('/stations', self.DELAY,
                           datasetid=dataset, datatypeid=datatype)
        mindate = min(s['mindate'] for s in stations)
        maxdate = max(s['maxdate'] for s in stations)
        logger.info("Available range is %s - %s", mindate, maxdate)

        self.discover_data_range(dataset, datatype, city_dict,
                                 mindate, maxdate)

    def discover_data_range(self, dataset, datatype, city_dict, start, end):
        logger.info("Discovering data %s from %s for %s",
                    datatype, dataset, city_dict['name'])

        nb_years = 10 if dataset == 'GSOM' else 1
        for start, end in iterate_years(start, end, nb_years):
            logger.info("%s - %s...", start, end)

            description = "NOAA {} of {} for {}, {}-{}".format(
                self.DATASET_NAMES[dataset], self.DATATYPE_NAMES[datatype],
                city_dict['name'], start, end)

            self.record_dataset(
                dict(
                    noaa_dataset_id=dataset,
                    noaa_datatype_id=datatype,
                    noaa_city_id=city_dict['id'],
                    noaa_start=start,
                    noaa_end=end,
                ),
                dict(
                    name=description,
                    description=description,
                    source='noaa',
                    latitude=city_dict['latitude'],
                    longitude=city_dict['longitude'],
                ),
                dataset_id='{}.{}.{}.{}'.format(dataset, datatype,
                                                city_dict['id'], start))

    def handle_query(self, query, publish):
        # publish(...)
        pass  # TODO: If this is about the weather, pick the right datasets


if __name__ == '__main__':
    setup_logging()
    asyncio.get_event_loop().run_until_complete(
        NoaaDiscoverer('datamart.noaa').run()
    )
