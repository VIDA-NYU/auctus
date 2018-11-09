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
    STATIONS = [
        {'station_id': 'GHCND:USW00094789',
         'station_name': 'JFK INTERNATIONAL AIRPORT, NY US',
         'latitude': '40.6386', 'longitude': '-73.7622',
         'city_id': 'CITY:US360019', 'city_name': 'New York, NY US'},
    ]

    DELAY = 0.5

    CHECK_INTERVAL = timedelta(hours=12)

    def __init__(self, *args, **kwargs):
        super(NoaaDiscoverer, self).__init__(*args, **kwargs)

        with pkg_resources.resource_stream('datamart_noaa_discovery',
                                           'noaa_city_stations.csv') as bio:
            tio = TextIOWrapper(bio, encoding='utf-8', newline='')
            csvfile = csv.DictReader(tio)
            assert csvfile.fieldnames == ['station_id', 'station_name',
                                          'latitude', 'longitude',
                                          'city_id', 'city_name']
            self.STATIONS = list(csvfile)
            for station in self.STATIONS:
                station['latitude'] = float(station['latitude'])
                station['longitude'] = float(station['longitude'])
        logger.info("Loaded %d stations", len(self.STATIONS))

        self.headers = {'token': os.environ['NOAA_TOKEN'],
                        'Accept': 'application/json'}

    def main_loop(self):
        while True:
            for dataset, dataset_name in self.DATASETS:
                logger.info("Processing dataset %s (%s)...",
                            dataset, dataset_name)
                for datatype, datatype_name in self.DATATYPES:
                    for station_dict in self.STATIONS:
                        try:
                            self.discover_data(dataset, dataset_name,
                                               datatype, datatype_name,
                                               station_dict)
                        except Exception:
                            logger.exception("Error handling dataset=%r "
                                             "datatype=%r station_id=%r",
                                             dataset, datatype,
                                             station_dict['station_id'])

            sleep_until = datetime.utcnow() + self.CHECK_INTERVAL
            while datetime.utcnow() < sleep_until:
                time.sleep((sleep_until - datetime.utcnow()).total_seconds())

    def discover_data(self, dataset, dataset_name, datatype, datatype_name,
                      station_dict):
        # Find available range
        time.sleep(self.DELAY)
        r = requests.get(
            'https://www.ncdc.noaa.gov/cdo-web/api/v2/stations/{}'
                .format(station_dict['station_id']),
            params=dict(datasetid=dataset,
                        datatypeid=datatype),
            headers=self.headers,
        )
        r.raise_for_status()
        station = r.json()

        # Download dataset
        # TODO: Record dataset without materializing it?
        self.download_data(dataset, dataset_name,
                           datatype, datatype_name,
                           station_dict,
                           station['mindate'], station['maxdate'])

    def download_data(self, dataset, dataset_name, datatype, datatype_name,
                      station_dict, start, end):
        logger.info("Downloading data %s (%s) for %s",
                    datatype, datatype_name, station_dict['station_name'])

        storage = self.create_storage()
        with open(os.path.join(storage.path, 'main.csv'), 'w') as dest:
            writer = csv.writer(dest)
            writer.writerow(['date', datatype])
            offset = 0
            start = '2018-01-01'
            end = '2018-02-15'
            while True:
                time.sleep(self.DELAY)
                r = requests.get(
                    'https://www.ncdc.noaa.gov/cdo-web/api/v2/data',
                    headers=self.headers,
                    params=dict(datasetid=dataset,
                                datatypeid=datatype,
                                stationid=station_dict['station_id'],
                                startdate=start,
                                enddate=end,
                                limit=1000,
                                offset=offset),
                )
                r.raise_for_status()
                obj = r.json()
                if 'results' not in obj:
                    # Not sure what this means. No data?
                    logger.error("Empty JSON response!")
                    break
                data = obj['results']

                for row in data:
                    writer.writerow([row['date'], row['value']])

                # Check for next page
                count = int(obj['metadata']['resultset']['count'])
                offset += len(data)
                if offset >= count:
                    break

        description = "NOAA {} of {} for {}".format(
            dataset_name, datatype_name, station_dict['station_name']),

        self.record_dataset(
            storage,
            dict(
                noaa_dataset_id=dataset,
                noaa_datatype_id=datatype,
                noaa_station_id=station_dict['station_id'],
                noaa_start=start,
                noaa_end=end,
            ),
            dict(
                name=description,
                description=description,
                latitude=station_dict['latitude'],
                longitude=station_dict['longitude'],
            ),
            dataset_id='{}.{}.{}'.format(dataset, datatype,
                                         station_dict['station_id']))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")
    NoaaDiscoverer('datamart.noaa_discoverer')
    asyncio.get_event_loop().run_forever()
