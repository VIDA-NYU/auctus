import asyncio
import csv
from datetime import datetime, timedelta
import logging
import os
import requests
import time

from datamart_core import Discoverer


logger = logging.getLogger(__name__)


class NoaaDiscoverer(Discoverer):
    DATASETS = [
        ('GHCND', "Daily summaries"),
        ('GSOM', "Monthly summaries"),
    ]
    DATATYPES = [
        ('AWND', "average wind speed"),
        ('TAVG', "average temperature"),
    ]
    STATIONS = [
        ('GHCND:USW00094789', 'New York JFK'),
    ]

    CHECK_INTERVAL = timedelta(hours=12)

    def __init__(self, *args, **kwargs):
        super(NoaaDiscoverer, self).__init__(*args, **kwargs)

        self.headers = {'token': os.environ['NOAA_TOKEN'],
                        'Accept': 'application/json'}

    def main_loop(self):
        while True:
            for dataset, dataset_name in self.DATASETS:
                logger.info("Processing dataset %s (%s)...",
                            dataset, dataset_name)
                for datatype, datatype_name in self.DATATYPES:
                    for station, station_name in self.STATIONS:
                        self.discover_data(dataset, dataset_name,
                                           datatype, datatype_name,
                                           station, station_name)

            sleep_until = datetime.utcnow() + self.CHECK_INTERVAL
            while datetime.utcnow() < sleep_until:
                time.sleep((sleep_until - datetime.utcnow()).total_seconds())

    def discover_data(self, dataset, dataset_name, datatype, datatype_name,
                      station, station_name):
        # Find available range
        r = requests.get(
            'https://www.ncdc.noaa.gov/cdo-web/api/v2/stations/{}'
                .format(station),
        )
        r.raise_for_status()
        station = r.json()

        # Download dataset
        # TODO: Record dataset without materializing it?
        self.download_data(dataset, dataset_name,
                           datatype, datatype_name,
                           station, station_name,
                           station['mindate'], station['maxdate'])

    def download_data(self, dataset, dataset_name, datatype, datatype_name,
                      station, station_name, start, end):
        logger.info("Downloading data %s for %s",
                    datatype_name, station_name)

        storage = self.create_storage()
        with open(os.path.join(storage.path, 'main.csv'), 'wb') as dest:
            writer = csv.writer(dest)
            writer.writerow(['date', datatype])
            offset = 0
            while True:
                r = requests.get(
                    'https://www.ncdc.noaa.gov/cdo-web/api/v2/data',
                    headers=self.headers,
                    params=dict(datasetid=dataset,
                                datatypeid=datatype,
                                stationid=station,
                                startdate=start,
                                enddate=end,
                                limit=1000,
                                offset=offset),
                )
                r.raise_for_status()
                obj = r.json()
                data = obj['results']

                for row in data:
                    writer.writerow(row['date'], row['value'])

                # Check for next page
                count = int(obj['metadata']['resultset']['count'])
                offset += len(data)
                if offset >= count:
                    break
                time.sleep(1)

        description = "NOAA {} of {} for {}".format(
            dataset_name, datatype_name, station_name),

        self.record_dataset(
            storage,
            dict(
                noaa_dataset_id=dataset,
                noaa_datatype_id=datatype,
                noaa_station_id=station,
                noaa_start=start,
                noaa_end=end,
            ),
            dict(
                name=description,
                description=description,
            ),
            dataset_id='{}.{}.{}.{}-{}'.format(dataset, datatype, station,
                                               start, end))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")
    NoaaDiscoverer('datamart.noaa_discoverer')
    asyncio.get_event_loop().run_forever()
