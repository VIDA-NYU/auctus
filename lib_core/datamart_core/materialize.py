import contextlib
import datamart_materialize
import logging
import os
import prometheus_client
import shutil
import zipfile

from .discovery import encode_dataset_id


logger = logging.getLogger(__name__)


PROM_DOWNLOAD = prometheus_client.Histogram(
    'download_seconds',
    "Time spent on download during materialization",
    buckets=[1.0, 10.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0,
             float('inf')],
)
PROM_CONVERT = prometheus_client.Histogram(
    'convert_seconds',
    "Time spent on conversion during materialization",
    buckets=[1.0, 10.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0,
             float('inf')],
)


def make_zip_recursive(zip_, src, dst=''):
    if os.path.isdir(src):
        for name in os.listdir(src):
            make_zip_recursive(
                zip_,
                os.path.join(src, name),
                dst + '/' + name if dst else name,
            )
    else:
        zip_.write(src, dst)


@contextlib.contextmanager
def get_dataset(metadata, dataset_id, format='csv'):
    if not format:
        raise ValueError

    logger.info(
        "Getting dataset %r, size %s",
        dataset_id, metadata.get('size', 'unknown'),
    )

    # To limit the number of downloads, we always materialize the CSV file, and
    # convert it to the requested format if necessary. This avoids downloading
    # the CSV again just because we want a different format

    # TODO: Rewrite this, get from S3
    # If not in S3, download while streaming to S3
    # Check for CSV before requested format (if CSV is absent, other format is out of date)

    # Context to lock the CSV
    csv_lock = contextlib.ExitStack()
    with csv_lock:
        # Try to read from persistent storage
        shared = os.path.join('/datasets', encode_dataset_id(dataset_id))
        if os.path.exists(shared):
            logger.info("Reading from /datasets")
            csv_path = os.path.join(shared, 'main.csv')
        else:
            # Otherwise, materialize the CSV
            def create_csv(cache_temp):
                logger.info("Materializing CSV...")
                with PROM_DOWNLOAD.time():
                    datamart_materialize.download(
                        {'id': dataset_id, 'metadata': metadata},
                        cache_temp, None,
                        format='csv',
                        size_limit=10_000_000_000,  # 10 GB
                    )

            csv_key = encode_dataset_id(dataset_id) + '_' + 'csv'
            csv_path = csv_lock.enter_context(
                cache_get_or_set('/cache/datasets', csv_key, create_csv)
            )

        # If CSV was requested, send it
        if format == 'csv':
            yield csv_path
            return

        # Otherwise, do format conversion
        key = encode_dataset_id(dataset_id) + '_' + format

        def create(cache_temp):
            # Do format conversion from CSV file
            logger.info("Converting CSV to %r", format)
            with PROM_CONVERT.time():
                with open(csv_path, 'rb') as src:
                    writer_cls = datamart_materialize.get_writer(format)
                    writer = writer_cls(dataset_id, cache_temp, metadata)
                    with writer.open_file('wb') as dst:
                        shutil.copyfileobj(src, dst)

                # Make a ZIP if it's a folder
                if os.path.isdir(cache_temp):
                    logger.info("Result is a directory, creating ZIP file")
                    zip_name = cache_temp + '.zip'
                    with zipfile.ZipFile(zip_name, 'w') as zip_:
                        make_zip_recursive(zip_, cache_temp)
                    shutil.rmtree(cache_temp)
                    os.rename(zip_name, cache_temp)

        with cache_get_or_set('/cache/datasets', key, create) as cache_path:
            yield cache_path
