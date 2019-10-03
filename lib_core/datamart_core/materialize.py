import contextlib
import datamart_materialize
import logging
import os
import prometheus_client
import shutil

from datamart_core.fscache import cache_get_or_set

from .discovery import encode_dataset_id


logger = logging.getLogger(__name__)


PROM_DOWNLOAD = prometheus_client.Histogram(
    'download_seconds',
    "Materialization time",
    buckets=[1.0, 10.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0,
             float('inf')],
)


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
                    )

            csv_cache_key = encode_dataset_id(dataset_id) + '_' + 'csv'
            csv_path = csv_lock.enter_context(
                cache_get_or_set('/dataset_cache', csv_cache_key, create_csv)
            )

        # If CSV was requested, send it
        if format == 'csv':
            yield csv_path
            return

        # Otherwise, do format conversion
        cache_key = encode_dataset_id(dataset_id) + '_' + format

        def create(cache_temp):
            # Do format conversion from CSV file
            logger.info("Converting CSV to %r", format)
            with open(csv_path, 'rb') as src:
                writer_cls = datamart_materialize.get_writer(format)
                writer = writer_cls(dataset_id, cache_temp, metadata)
                with writer.open_file('wb') as dst:
                    shutil.copyfileobj(src, dst)

        with cache_get_or_set('/dataset_cache', cache_key, create) as cache_path:
            yield cache_path
