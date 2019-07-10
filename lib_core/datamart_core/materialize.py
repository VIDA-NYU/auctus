import contextlib
import datamart_materialize
import logging
import os
import prometheus_client
import shutil

from datamart_core.common import FilesystemLocks

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
    logger.info("Getting dataset %r", dataset_id)
    shared = os.path.join('/datasets', encode_dataset_id(dataset_id))
    if format == 'csv' and os.path.exists(shared):
        # Read directly from stored file
        logger.info("Reading from /datasets")
        yield os.path.join(shared, 'main.csv')
        return

    cache_path = '/dataset_cache/' + encode_dataset_id(dataset_id)
    lock_path = cache_path + '.lock'
    while True:
        try:
            lock = FilesystemLocks.lock_shared(lock_path)
        except FileNotFoundError:
            pass
        else:
            try:
                if os.path.exists(cache_path):
                    # Dataset exists and we have it locked, return it
                    yield os.path.join(os.path.join(cache_path, 'main.csv'))
                    return
                # Dataset was removed while we waited -- we'll try downloading
            finally:
                FilesystemLocks.unlock_shared(lock)

        lock = FilesystemLocks.lock_exclusive(lock_path)
        try:
            if os.path.exists(cache_path):
                # Cache was created while we waited
                # We can't downgrade to a shared lock, so restart
                continue
            else:
                # Cache doesn't exist and we have it locked -- download
                if os.path.exists(shared):
                    # Do format conversion from stored file
                    logger.info("Converting stored file to %r", format)
                    with open(os.path.join(shared, 'main.csv'), 'rb') as src:
                        writer_cls = datamart_materialize.get_writer(format)
                        writer = writer_cls(dataset_id, cache_path, metadata)
                        with writer.open_file('wb') as dst:
                            shutil.copyfileobj(src, dst)
                else:
                    # Materialize
                    logger.info("Materializing...")
                    with PROM_DOWNLOAD.time():
                        datamart_materialize.download(
                            {'id': dataset_id, 'metadata': metadata},
                            cache_path, None, format=format)

                # We can't downgrade to a shared lock, so restart
                continue
        finally:
            FilesystemLocks.unlock_exclusive(lock)
