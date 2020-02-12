import datamart_materialize
from datamart_materialize.adaptors import ZipWriter
import logging
import os
import prometheus_client
import shutil

from .common import hash_json
from .discovery import encode_dataset_id
from .objectstore import get_object_store


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


def get_dataset(metadata, dataset_id, format='csv', format_options=None):
    if not format:
        raise ValueError

    logger.info(
        "Getting dataset %r, size %s",
        dataset_id, metadata.get('size', 'unknown'),
    )

    object_store = get_object_store()

    # To limit the number of downloads, we always materialize the CSV file, and
    # convert it to the requested format if necessary. This avoids downloading
    # the CSV again just because we want a different format

    # Try to read from persistent storage
    csv_file = None
    try:
        csv_file = object_store.open('datasets', encode_dataset_id(dataset_id))
        logger.info("Reading from datasets bucket")
    except FileNotFoundError:
        pass

    # Try the cache
    if csv_file is None:
        key = encode_dataset_id(dataset_id) + '_' + 'csv'
        try:
            csv_file = object_store.open('cached-datasets', key)
        except FileNotFoundError:
            pass

        # Otherwise, materialize the CSV
        if csv_file is None:
            with object_store.open('cached-datasets', key, 'wb') as cache_csv:
                logger.info("Materializing CSV...")
                with PROM_DOWNLOAD.time():
                    datamart_materialize.download(
                        {'id': dataset_id, 'metadata': metadata},
                        cache_csv, None,
                        format='csv',
                        size_limit=10000000000,  # 10 GB
                    )

            csv_file = object_store.open('cached-datasets', key)

    # If CSV was requested, send it
    if format == 'csv':
        if format_options:
            raise ValueError("Invalid output options")
        return csv_file

    # Otherwise, do format conversion
    writer_cls = datamart_materialize.get_writer(format)
    all_format_options = dict(getattr(writer_cls, 'default_options', ()))
    all_format_options.update(format_options)
    key = '%s_%s_%s' % (
        encode_dataset_id(dataset_id), format,
        hash_json(all_format_options),
    )

    try:
        return object_store.open('cached-datasets', key)
    except FileNotFoundError:
        pass

    # Do format conversion from CSV file
    logger.info("Converting CSV to %r opts=%r", format, format_options)
    with object_store.open('cached-datasets', key, 'wb') as cache_out:
        with PROM_CONVERT.time():
            if format_options:
                kwargs = dict(format_options=format_options)
            else:
                kwargs = {}
            if getattr(writer_cls, 'should_zip', False):
                cache_out = ZipWriter(cache_out)
            writer = writer_cls(cache_out, **kwargs)
            writer.set_metadata(dataset_id, metadata)
            with writer.open_file('wb') as dst:
                shutil.copyfileobj(csv_file, dst)
            writer.finish()

    return object_store.open('cached-datasets', key)
