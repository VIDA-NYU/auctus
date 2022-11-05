import advocate
import contextlib
import datamart_materialize
import ipaddress
import logging
import opentelemetry.trace
import os
import prometheus_client
import shutil
import socket
import zipfile

from datamart_fslock.cache import cache_get_or_set

from .common import hash_json
from .discovery import encode_dataset_id
from .objectstore import get_object_store


logger = logging.getLogger(__name__)
tracer = opentelemetry.trace.get_tracer(__name__)


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


def dataset_cache_key(dataset_id, metadata, format, format_options):
    if format == 'csv':
        if format_options:
            raise ValueError
        materialize = metadata.get('materialize', {})
        metadata = {'id': dataset_id}
    else:
        metadata = dict(metadata, id=dataset_id)
        materialize = metadata.pop('materialize', {})
    h = hash_json({
        'format': format,
        'format_options': format_options,
        'metadata': metadata,
        'materialize': materialize,
        # Note that DATAMART_VERSION is NOT in here
        # We rely on the admin clearing the cache if required
    })
    # The hash is sufficient, other components are for convenience
    return '%s_%s.%s' % (
        encode_dataset_id(dataset_id),
        h,
        format,
    )


def get_from_dataset_storage(metadata, dataset_id, destination):
    object_store = get_object_store()

    with contextlib.ExitStack() as s3_stack:
        try:
            csv_file = s3_stack.enter_context(
                object_store.open('datasets', encode_dataset_id(dataset_id))
            )
        except FileNotFoundError:
            return False
        else:
            logger.info("Reading from datasets bucket")
            with open(destination, 'wb') as fp:
                shutil.copyfileobj(csv_file, fp)

            # Apply converters
            materialize = metadata.get('materialize', {})
            if materialize.get('convert'):
                orig_temp = destination + '.orig'
                try:
                    os.rename(destination, orig_temp)
                    writer = datamart_materialize.make_writer(
                        destination,
                        format='csv',
                    )
                    for converter in reversed(materialize.get('convert', [])):
                        converter_args = dict(converter)
                        converter_id = converter_args.pop('identifier')
                        converter_class = datamart_materialize.converters[converter_id]
                        writer = converter_class(writer, **converter_args)

                    with writer.open_file('wb') as f_out:
                        with open(orig_temp, 'rb') as f_in:
                            for chunk in iter(lambda: f_in.read(4096), b''):
                                f_out.write(chunk)
                finally:
                    os.remove(orig_temp)

            return True


def advocate_session():
    kwargs = {}
    if os.environ.get('AUCTUS_REQUEST_WHITELIST', '').strip():
        kwargs['ip_whitelist'] = {
            ipaddress.ip_network(info[4][0] + '/32')
            for host in os.environ['AUCTUS_REQUEST_WHITELIST'].split(',')
            for info in socket.getaddrinfo(
                host, 80, 0, socket.SOCK_STREAM,
            )
        }
    if os.environ.get('AUCTUS_REQUEST_BLACKLIST', '').strip():
        kwargs['ip_blacklist'] = {
            ipaddress.ip_network(net)
            for net in os.environ['AUCTUS_REQUEST_BLACKLIST'].split(',')
        }
    validator = advocate.AddrValidator(**kwargs)
    return advocate.Session(validator=validator)


@contextlib.contextmanager
def get_dataset(metadata, dataset_id, format='csv', format_options=None):
    if not format:
        raise ValueError("Invalid output options")

    logger.info(
        "Getting dataset %r, size %s",
        dataset_id, metadata.get('size', 'unknown'),
    )

    # To limit the number of downloads, we always materialize the CSV file, and
    # convert it to the requested format if necessary. This avoids downloading
    # the CSV again just because we want a different format

    # Context to lock the CSV
    with contextlib.ExitStack() as dataset_lock:
        def create_csv(cache_temp):
            # Try to read from persistent storage
            if get_from_dataset_storage(metadata, dataset_id, cache_temp):
                return

            # Otherwise, materialize the CSV
            logger.info("Materializing CSV...")
            with tracer.start_as_current_span(
                'materialize/download',
                attributes={
                    'dataset_id': dataset_id,
                },
            ):
                with advocate_session() as http_session:
                    with PROM_DOWNLOAD.time():
                        datamart_materialize.download(
                            {'id': dataset_id, 'metadata': metadata},
                            cache_temp, None,
                            format='csv',
                            size_limit=10000000000,  # 10 GB
                            http=http_session,
                        )
                    logger.info("CSV is %d bytes", os.stat(cache_temp).st_size)

        csv_key = dataset_cache_key(dataset_id, metadata, 'csv', {})
        csv_path = dataset_lock.enter_context(
            cache_get_or_set(
                '/cache/datasets', csv_key, create_csv,
            )
        )

        # If CSV was requested, send it
        if format == 'csv':
            if format_options:
                raise ValueError("Invalid output options")
            yield csv_path
            return

        # Otherwise, do format conversion
        writer_cls = datamart_materialize.get_writer(format)
        if hasattr(writer_cls, 'parse_options'):
            format_options = writer_cls.parse_options(format_options)
        elif format_options:
            raise ValueError("Invalid output options")
        key = dataset_cache_key(
            dataset_id, metadata,
            format, format_options,
        )

        def create(cache_temp):
            # Do format conversion from CSV file
            logger.info("Converting CSV to %r opts=%r", format, format_options)
            with tracer.start_as_current_span(
                'materialize/convert-format',
                attributes={
                    'dataset_id': dataset_id,
                    'format': format,
                },
            ):
                with PROM_CONVERT.time():
                    with open(csv_path, 'rb') as src:
                        writer = writer_cls(
                            cache_temp, format_options=format_options,
                        )
                        writer.set_metadata(dataset_id, metadata)
                        with writer.open_file('wb') as dst:
                            shutil.copyfileobj(src, dst)
                        writer.finish()

                    # Make a ZIP if it's a folder
                    if os.path.isdir(cache_temp):
                        logger.info("Result is a directory, creating ZIP file")
                        zip_name = cache_temp + '.zip'
                        with zipfile.ZipFile(zip_name, 'w') as zip_:
                            make_zip_recursive(zip_, cache_temp)
                        shutil.rmtree(cache_temp)
                        os.rename(zip_name, cache_temp)

        with dataset_lock.pop_all():
            cache_path = dataset_lock.enter_context(
                cache_get_or_set(
                    '/cache/datasets', key, create,
                )
            )
        yield cache_path
