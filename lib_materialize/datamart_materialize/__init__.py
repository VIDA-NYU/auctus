import logging
from pkg_resources import iter_entry_points
import requests
import time


__version__ = '0.5.1'


logger = logging.getLogger(__name__)


class UnconfiguredMaterializer(Exception):
    """Raised by a materializer when it is not configured.

    Some materializers need configuration, such as an API key or token.
    """


def _write_file(response, writer):
    """Write download results to disk.
    """
    with writer.open_file('wb') as fp:
        for chunk in response.iter_content(chunk_size=4096):
            if chunk:  # filter out keep-alive chunks
                fp.write(chunk)


def _direct_download(url, writer):
    """Direct download of a file from a URL.

    This is used when the materialization info contains a ``direct_url`` key,
    to simply download the file from the source.

    :param url: URL of the file to download.
    :param writer: Output writer used to write the dataset.
    """
    response = requests.get(url, allow_redirects=True, stream=True)
    response.raise_for_status()
    _write_file(response, writer)


def _proxy_download(dataset_id, writer, proxy):
    """Use a Datamart query service to materialize for us.

    This is used when the materializer is not available locally. We request
    that the Datamart handle materialization, and then we download from there.

    :param dataset_id: Dataset ID from search index.
    :param writer: Output writer used to write the dataset.
    :param proxy: URL of a Datamart server to use as a proxy.
    """
    response = requests.get(proxy + '/download/' + dataset_id,
                            allow_redirects=True, stream=True)
    response.raise_for_status()
    _write_file(response, writer)


materializers = {}
writers = {}
converters = {}

_materializers_loaded = False


def load_materializers():
    """Load materializers/writers from package entrypoint metadata.

    This is called automatically the first time we need it, or you can call it
    again if more materializers/writers get installed.
    """
    global materializers, writers, converters, _materializers_loaded
    _materializers_loaded = True

    def load(what, entry_point_name):
        result = {}
        for entry_point in iter_entry_points(entry_point_name):
            try:
                obj = entry_point.load()
            except Exception:
                logger.exception("Failed to load %s %s from %s %s",
                                 what,
                                 entry_point.name,
                                 entry_point.dist.project_name,
                                 entry_point.dist.version)
            else:
                result[entry_point.name] = obj
                logger.info("%s loaded: %s", what, entry_point.name)
        return result

    materializers = load('materializer', 'datamart_materialize')
    writers = load('writer', 'datamart_materialize.writer')
    converters = load('converter', 'datamart_materialize.converter')


def get_writer(format):
    if not _materializers_loaded:
        load_materializers()
    return writers[format]


class CsvWriter(object):
    needs_metadata = False

    def __init__(self, dataset_id, destination, metadata):
        self.destination = destination

    def open_file(self, mode='wb', name=None, **kwargs):
        if name is not None:
            raise ValueError("CsvWriter can only write single-table datasets")
        if hasattr(self.destination, 'write'):
            return self.destination
        else:
            return open(self.destination, mode, **kwargs)

    def finish(self):
        return None


def download(dataset, destination, proxy, format='csv'):
    """Materialize a dataset on disk.

    :param dataset: Dataset description from search index.
    :param destination: Path where the dataset will be written.
    :param proxy: URL of a Datamart server to use as a proxy if we can't
        materialize locally. If ``None``, ``KeyError`` will be raised if this
        materializer is unavailable.
    :param format: Output format.
    """
    if not _materializers_loaded:
        load_materializers()

    try:
        writer_cls = writers[format]
    except KeyError:
        raise ValueError("No writer for output format %r" % format)

    if isinstance(dataset, str):
        # If dataset is just an ID, we use the proxy
        if proxy:
            metadata = None
            if getattr(writer_cls, 'needs_metadata', False):
                logger.info("Obtaining metadata from proxy...")
                response = requests.get(proxy + '/metadata/' + dataset)
                response.raise_for_status()
                metadata = response.json()['metadata']
            writer = writer_cls(dataset, destination, metadata)
            _proxy_download(dataset, writer, proxy)
            return writer.finish()
        else:
            raise ValueError("A proxy must be specified to download a dataset "
                             "from its ID")
    elif not isinstance(dataset, dict):
        raise TypeError("'dataset' must be either a str or a dict")
    else:
        dataset_id = None
        materialize = dataset
        metadata = None
        if 'metadata' in materialize:
            metadata = materialize = materialize['metadata']
            dataset_id = dataset.get('id')
        if 'materialize' in materialize:
            metadata = materialize
            materialize = materialize['materialize']
            dataset_id = dataset.get('id')

    writer = writer_cls(dataset_id, destination, metadata)

    for converter in reversed(materialize.get('convert', [])):
        converter_args = dict(converter)
        converter_id = converter_args.pop('identifier')
        converter_class = converters[converter_id]
        writer = converter_class(writer, **converter_args)

    if 'direct_url' in materialize:
        logger.info("Direct download: %s", materialize['direct_url'])
        start = time.perf_counter()
        _direct_download(materialize['direct_url'], writer)
        logger.info("Download successful, %.2fs", time.perf_counter() - start)
        return writer.finish()
    elif 'identifier' in materialize:
        identifier = materialize['identifier']
        try:
            materializer = materializers[identifier]
        except KeyError:
            pass
        else:
            try:
                logger.info("Calling materializer...")
                start = time.perf_counter()
                materializer.download(materialize, writer)
                logger.info("Materializer successful, %.2fs",
                            time.perf_counter() - start)
                return writer.finish()
            except UnconfiguredMaterializer as e:
                logger.warning("Materializer is not configured properly: %s",
                               ", ".join(e.args))
        if proxy and dataset_id:
            logger.info("Calling materialization proxy...")
            start = time.perf_counter()
            _proxy_download(dataset_id, writer, proxy)
            logger.info("Materialization through proxy successful, %.2fs",
                        time.perf_counter() - start)
            return writer.finish()
        else:
            raise KeyError("Materializer unavailable: '%s'" % identifier)
    else:
        raise ValueError("Invalid materialization info")

    # unreachable
