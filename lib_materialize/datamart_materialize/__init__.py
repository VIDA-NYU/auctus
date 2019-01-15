import logging
from pkg_resources import iter_entry_points
import requests


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
    """Use a DataMart query service to materialize for us.

    This is used when the materializer is not available locally. We request
    that the DataMart handle materialization, and then we download from there.

    :param dataset_id: Dataset ID from search index.
    :param writer: Output writer used to write the dataset.
    :param proxy: URL of a DataMart server to use as a proxy.
    """
    response = requests.get(proxy + '/download/' + dataset_id,
                            allow_redirects=True, stream=True)
    response.raise_for_status()
    _write_file(response, writer)


materializers = {}
writers = {}

_materializers_loaded = False


def load_materializers():
    """Load materializers/writers from package entrypoint metadata.

    This is called automatically the first time we need it, or you can call it
    again if more materializers/writers get installed.
    """
    global materializers, writers, _materializers_loaded
    _materializers_loaded = True

    materializers = {}
    for entry_point in iter_entry_points('datamart_materialize'):
        try:
            materializer = entry_point.load()
        except Exception:
            logger.exception("Failed to load materializer %s from %s %s",
                             entry_point.name,
                             entry_point.dist.project_name,
                             entry_point.dist.version)
        else:
            materializers[entry_point.name] = materializer
            logger.info("Materializer loaded: %s", entry_point.name)

    writers = {}
    for entry_point in iter_entry_points('datamart_materialize.writer'):
        try:
            writer = entry_point.load()
        except Exception:
            logger.exception("Failed to load writer %s from %s %s",
                             entry_point.name,
                             entry_point.dist.project_name,
                             entry_point.dist.version)
        else:
            writers[entry_point.name] = writer
            logger.info("Writer loaded: %s", entry_point.name)


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
        return open(self.destination, mode, **kwargs)

    def finish(self):
        return None


def download(dataset, destination, proxy, format='csv'):
    """Materialize a dataset on disk.

    :param dataset: Dataset description from search index.
    :param destination: Path where the dataset will be written.
    :param proxy: URL of a DataMart server to use as a proxy if we can't
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
                metadata = response.json()
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

    if 'direct_url' in materialize:
        logger.info("Direct download: %s", materialize['direct_url'])
        _direct_download(materialize['direct_url'], writer)
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
                materializer.download(materialize, writer)
                logger.info("Materializer successful")
                return writer.finish()
            except UnconfiguredMaterializer as e:
                logger.warning("Materializer is not configured properly: %s",
                               ", ".join(e.args))
        if proxy and dataset_id:
            logger.info("Calling materialization proxy...")
            _proxy_download(dataset_id, writer, proxy)
            logger.info("Materialization through proxy successful")
            return writer.finish()
        else:
            raise KeyError("Materializer unavailable: '%s'" % identifier)
    else:
        raise ValueError("Invalid materialization info")

    # unreachable
