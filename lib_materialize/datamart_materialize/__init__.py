import json
import logging
from pkg_resources import iter_entry_points
import requests


logger = logging.getLogger(__name__)


DEFAULT_URL = 'http://datamart_query:8002'


class UnconfiguredMaterializer(Exception):
    """Raised by a materializer when it is not configured.

    Some materializers need configuration, such as an API key or token.
    """


def _write(response, destination):
    """Write download results to disk.
    """
    if not hasattr(destination, 'write'):
        with open(destination, 'wb') as f:
            return _write(response, f)

    for chunk in response.iter_content(chunk_size=4096):
        if chunk:  # filter out keep-alive chunks
            destination.write(chunk)


def _direct_download(url, destination):
    """Direct download of a file from a URL.

    This is used when the materialization info contains a ``direct_url`` key,
    to simply download the file from the source.

    :param url: URL of the file to download.
    :param destination: Path where the dataset will be written.
    """
    response = requests.get(url, allow_redirects=True, stream=True)
    response.raise_for_status()
    _write(response, destination)


def _proxy_download(materialization_dict, destination, proxy):
    """Use a DataMart query service to materialize for us.

    This is used when the materializer is not available locally. We request
    that the DataMart handle materialization, and then we download from there.

    :param materialization_dict: Materialization information from search index.
    :param destination: Path where the dataset will be written.
    :param proxy: URL of a DataMart server to use as a proxy.
    """
    params = {'materialize': json.dumps(materialization_dict)}
    response = requests.get(proxy + '/download', params=params,
                            allow_redirects=True, stream=True)
    response.raise_for_status()
    _write(response, destination)


materializers = {}

_materializers_loaded = False


def load_materializers():
    """Load materializers from package entrypoint metadata.

    This is called automatically the first time we need a materializer, or you
    can call it again if more materializers get installed.
    """
    global materializers, _materializers_loaded
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


def download(materialization_dict, destination, proxy=DEFAULT_URL):
    """Materialize a dataset on disk.

    :param materialization_dict: Materialization information from search index.
    :param destination: Path where the dataset will be written.
    :param proxy: URL of a DataMart server to use as a proxy if we can't
        materialize locally. If False, ``KeyError`` will be raised if this
        materializer is unavailable.
    """
    if 'materialize' in materialization_dict:
        materialization_dict = materialization_dict['materialize']

    if 'direct_url' in materialization_dict:
        logger.info("Direct download: %s", materialization_dict['direct_url'])
        _direct_download(materialization_dict['direct_url'], destination)
    elif 'identifier' in materialization_dict:
        identifier = materialization_dict['identifier']
        if not _materializers_loaded:
            load_materializers()
        try:
            materializer = materializers[identifier]
        except KeyError:
            pass
        else:
            try:
                logger.info("Calling materializer...")
                materializer.download(materialization_dict, destination)
                logger.info("Materializer successful")
                return
            except UnconfiguredMaterializer as e:
                logger.warning("Materializer is not configured properly: %s",
                               ", ".join(e.args))
        if proxy:
            logger.info("Calling materialization proxy...")
            _proxy_download(materialization_dict, destination, proxy)
            logger.info("Materialization through proxy successful")
        else:
            raise KeyError("Materializer unavailable: '%s'" % identifier)
    else:
        raise ValueError("Invalid materialization info")
