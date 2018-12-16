import logging
import requests
import warnings


__version__ = '0.0'


__all__ = ['Dataset', 'search']


logger = logging.getLogger(__name__)


DEFAULT_URL = 'https://datamart.d3m.vida-nyu.org'


class DatamartError(RuntimeError):
    """Error from DataMart."""


def search(url=DEFAULT_URL, fail=False, **kwargs):
    """Search for datasets.

    :param url: URL of the DataMart system. Defaults to
        ``datamart.d3m.vida-nyu.org``
    :param fail: Whether to raise an exception if some keywords are not
        understood.
    :param kwargs: Search parameters.
    :return: A list of ``Dataset`` objects.
    """
    # Read arguments, build request
    request = {}
    unsupported = None
    if 'keywords' in kwargs:
        request['keywords'] = kwargs.pop('keywords')
    if kwargs:
        unsupported = ', '.join(sorted(kwargs.keys()))
        unsupported = "Unsupported arguments: %s" % unsupported

    # TODO: Profile data if possible, upload otherwise

    # Report errors
    if unsupported and not fail:
        logger.warning(unsupported)
    if not request:
        raise ValueError("Empty query")
    if unsupported and fail:
        raise ValueError(unsupported)

    # Send request
    response = requests.post(url + '/query',
                             headers={'Accept': 'application/json',
                                      'Content-Type': 'application/json'},
                             json=request)
    if response.status_code != 200:
        raise DatamartError("Error from DataMart: %s %s" % (
            response.status_code, response.reason))

    # Parse response
    return [Dataset.from_json(result, url)
            for result in response.json()['results']]


class Dataset(object):
    """Pointer to a dataset on DataMart.
    """
    def __init__(self, id, metadata, url=DEFAULT_URL,
                 score=None, discoverer=None):
        self.id = id
        self._url = url
        self.score = score
        self.discoverer = discoverer
        self.metadata = metadata

    @classmethod
    def from_json(cls, result, url=DEFAULT_URL):
        return cls(result['id'], result['metadata'], url=url,
                   score=result['score'], discoverer=result['discoverer'])

    def download(self, destination, proxy=None):
        """Download this dataset to the disk.

        :param destination: Path or opened file where to write the data.
        :param proxy: Whether we should have the DataMart server do the
            materialization for us. If ``False``, raise an error unless we can
            do it locally; if ``True``, do not attempt to materialize locally,
            only use the server. If ``None`` (default), use the server if we
            can't materialize locally.
        """
        if not proxy:
            try:
                import datamart_materialize
            except ImportError:
                if proxy is False:
                    raise RuntimeError("proxy=False but datamart_materialize "
                                       "is not installed locally")
                warnings.warn("datamart_materialize is not installed, "
                              "DataMart server will do materialization for us")
            else:
                datamart_materialize.download(
                    dataset={'id': self.id, 'metadata': self.metadata},
                    destination=destination,
                    proxy=self._url if proxy is None else None,
                )
                return

        if not hasattr(destination, 'write'):
            with open(destination, 'wb') as f:
                return self.download(f)

        response = requests.get(self._url + '/download/%s' % self.id,
                                allow_redirects=True,
                                stream=True)
        if response.status_code != 200:
            if response.headers.get('Content-Type') == 'application/json':
                try:
                    raise DatamartError("Error from DataMart: %s" %
                                        response.json()['error'])
                except (KeyError, ValueError):
                    pass
            raise DatamartError("Error from DataMart: %s %s" % (
                response.status_code, response.reason))

        for chunk in response.iter_content(chunk_size=4096):
            if chunk:  # filter out keep-alive chunks
                destination.write(chunk)

    def __repr__(self):
        return '<Dataset %r score=%r discoverer=%r>' % (self.id, self.score,
                                                        self.discoverer)
