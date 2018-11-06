import logging
import requests


__version__ = '0.0'


__all__ = ['Dataset', 'search']


logger = logging.getLogger(__name__)


DEFAULT_URL = 'http://datamart_query:8002'


class DatamartError(RuntimeError):
    """Error from DataMart."""


def search(url=DEFAULT_URL, fail=False, **kwargs):
    # Read arguments, build request
    request = {}
    unsupported = None
    if 'keywords' in kwargs:
        request['keywords'] = kwargs.pop('keywords')
    if kwargs:
        unsupported = ', '.join(sorted(kwargs.keys()))
        unsupported = "Unsupported arguments: %s" % unsupported

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

    def _request(self, stream=True):
        response = requests.get(self._url + '/download/%s' % self.id,
                                allow_redirects=True,
                                stream=stream)
        if response.status_code != 200:
            if response.headers.get('Content-Type') == 'application/json':
                try:
                    raise DatamartError("Error from DataMart: %s" %
                                        response.json()['error'])
                except (KeyError, ValueError):
                    pass
            raise DatamartError("Error from DataMart: %s %s" % (
                response.status_code, response.reason))
        return response

    def download(self, destination):
        if not hasattr(destination, 'write'):
            with open(destination, 'wb') as f:
                return self.download(destination)

        response = self._request(stream=True)
        for chunk in response.iter_content(chunk_size=4096):
            if chunk:  # filter out keep-alive new chunks
                destination.write(chunk)

    def to_dataframe(self):
        import pandas

        response = self._request(stream=True)
        return pandas.read_csv(response.raw)

    def __repr__(self):
        return '<Dataset %r score=%r discoverer=%r>' % (self.id, self.score,
                                                        self.discoverer)
