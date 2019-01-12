import logging
import os
import requests
import warnings


__version__ = '0.0'


__all__ = ['Dataset', 'search']


logger = logging.getLogger(__name__)


DEFAULT_URL = 'https://datamart.d3m.vida-nyu.org'


class DatamartError(RuntimeError):
    """Error from DataMart."""


def search(url=DEFAULT_URL, query=None, data=None, send_data=False):
    """Search for datasets.

    :param query: JSON object describing the query.
    :param data: the data you are trying to augment.
        For now, it can be a path to a CSV file (str),
        a path to a D3M dataset directory (str).
    :param send_data: if False, send the data path; if True, send
        the data.
    :return: None.
    """

    request = dict()
    if data:
        if not send_data:
            request['data'] = data
        else:
            if os.path.isdir(data):
                # path to a D3M dataset
                data_file = os.path.join(data, 'tables', 'learningData.csv')
                if not os.path.exists(data_file):
                    raise DatamartError(
                        "Error from DataMart: '%s' does not exist." % data_file)
                request['data'] = open(data_file).read()
            else:
                # path to a CSV file
                if not os.path.exists(data):
                    raise DatamartError(
                        "Error from DataMart: '%s' does not exist." % data)
                request['data'] = open(data).read()
    if query:
        request['query'] = query

    # Send request
    response = requests.post(url + '/search',
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
                 score=None, join_columns=[], union_columns=[]):
        self.id = id
        self._url = url
        self.score = score
        self.metadata = metadata
        self.join_columns = join_columns
        self.union_columns = union_columns

    @classmethod
    def from_json(cls, result, url=DEFAULT_URL):
        return cls(id=result['id'], metadata=result['metadata'],
                   url=url, score=result['score'])

    def get_augmentation_information(self):
        """Returns the pairs of columns for union and join, if applicable.
        """

        return dict(union=self.union_columns, join=self.join_columns)

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
        return '<Dataset %r score=%r>' % (self.id, self.score)
