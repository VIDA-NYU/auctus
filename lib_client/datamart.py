import io
import json
import logging
import os
import pandas as pd
import requests
import shutil
import tempfile
import warnings
import zipfile


__version__ = '0.5'


__all__ = ['Dataset', 'search']


logger = logging.getLogger(__name__)


DEFAULT_URL = 'https://datamart.d3m.vida-nyu.org'


class DatamartError(RuntimeError):
    """Error from DataMart."""


def handle_data(data, send_data):
    if isinstance(data, pd.DataFrame):
        s_buf = io.StringIO()
        data.to_csv(s_buf, index=False)
        s_buf.seek(0)
        return s_buf
    elif isinstance(data, Dataset):
        raise DatamartError("To have a Dataset object as input, please "
                            "use the parameter 'augment_data' in "
                            "the 'augment' function.")
    elif isinstance(data, dict) and len(data.keys()) > 0:
        # d3m.container.Dataset
        s_buf = io.StringIO()
        data[list(data.keys())[0]].to_csv(s_buf, index=False)
        s_buf.seek(0)
        return s_buf
    else:
        if not send_data:
            return data
        else:
            if os.path.isdir(data):
                # path to a D3M dataset
                data_file = os.path.join(data, 'tables', 'learningData.csv')
                if not os.path.exists(data_file):
                    raise DatamartError(
                        "Error from DataMart: '%s' does not exist." % data_file)
                return open(data_file)
            else:
                # path to a CSV file
                if not os.path.exists(data):
                    raise DatamartError(
                        "Error from DataMart: '%s' does not exist." % data)
                return open(data)


def handle_response(response, format):
    type_ = response.headers.get('Content-Type', '')
    if type_.startswith('application/zip'):
        # saving zip file
        buf = io.BytesIO(response.content)
        buf.seek(0)
        temp_file = tempfile.NamedTemporaryFile(mode='wb', delete=False)
        shutil.copyfileobj(buf, temp_file)
        temp_file.close()

        # reading zip file
        zip = zipfile.ZipFile(temp_file.name, 'r')

        if 'pandas' in format:
            learning_data = pd.read_csv(zip.open('tables/learningData.csv'))
            dataset_doc = json.load(zip.open('datasetDoc.json'))
            zip.close()
            os.remove(temp_file.name)

            return learning_data, dataset_doc
        elif 'd3m' in format:

            try:
                from d3m.container import Dataset
            except ImportError:
                raise RuntimeError('d3m.container.Dataset not found')

            temp_dir = tempfile.mkdtemp()
            zip.extractall(temp_dir)
            zip.close()

            d3m_dataset = Dataset.load('file://' + os.path.join(temp_dir, 'datasetDoc.json'))

            os.remove(temp_file.name)
            shutil.rmtree(temp_dir)

            return d3m_dataset
        else:
            return None
    elif type_.startswith('text/plain'):
        return response.content.decode('utf-8')
    else:
        raise RuntimeError('Unrecognized content type: "%s"' % type_)


def search(url=DEFAULT_URL, query=None, data=None, send_data=False,
           timeout=None):
    """Search for datasets.

    :param query: JSON object describing the query.
    :param data: the data you are trying to augment.
        For now, it can be a path to a CSV file (str),
        a path to a D3M dataset directory (str),
        or a pandas.DataFrame object.
    :param send_data: if False, send the data path; if True, send
        the data.
    """

    files = dict()
    if data is not None:
        files['data'] = handle_data(data, send_data)
    if query:
        files['query'] = json.dumps(query)

    # Send request
    response = requests.post(url + '/search', timeout=timeout,
                             files=files)
    if response.status_code != 200:
        raise DatamartError("Error from DataMart: %s %s" % (
            response.status_code, response.reason))

    # Parse response
    return [Dataset.from_json(result, url)
            for result in response.json()['results']]


def download(dataset, destination, url=DEFAULT_URL, proxy=None, format='csv',
             timeout=None):
    if isinstance(dataset, Dataset):
        dataset.download(destination, proxy, format)
    elif not isinstance(dataset, str):
        raise TypeError("'dataset' argument should be a str or Dataset object")

    if format != 'd3m' and not hasattr(destination, 'write'):
        with open(destination, 'wb') as f:
            return download(dataset, f, url, proxy, format)

    url = url + '/download/%s?format=%s' % (dataset, format)
    response = requests.get(url,
                            allow_redirects=True,
                            stream=True,
                            timeout=timeout)
    if response.status_code != 200:
        if response.headers.get('Content-Type') == 'application/json':
            try:
                raise DatamartError("Error from DataMart: %s" %
                                    response.json()['error'])
            except (KeyError, ValueError):
                pass
        raise DatamartError("Error from DataMart: %s %s" % (
            response.status_code, response.reason))

    if format != 'd3m':
        for chunk in response.iter_content(chunk_size=4096):
            if chunk:  # filter out keep-alive chunks
                destination.write(chunk)
    else:
        # Download D3M ZIP to temporary file
        fd, tmpfile = tempfile.mkstemp(prefix='datamart_download_',
                                       suffix='.d3m.zip')
        try:
            with open(tmpfile, 'wb') as f:
                for chunk in response.iter_content(chunk_size=4096):
                    if chunk:  # filter out keep-alive chunks
                        f.write(chunk)

            # Unzip
            zip = zipfile.ZipFile(tmpfile)
            zip.extractall(destination)
        finally:
            os.close(fd)
            os.remove(tmpfile)


def augment(data, augment_data, destination=None, format='pandas', send_data=False):
    """Augments data with augment_data.

    :param data: the data you are trying to augment.
        For now, it can be a path to a CSV file (str),
        a path to a D3M dataset directory (str),
        or a pandas.DataFrame object.
    :param send_data: if False, send the data path; if True, send
        the data.
    :param augment_data: the dataset that will be augmented with
        data (a datamart.Dataset object).
    :param destination: the location in disk where the new data
        will be saved (optional). DataMart must have access to
        the path.
    :param format: the format of the output, if destination is not defined.
        Either 'pandas' for a pandas.DataFrame object, or 'd3m'
        for a d3m.container.Dataset object.
    """

    files = dict()
    files['data'] = handle_data(data, send_data)
    files['task'] = json.dumps(augment_data.get_json())
    if destination:
        files['destination'] = destination

    # Send request
    response = requests.post(augment_data.url + '/augment',
                             files=files)
    if response.status_code != 200:
        raise DatamartError("Error from DataMart: %s %s" % (
            response.status_code, response.reason))

    return handle_response(response, format)


def join(left_data, right_data, left_columns,
         right_columns, destination=None, format='pandas',
         send_data=False, url=DEFAULT_URL):
    """Joins two datasets.

    :param left_data: the left-side dataset for join.
        For now, it can be a path to a CSV file (str),
        a path to a D3M dataset directory (str),
        or a pandas.DataFrame object.
    :param right_data: the right-side dataset for join.
        For now, it can be a path to a CSV file (str),
        a path to a D3M dataset directory (str),
        or a pandas.DataFrame object.
    :param left_columns: a list of lists of indices(int)/headers(str)
        of the left-side dataset
    :param right_columns: a list of lists of indices(int)/headers(str)
        of the right-side dataset
    :param send_data: if False, send the data path; if True, send
        the data.
    :param destination: the location in disk where the new data
        will be saved (optional). DataMart must have access to
        the path.
    :param format: the format of the output, if destination is not defined.
        Either 'pandas' for a pandas.DataFrame object, or 'd3m'
        for a d3m.container.Dataset object.
    """

    files = dict()
    files['left_data'] = handle_data(left_data, send_data)
    files['right_data'] = handle_data(right_data, send_data)
    files['columns'] = json.dumps(dict(left_columns=left_columns,
                                       right_columns=right_columns))
    if destination:
        files['destination'] = destination

    # Send request
    response = requests.post(url + '/join', files=files)
    if response.status_code != 200:
        raise DatamartError("Error from DataMart: %s %s" % (
            response.status_code, response.reason))

    return handle_response(response, format)


def union(left_data, right_data, left_columns,
          right_columns, destination=None, format='pandas',
          send_data=False, url=DEFAULT_URL):
    """Unions two datasets.

    :param left_data: the first dataset for union.
        For now, it can be a path to a CSV file (str),
        a path to a D3M dataset directory (str),
        or a pandas.DataFrame object.
    :param right_data: the second dataset for union.
        For now, it can be a path to a CSV file (str),
        a path to a D3M dataset directory (str),
        or a pandas.DataFrame object.
    :param left_columns: a list of lists of indices(int)/headers(str)
        of the left dataset
    :param right_columns: a list of lists of indices(int)/headers(str)
        of the right dataset
    :param send_data: if False, send the data path; if True, send
        the data.
    :param destination: the location in disk where the new data
        will be saved (optional). DataMart must have access to
        the path.
    :param format: the format of the output, if destination is not defined.
        Either 'pandas' for a pandas.DataFrame object, or 'd3m'
        for a d3m.container.Dataset object.
    """

    files = dict()
    files['left_data'] = handle_data(left_data, send_data)
    files['right_data'] = handle_data(right_data, send_data)
    files['columns'] = json.dumps(dict(left_columns=left_columns,
                                       right_columns=right_columns))
    if destination:
        files['destination'] = destination

    # Send request
    response = requests.post(url + '/union', files=files)
    if response.status_code != 200:
        raise DatamartError("Error from DataMart: %s %s" % (
            response.status_code, response.reason))

    return handle_response(response, format)


class Dataset(object):
    """Pointer to a dataset on DataMart.
    """
    def __init__(self, id, metadata, url=DEFAULT_URL,
                 score=None, join_columns=[], union_columns=[]):
        self.id = id
        self.url = url
        self.score = score
        self.metadata = metadata
        self.join_columns = join_columns
        self.union_columns = union_columns

    @classmethod
    def from_json(cls, result, url=DEFAULT_URL):
        join_columns = [] if 'join_columns' not in result else result['join_columns']
        union_columns = [] if 'union_columns' not in result else result['union_columns']
        return cls(id=result['id'], metadata=result['metadata'],
                   url=url, score=result['score'],
                   join_columns=join_columns,
                   union_columns=union_columns)

    def get_json(self):
        result = dict(
            id=self.id,
            score=self.score,
            metadata=self.metadata
        )
        if self.join_columns:
            result['join_columns'] = self.join_columns
        elif self.union_columns:
            result['union_columns'] = self.union_columns
        else:
            raise RuntimeError('There is no augmentation task to perform.')
        return result

    def get_augmentation_information(self):
        """Returns the pairs of columns for union and join, if applicable.
        """

        return dict(union=self.union_columns, join=self.join_columns)

    def download(self, destination, proxy=None, format='csv'):
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
                    proxy=self.url if proxy is None else None,
                    format=format,
                )
                return

        download(self.id, destination, self.url, proxy, format)

    def __repr__(self):
        if self.join_columns or self.union_columns:
            if self.join_columns:
                augmentation = 'join'
            else:
                augmentation = 'union'
            return '<Dataset %r score=%r augmentation=%s>' % (self.id, self.score, augmentation)
        return '<Dataset %r score=%r>' % (self.id, self.score)
