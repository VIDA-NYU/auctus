import aio_pika
import asyncio
import elasticsearch
import logging
import json
import os
import prometheus_client
from prometheus_async.aio import time as prom_async_time
import shutil
import tornado.ioloop
from tornado.routing import URLSpec
import tornado.httputil
import tornado.web
from tornado.web import HTTPError, RequestHandler
import zipfile

from datamart_augmentation.augmentation import augment
from datamart_core.common import log_future
from datamart_core.materialize import get_dataset

from .graceful_shutdown import GracefulApplication, GracefulHandler
from .search import ClientError, parse_query, \
    get_augmentation_search_results, ProfilePostedData


logger = logging.getLogger(__name__)


BUF_SIZE = 128000
MAX_STREAMED_SIZE = 1024 * 1024 * 1024
MAX_CONCURRENT = 2
SCORE_THRESHOLD = 0.0


BUCKETS = [0.5, 1.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0, 300.0, 600.0]

PROM_SEARCH_TIME = prometheus_client.Histogram('req_search_seconds',
                                               "Search request time",
                                               buckets=BUCKETS)
PROM_SEARCH = prometheus_client.Counter('req_search_count',
                                        "Search requests")
PROM_DOWNLOAD_TIME = prometheus_client.Histogram('req_download_seconds',
                                                 "Download request time",
                                                 buckets=BUCKETS)
PROM_DOWNLOAD = prometheus_client.Counter('req_download_count',
                                          "Download requests")
PROM_DOWNLOAD_ID = prometheus_client.Counter('req_download_id_count',
                                             "Download by ID requests")
PROM_METADATA_TIME = prometheus_client.Histogram('req_metadata_seconds',
                                                 "Metadata request time",
                                                 buckets=BUCKETS)
PROM_METADATA = prometheus_client.Counter('req_metadata_count',
                                          "Metadata requests")
PROM_AUGMENT_TIME = prometheus_client.Histogram('req_augment_seconds',
                                                "Augment request time",
                                                buckets=BUCKETS)
PROM_AUGMENT = prometheus_client.Counter('req_augment_count',
                                         "Augment requests")


class BaseHandler(RequestHandler):
    """Base class for all request handlers.
    """

    def get_json(self):
        type_ = self.request.headers.get('Content-Type', '')
        if not type_.startswith('application/json'):
            raise HTTPError(400, "Expected JSON")
        return json.loads(self.request.body.decode('utf-8'))

    def send_json(self, obj):
        if isinstance(obj, list):
            obj = {'results': obj}
        elif not isinstance(obj, dict):
            raise ValueError("Can't encode %r to JSON" % type(obj))
        self.set_header('Content-Type', 'application/json; charset=utf-8')
        return self.finish(json.dumps(obj))

    def send_error_json(self, status, message):
        logger.info("Sending error %s JSON: %s", status, message)
        self.set_status(status)
        return self.send_json({'error': message})


class CorsHandler(BaseHandler):
    def prepare(self):
        super(CorsHandler, self).prepare()
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Methods', 'POST')
        self.set_header('Access-Control-Allow-Headers', 'Content-Type')

    def options(self):
        # CORS pre-flight
        self.set_status(204)
        return self.finish()


class Search(CorsHandler, GracefulHandler, ProfilePostedData):
    @prom_async_time(PROM_SEARCH_TIME)
    async def post(self):
        PROM_SEARCH.inc()

        type_ = self.request.headers.get('Content-type', '')
        data = None
        if type_.startswith('application/json'):
            query = self.get_json()
        elif (type_.startswith('multipart/form-data') or
                type_.startswith('application/x-www-form-urlencoded')):
            query = self.get_body_argument('query', None)
            if query is None and 'query' in self.request.files:
                query = self.request.files['query'][0].body.decode('utf-8')
            if query is not None:
                query = json.loads(query)
            data = self.get_body_argument('data', None)
            if 'data' in self.request.files:
                data = self.request.files['data'][0].body
            elif data is not None:
                data = data.encode('utf-8')
        elif (type_.startswith('text/csv') or
                type_.startswith('application/csv')):
            query = None
            data = self.request.body
        else:
            return self.send_error_json(
                400,
                "Either use multipart/form-data to send the 'data' file and "
                "'query' JSON, or use application/json to send a query alone, "
                "or use text/csv to send data alone",
            )

        logger.info("Got search, content-type=%r%s%s",
                    type_.split(';')[0],
                    ', query' if query else '',
                    ', data' if data else '')

        # parameter: data
        data_profile = dict()
        if data:
            try:
                data_path, data_profile = self.handle_data_parameter(data)
            except ClientError as e:
                return self.send_error_json(400, e.args[0])

        # parameter: query
        query_args = list()
        tabular_variables = list()
        if query:
            try:
                query_args, tabular_variables = parse_query(query)
            except ClientError as e:
                return self.send_error_json(400, e.args[0])

        # At least one of them must be provided
        if not query_args and not data_profile:
            return self.send_error_json(
                400,
                "At least one of the input parameters must be provided.",
            )

        if not data_profile:
            hits = self.application.elasticsearch.search(
                index='datamart',
                body={
                    'query': {
                        'bool': {
                            'must': query_args,
                        },
                    },
                },
                size=1000
            )['hits']['hits']

            results = []
            for h in hits:
                meta = h.pop('_source')
                if 'description' in meta and len(meta['description']) > 100:
                    meta['description'] = meta['description'][:97] + "..."
                results.append(dict(
                    id=h['_id'],
                    score=h['_score'],
                    metadata=meta,
                    augmentation={
                        'type': 'none',
                        'left_columns': [],
                        'right_columns': []
                    }
                ))
            return self.send_json(results)
        else:
            return self.send_json(
                get_augmentation_search_results(
                    self.application.elasticsearch,
                    data_profile,
                    query_args,
                    tabular_variables,
                    SCORE_THRESHOLD
                )
            )


class RecursiveZipWriter(object):
    def __init__(self, write):
        self._write = write
        self._zip = zipfile.ZipFile(self, 'w')

    def write_recursive(self, src, dst=''):
        if os.path.isdir(src):
            for name in os.listdir(src):
                self.write_recursive(os.path.join(src, name),
                                     dst + '/' + name if dst else name)
        else:
            self._zip.write(src, dst)

    def write(self, data):
        self._write(data)
        return len(data)

    def flush(self):
        return

    def close(self):
        self._zip.close()


class BaseDownload(BaseHandler):
    def send_dataset(self, dataset_id, metadata, output_format='csv'):
        materialize = metadata.get('materialize', {})

        # If there's a direct download URL
        if ('direct_url' in materialize and
                output_format == 'csv' and not materialize.get('convert')):
            # Redirect the client to it
            self.redirect(materialize['direct_url'])
        else:
            getter = get_dataset(metadata, dataset_id, format=output_format)
            try:
                dataset_path = getter.__enter__()
            except Exception:
                self.send_error_json(500, "Materializer reports failure")
                raise
            try:
                if os.path.isfile(dataset_path):
                    self.set_header('Content-Type', 'application/octet-stream')
                    self.set_header('X-Content-Type-Options', 'nosniff')
                    self.set_header('Content-Disposition',
                                    'attachment; filename="%s"' % dataset_id)
                    with open(dataset_path, 'rb') as fp:
                        buf = fp.read(4096)
                        while buf:
                            self.write(buf)
                            if len(buf) != 4096:
                                break
                            buf = fp.read(4096)
                else:  # Directory
                    self.set_header('Content-Type', 'application/zip')
                    self.set_header(
                        'Content-Disposition',
                        'attachment; filename="%s.zip"' % dataset_id)
                    writer = RecursiveZipWriter(self.write)
                    writer.write_recursive(dataset_path)
                    writer.close()
                return self.finish()
            finally:
                getter.__exit__(None, None, None)


class DownloadId(CorsHandler, GracefulHandler, BaseDownload):
    @PROM_DOWNLOAD_TIME.time()
    def get(self, dataset_id):
        PROM_DOWNLOAD_ID.inc()

        output_format = self.get_query_argument('format', 'csv')

        # Get materialization data from Elasticsearch
        try:
            metadata = self.application.elasticsearch.get(
                'datamart', '_doc', id=dataset_id
            )['_source']
        except elasticsearch.NotFoundError:
            raise HTTPError(404)

        return self.send_dataset(dataset_id, metadata, output_format)


class Download(CorsHandler, GracefulHandler, BaseDownload, ProfilePostedData):
    @PROM_DOWNLOAD_TIME.time()
    def post(self):
        PROM_DOWNLOAD.inc()

        type_ = self.request.headers.get('Content-type', '')

        task = None
        data = None
        output_format = 'd3m'
        if type_.startswith('application/json'):
            task = self.get_json()
        elif (type_.startswith('multipart/form-data') or
                type_.startswith('application/x-www-form-urlencoded')):
            task = self.get_body_argument('task', None)
            if task is None and 'task' in self.request.files:
                task = self.request.files['task'][0].body.decode('utf-8')
            data = self.get_body_argument('data', None)
            if 'data' in self.request.files:
                data = self.request.files['data'][0].body
            elif data is not None:
                data = data.encode('utf-8')
            output_format = self.get_argument('format', None)
            if output_format is None and 'format' in self.request.files:
                output_format = (
                    self.request.files['format'][0].body.decode('utf-8'))
            if output_format is None:
                output_format = 'csv'
        if task is None:
            return self.send_error_json(
                400,
                "Either use multipart/form-data to send the 'data' file and "
                "'task' JSON, or use application/json to send 'task' alone",
            )

        task = json.loads(task)

        # materialize augmentation data
        metadata = task['metadata']

        if not data:
            return self.send_dataset(task['id'], metadata, output_format)
        else:
            # data
            try:
                data_path, data_profile = self.handle_data_parameter(data)
            except ClientError as e:
                return self.send_error_json(400, e.args[0])

            # first, look for possible augmentation
            search_results = get_augmentation_search_results(
                es=self.application.elasticsearch,
                data_profile=data_profile,
                query_args=None,
                tabular_variables=None,
                score_threshold=SCORE_THRESHOLD,
                dataset_id=task['id'],
                union=False
            )

            if not search_results:
                return self.send_error_json(
                    400,
                    "The DataMart dataset referenced by 'task' cannot augment "
                    "'data'.",
                )

            task = search_results[0]

            with get_dataset(metadata, task['id'], format='csv') as newdata:
                # perform augmentation
                new_path = augment(
                    data_path,
                    newdata,
                    data_profile,
                    task,
                    return_only_datamart_data=True
                )

            # send a zip file
            self.set_header('Content-Type', 'application/zip')
            self.set_header(
                'Content-Disposition',
                'attachment; filename="augmentation.zip"')
            writer = RecursiveZipWriter(self.write)
            writer.write_recursive(new_path)
            writer.close()
            shutil.rmtree(os.path.abspath(os.path.join(new_path, '..')))


class Metadata(CorsHandler, GracefulHandler):
    @PROM_METADATA_TIME.time()
    def get(self, dataset_id):
        PROM_METADATA.inc()

        es = self.application.elasticsearch
        try:
            metadata = es.get('datamart', '_doc', id=dataset_id)['_source']
        except elasticsearch.NotFoundError:
            raise HTTPError(404)

        return self.send_json(metadata)


class Augment(CorsHandler, GracefulHandler, ProfilePostedData):
    @prom_async_time(PROM_AUGMENT_TIME)
    async def post(self):
        PROM_AUGMENT.inc()

        type_ = self.request.headers.get('Content-type', '')
        if not type_.startswith('multipart/form-data'):
            return self.send_error_json(400, "Use multipart/form-data to send "
                                             "the 'data' file and 'task' JSON")

        task = self.get_body_argument('task', None)
        if task is None and 'task' in self.request.files:
            task = self.request.files['task'][0].body.decode('utf-8')
        if task is None:
            return self.send_error_json(400, "Missing 'task' JSON")
        task = json.loads(task)

        destination = self.get_argument('destination', None)

        data = self.get_body_argument('data', None)
        if data is not None:
            data = data.encode('utf-8')
        elif 'data' in self.request.files:
            data = self.request.files['data'][0].body
        else:
            return self.send_error_json(400, "Missing 'data'")

        columns = self.get_body_argument('columns', None)
        if 'columns' in self.request.files:
            columns = self.request.files['columns'][0].body.decode('utf-8')
        if columns is not None:
            columns = json.loads(columns)

        # data
        try:
            data_path, data_profile = self.handle_data_parameter(data)
        except ClientError as e:
            return self.send_error_json(400, e.args[0])

        # materialize augmentation data
        metadata = task['metadata']

        # no augmentation task provided -- will first look for possible augmentation
        if task['augmentation']['type'] == 'none':
            search_results = get_augmentation_search_results(
                es=self.application.elasticsearch,
                data_profile=data_profile,
                query_args=None,
                tabular_variables=None,
                score_threshold=SCORE_THRESHOLD,
                dataset_id=task['id'],
                union=False
            )

            if search_results:
                # get first result
                task = search_results[0]
            else:
                return self.send_error_json(400,
                                            "The DataMart dataset referenced "
                                            "by 'task' cannot augment 'data'.")

        with get_dataset(metadata, task['id'], format='csv') as newdata:
            # perform augmentation
            new_path = augment(
                data_path,
                newdata,
                data_profile,
                task,
                columns=columns,
                destination=destination
            )

        if destination:
            # send the path
            self.set_header('Content-Type', 'text/plain; charset=utf-8')
            self.write(new_path)
        else:
            # send a zip file
            self.set_header('Content-Type', 'application/zip')
            self.set_header(
                'Content-Disposition',
                'attachment; filename="augmentation.zip"')
            writer = RecursiveZipWriter(self.write)
            writer.write_recursive(new_path)
            writer.close()
            shutil.rmtree(os.path.abspath(os.path.join(new_path, '..')))

        return self.finish()


class Health(CorsHandler):
    def get(self):
        if self.application.is_closing:
            self.set_status(503, reason="Shutting down")
            return self.finish('shutting down')
        else:
            return self.finish('ok')


class Application(GracefulApplication):
    def __init__(self, *args, es, **kwargs):
        super(Application, self).__init__(*args, **kwargs)

        self.is_closing = False

        self.work_tickets = asyncio.Semaphore(MAX_CONCURRENT)

        self.elasticsearch = es
        self.channel = None

        log_future(asyncio.get_event_loop().create_task(self._amqp()), logger)

    async def _amqp(self):
        connection = await aio_pika.connect_robust(
            host=os.environ['AMQP_HOST'],
            login=os.environ['AMQP_USER'],
            password=os.environ['AMQP_PASSWORD'],
        )
        self.channel = await connection.channel()
        await self.channel.set_qos(prefetch_count=1)

    def log_request(self, handler):
        if handler.request.path == '/health':
            return
        super(Application, self).log_request(handler)


def make_app(debug=False):
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )

    return Application(
        [
            URLSpec('/search', Search, name='search'),
            URLSpec('/download/([^/]+)', DownloadId, name='download_id'),
            URLSpec('/download', Download, name='download'),
            URLSpec('/metadata/([^/]+)', Metadata, name='metadata'),
            URLSpec('/augment', Augment, name='augment'),
            URLSpec('/health', Health, name='health'),
        ],
        debug=debug,
        serve_traceback=True,
        es=es,
    )


def main():
    logging.root.handlers.clear()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")
    prometheus_client.start_http_server(8000)

    app = make_app()
    app.listen(8002, xheaders=True, max_buffer_size=2147483648)
    loop = tornado.ioloop.IOLoop.current()
    loop.start()
