import aio_pika
import asyncio
from datetime import datetime
from dateutil.parser import parse
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
from datamart_core.common import log_future, Type
from datamart_core.materialize import get_dataset

from .graceful_shutdown import GracefulApplication, GracefulHandler
from .search import ClientError, get_augmentation_search_results, \
    handle_data_parameter


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


class Search(CorsHandler, GracefulHandler):
    def parse_query_variables(self, data, tabular_variables=None):
        output = list()

        if not data:
            return output

        for variable in data:
            if 'type' not in variable:
                return self.send_error_json(
                    400,
                    'variable is missing property "type"',
                )
            variable_query = list()

            # temporal variable
            # TODO: handle 'granularity'
            if 'temporal_variable' in variable['type']:
                variable_query.append({
                    'nested': {
                        'path': 'columns',
                        'query': {
                            'match': {'columns.semantic_types': Type.DATE_TIME},
                        },
                    },
                })
                start = end = None
                if 'start' in variable and 'end' in variable:
                    try:
                        start = parse(variable['start']).timestamp()
                        end = parse(variable['end']).timestamp()
                    except Exception:
                        pass
                elif 'start' in variable:
                    try:
                        start = parse(variable['start']).timestamp()
                        end = datetime.now().timestamp()
                    except Exception:
                        pass
                elif 'end' in variable:
                    try:
                        start = 0
                        end = parse(variable['end']).timestamp()
                    except Exception:
                        pass
                else:
                    pass
                if start and end:
                    variable_query.append({
                        'nested': {
                            'path': 'columns.coverage',
                            'query': {
                                'range': {
                                    'columns.coverage.range': {
                                        'gte': start,
                                        'lte': end,
                                        'relation': 'intersects'
                                    }
                                }
                            }
                        }
                    })

            # geospatial variable
            # TODO: handle 'granularity'
            elif 'geospatial_variable' in variable['type']:
                if ('latitude1' not in variable or
                        'latitude2' not in variable or
                        'longitude1' not in variable or
                        'longitude2' not in variable):
                    continue
                longitude1 = min(
                    float(variable['longitude1']),
                    float(variable['longitude2'])
                )
                longitude2 = max(
                    float(variable['longitude1']),
                    float(variable['longitude2'])
                )
                latitude1 = max(
                    float(variable['latitude1']),
                    float(variable['latitude2'])
                )
                latitude2 = min(
                    float(variable['latitude1']),
                    float(variable['latitude2'])
                )
                variable_query.append({
                    'nested': {
                        'path': 'spatial_coverage.ranges',
                        'query': {
                            'bool': {
                                'filter': {
                                    'geo_shape': {
                                        'spatial_coverage.ranges.range': {
                                            'shape': {
                                                'type': 'envelope',
                                                'coordinates':
                                                    [[longitude1, latitude1],
                                                     [longitude2, latitude2]]
                                            },
                                            'relation': 'intersects'
                                        }
                                    }
                                }
                            }
                        }
                    }
                })

            # tabular variable
            # TODO: handle 'relationship'
            #  for now, it assumes the relationship is 'contains'
            elif 'tabular_variable' in variable['type']:
                if 'columns' in variable:
                    for column_index in variable['columns']:
                        tabular_variables.append(column_index)

            if variable_query:
                output.append({
                    'bool': {
                        'must': variable_query,
                    }
                })

        if output:
            return {
                'bool': {
#                    'should': output,
#                    'minimum_should_match': 1,
                    'must': output
                }
            }
        return {}

    def parse_query(self, query_json):
        query_args = list()

        # keywords
        keywords_query_all = list()
        if 'keywords' in query_json and query_json['keywords']:
            if not isinstance(query_json['keywords'], list):
                return self.send_error_json(400, '"keywords" must be an array')
            keywords_query = list()
            # description
            keywords_query.append({
                'match': {
                    'description': {
                        'query': ' '.join(query_json['keywords']),
                        'operator': 'or'
                    }
                }
            })
            # name
            keywords_query.append({
                'match': {
                    'name': {
                        'query': ' '.join(query_json['keywords']),
                        'operator': 'or'
                    }
                }
            })
            # keywords
            for name in query_json['keywords']:
                keywords_query.append({
                    'nested': {
                        'path': 'columns',
                        'query': {
                            'match': {'columns.name': name},
                        },
                    },
                })
                keywords_query.append({
                    'wildcard': {
                        'materialize.identifier': '*%s*' % name.lower()
                    }
                })
            keywords_query_all.append({
                'bool': {
                    'should': keywords_query,
                    'minimum_should_match': 1
                }
            })

        if keywords_query_all:
            query_args.append(keywords_query_all)

        # tabular_variables
        tabular_variables = []

        # variables
        variables_query = None
        if 'variables' in query_json:
            variables_query = self.parse_query_variables(
                query_json['variables'],
                tabular_variables=tabular_variables
            )

        if variables_query:
            query_args.append(variables_query)

        return query_args, list(set(tabular_variables))

    @prom_async_time(PROM_SEARCH_TIME)
    async def post(self):
        PROM_SEARCH.inc()

        type_ = self.request.headers.get('Content-type', '')
        data = None
        if type_.startswith('application/json'):
            query = self.get_json()
        elif type_.startswith('multipart/form-data'):
            query = self.get_body_argument('query', None)
            if query is None and 'query' in self.request.files:
                query = self.request.files['query'][0].body.decode('utf-8')
            if query is not None:
                query = json.loads(query)
            if 'data' in self.request.files:
                data = self.request.files['data'][0].body
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
                data_path, data_profile, tmp = handle_data_parameter(data)
            except ClientError as e:
                return self.send_error_json(400, e.args[0])
            if tmp:
                os.remove(data_path)

        # parameter: query
        query_args = list()
        tabular_variables = list()
        if query:
            query_args, tabular_variables = self.parse_query(query)

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

    def write_recursive(self, src, dst):
        if os.path.isdir(src):
            for name in os.listdir(src):
                self.write_recursive(os.path.join(src, name),
                                     dst + '/' + name)
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
                    writer.write_recursive(dataset_path, '')
                    writer.close()
                return self.finish()
            finally:
                getter.__exit__(None, None, None)


class DownloadId(CorsHandler, GracefulHandler, BaseDownload):
    @prom_async_time(PROM_DOWNLOAD_TIME)
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


class Download(CorsHandler, GracefulHandler, BaseDownload):
    @prom_async_time(PROM_DOWNLOAD_TIME)
    def post(self):
        PROM_DOWNLOAD.inc()

        type_ = self.request.headers.get('Content-type', '')

        task = None
        data = None
        output_format = 'd3m'
        if type_.startswith('application/json'):
            task = self.get_json()
        elif type_.startswith('multipart/form-data'):
            task = self.get_body_argument('task', None)
            if task is None and 'task' in self.request.files:
                task = (
                    self.request.files['task'][0].body.decode('utf-8'))
            if 'data' in self.request.files:
                data = self.request.files['data'][0].body
            output_format = self.get_body_argument('format', None)
            if output_format is None and 'format' in self.request.files:
                output_format = (
                    self.request.files['format'][0].body.decode('utf-8'))
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
                data_path, data_profile, tmp = handle_data_parameter(data)
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

            if search_results:
                task = search_results[0]

                try:
                    with get_dataset(metadata, task['id'], format='csv') as newdata:
                        # perform augmentation
                        new_path = augment(
                            data_path,
                            newdata,
                            data_profile,
                            task,
                            return_only_datamart_data=True
                        )

                except Exception as e:
                    return self.send_error_json(400, e.args[0])

                # send a zip file
                self.set_header('Content-Type', 'application/zip')
                self.set_header(
                    'Content-Disposition',
                    'attachment; filename="augmentation.zip"')
                writer = RecursiveZipWriter(self.write)
                writer.write_recursive(new_path, '')
                writer.close()
                shutil.rmtree(os.path.abspath(os.path.join(new_path, '..')))

                if tmp:
                    os.remove(data_path)
                return self.finish()
            else:
                if tmp:
                    os.remove(data_path)
                return self.send_error_json(
                    400,
                    "The DataMart dataset referenced by 'task' cannot augment "
                    "'data'.",
                )

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


class Augment(CorsHandler, GracefulHandler):
    @prom_async_time(PROM_AUGMENT_TIME)
    async def post(self):
        PROM_AUGMENT.inc()

        type_ = self.request.headers.get('Content-type', '')
        if not type_.startswith('multipart/form-data'):
            return self.send_error_json(400, "Use multipart/form-data to send "
                                             "the 'data' file and 'task' JSON")

        task = None
        if 'task' in self.request.files:
            task = self.request.files['task'][0].body
        if task is not None:
            task = json.loads(task)

        destination = self.get_body_argument('destination', None)

        data = None
        if 'data' in self.request.files:
            data = self.request.files['data'][0].body

        columns = None
        if 'columns' in self.request.files:
            columns = self.request.files['columns'][0].body
        if columns is not None:
            columns = json.loads(columns)

        # data
        try:
            data_path, data_profile, tmp = handle_data_parameter(data)
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

        try:
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
        except Exception as e:
            return self.send_error_json(400, e.args[0])

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
            writer.write_recursive(new_path, '')
            writer.close()
            shutil.rmtree(os.path.abspath(os.path.join(new_path, '..')))

        if tmp:
            os.remove(data_path)
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


if __name__ == '__main__':
    main()
