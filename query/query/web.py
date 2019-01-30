import aio_pika
import asyncio
from datetime import datetime
from dateutil.parser import parse
import elasticsearch
import hashlib
import logging
import json
import os
import pickle
import prometheus_client
from prometheus_async.aio import time as prom_async_time
import shutil
import tempfile
import tornado.ioloop
from tornado.routing import URLSpec
import tornado.httputil
import tornado.web
from tornado.web import HTTPError, RequestHandler
import zipfile

from datamart_augmentation.search import \
    get_joinable_datasets, get_unionable_datasets
from datamart_augmentation.augmentation import \
    augment, augment_data
from datamart_core.common import log_future, Type
from datamart_core.materialize import get_dataset
from datamart_profiler import process_dataset

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


class CorsHandler(BaseHandler):
    def _cors(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Methods', 'POST')
        self.set_header('Access-Control-Allow-Headers', 'Content-Type')

    def options(self):
        # CORS pre-flight
        self._cors()
        self.set_status(204)
        self.finish()


@tornado.web.stream_request_body
class QueryHandler(CorsHandler):
    """Base class for the query request handler.
    """
    def initialize(self, augmentation_type=None):
        self.data = b''
        self.augmentation_type = augmentation_type

    def prepare(self):
        self.request.connection.set_max_body_size(MAX_STREAMED_SIZE)

    def data_received(self, data):
        self.data += data

    def get_form_data(self, default=None):
        type_ = self.request.headers.get('Content-Type', '')
        if type_.startswith('multipart/form-data'):
            args = dict()
            files = dict()
            tornado.httputil.parse_body_arguments(
                self.request.headers.get('Content-Type', ''),
                self.data,
                args,
                files
            )
            ret = dict()
            for key, value in args.items():
                if key not in ret:
                    ret[key] = value[0]
            for key, value in files.items():
                if key not in ret:
                    ret[key] = value[0]['body']
            return ret
        elif type_.startswith('application/json') and default:
            return {default: self.data}
        else:
            raise HTTPError(400, "Expected multipart/form-data or "
                                 "application/json")

    def get_json(self):
        type_ = self.request.headers.get('Content-Type', '')
        if not type_.startswith('application/json'):
            raise HTTPError(400, "Expected JSON")
        return json.loads(self.data.decode('utf-8'))

    def get_profile_data(self, filepath, metadata=None):
        # hashing data
        sha1 = hashlib.sha1()
        with open(filepath, 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                sha1.update(data)
        hash_ = sha1.hexdigest()

        # checking for cached data
        cached_data = os.path.join('/cache', hash_)
        if os.path.exists(cached_data):
            return pickle.load(open(cached_data, 'rb'))

        # profile data and save
        data_profile = process_dataset(filepath, metadata)
        pickle.dump(data_profile, open(cached_data, 'wb'))
        return data_profile

    def handle_data_parameter(self, data):
        """
        Handles the 'data' parameter.

        :param data: the input parameter
        :return: (data_path, data_profile, tmp)
          data_path: path to the input data
          data_profile: the profiling (metadata) of the data
          tmp: True if data_path points to a temporary file
        """

        if not isinstance(data, (str, bytes)):
            self.send_error(
                status_code=400,
                reason='The parameter "data" is in the wrong format.'
            )
            return

        tmp = False
        if not os.path.exists(data):
            # data represents the entire file
            logger.warning("Data is not a path!")

            tmp = True
            temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
            temp_file.write(data)
            temp_file.close()

            data_path = temp_file.name
            data_profile = self.get_profile_data(data_path)

        else:
            # data represents a file path
            logger.warning("Data is a path!")
            if os.path.isdir(data):
                # path to a D3M dataset
                data_file = os.path.join(data, 'tables', 'learningData.csv')
                if not os.path.exists(data_file):
                    self.send_error(
                        status_code=400,
                        reason='%s does not exist.' % data_file
                    )
                    return
                else:
                    data_path = data_file
                    data_profile = self.get_profile_data(data_file)
            else:
                # path to a CSV file
                data_path = data
                data_profile = self.get_profile_data(data)

        return data_path, data_profile, tmp


class Query(QueryHandler):

    def parse_query_variables(self, data, search_columns=[], required=False):
        output = list()

        if not data:
            return output

        for variable in data:
            if 'type' not in variable:
                self.send_error(
                    status_code=400,
                    reason='variable is missing property "type"'
                )
                return
            variable_query = list()

            # temporal
            # TODO: ignoring 'granularity' for now
            if 'temporal_entity' in variable['type']:
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

            # spatial
            # TODO: ignoring 'named_entities' for now
            elif 'geospatial_entity' in variable['type']:
                if 'bounding_box' in variable:
                    if ('latitude1' not in variable['bounding_box'] or
                            'latitude2' not in variable['bounding_box'] or
                            'longitude1' not in variable['bounding_box'] or
                            'longitude2' not in variable['bounding_box']):
                        continue
                    longitude1 = min(float(variable['bounding_box']['longitude1']),
                                     float(variable['bounding_box']['longitude2']))
                    longitude2 = max(float(variable['bounding_box']['longitude1']),
                                     float(variable['bounding_box']['longitude2']))
                    latitude1 = max(float(variable['bounding_box']['latitude1']),
                                    float(variable['bounding_box']['latitude2']))
                    latitude2 = min(float(variable['bounding_box']['latitude1']),
                                    float(variable['bounding_box']['latitude2']))
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
                if 'circle' in variable:
                    if ('latitude' not in variable['circle'] or
                            'longitude' not in variable['circle'] or
                            'radius' not in variable['circle']):
                        continue
                    radius = float(variable['circle']['radius'])
                    longitude1 = float(variable['circle']['longitude']) - radius
                    longitude2 = float(variable['circle']['longitude']) + radius
                    latitude1 = float(variable['circle']['latitude']) + radius
                    latitude2 = float(variable['circle']['latitude']) - radius
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

            # dataframe columns
            # TODO: ignoring 'index', 'relationship'
            elif 'dataframe_columns' in variable['type']:
                if 'names' in variable:
                    for name in variable['names']:
                        search_columns.append(name.strip().lower())

            # generic entity
            # TODO: ignoring 'variable_metadata',
            #  'variable_description', 'named_entities', and
            #  'column_values' for now
            elif 'generic_entity' in variable['type']:
                if 'about' in variable:
                    about_query = list()
                    for name in variable['about']:
                        about_query.append({
                            'nested': {
                                'path': 'columns',
                                'query': {
                                    'match': {'columns.name': name},
                                },
                            },
                        })
                        about_query.append({
                            'nested': {
                                'path': 'columns',
                                'query': {
                                    'wildcard': {'columns.name': '*%s*' % name.lower()},
                                },
                            },
                        })
                    variable_query.append({
                        'bool': {
                            'should': about_query,
                            'minimum_should_match': 1
                        }
                    })
                if 'variable_name' in variable:
                    name_query = list()
                    for name in variable['variable_name']:
                        name_query.append({
                            'nested': {
                                'path': 'columns',
                                'query': {
                                    'match': {'columns.name': name},
                                },
                            },
                        })
                        name_query.append({
                            'nested': {
                                'path': 'columns',
                                'query': {
                                    'wildcard': {'columns.name': '*%s*' % name.lower()},
                                },
                            },
                        })
                    variable_query.append({
                        'bool': {
                            'should': name_query,
                            'minimum_should_match': 1
                        }
                    })
                if 'variable_syntactic_type' in variable:
                    structural_query = list()
                    for type_ in variable['variable_syntactic_type']:
                        structural_query.append({
                            'nested': {
                                'path': 'columns',
                                'query': {
                                    'match': {'columns.structural_type': type_},
                                },
                            },
                        })
                    variable_query.append({
                        'bool': {
                            'should': structural_query,
                            'minimum_should_match': 1
                        }
                    })
                if 'variable_semantic_type' in variable:
                    semantic_query = list()
                    for type_ in variable['variable_semantic_type']:
                        semantic_query.append({
                            'nested': {
                                'path': 'columns',
                                'query': {
                                    'match': {'columns.semantic_types': type_},
                                },
                            },
                        })
                    variable_query.append({
                        'bool': {
                            'should': semantic_query,
                            'minimum_should_match': 1
                        }
                    })

            if variable_query:
                output.append({
                    'bool': {
                        'must': variable_query,
                    }
                })

        if output:
            if required:
                return {
                    'bool': {
                        'must': output,
                    }
                }
            return {
                'bool': {
                    'should': output,
                    'minimum_should_match': 1,
                }
            }
        return {}

    def parse_query(self, query_json):
        query_args = list()

        # dataset
        # TODO: ignoring the following properties for now:
        #   creator, date_published, date_created
        dataset_query = list()
        if 'dataset' in query_json:
            # about
            if 'about' in query_json['dataset']:
                if not isinstance(query_json['dataset']['about'], str):
                    self.send_error(
                        status_code=400,
                        reason='dataset.about must be a string'
                    )
                    return
                about_query = list()
                for name in query_json['dataset']['about'].split():
                    about_query.append({
                        'match': {'description': name}
                    })
                    about_query.append({
                        'wildcard': {'description': '*%s*' % name.lower()}
                    })
                    about_query.append({
                        'match': {'name': name}
                    })
                    about_query.append({
                        'wildcard': {'name': '*%s*' % name.lower()}
                    })
                    about_query.append({
                        'nested': {
                            'path': 'columns',
                            'query': {
                                'match': {'columns.name': name},
                            },
                        },
                    })
                    about_query.append({
                        'nested': {
                            'path': 'columns',
                            'query': {
                                'wildcard': {'columns.name': '*%s*' % name.lower()},
                            },
                        },
                    })
                dataset_query.append({
                    'bool': {
                        'should': about_query,
                        'minimum_should_match': 1
                    }
                })

            # keywords
            if 'keywords' in query_json['dataset']:
                if not isinstance(query_json['dataset']['keywords'], list):
                    self.send_error(
                        status_code=400,
                        reason='dataset.keywords must be an array'
                    )
                    return
                keywords_query = list()
                for name in query_json['dataset']['keywords']:
                    keywords_query.append({
                        'match': {'description': name}
                    })
                    keywords_query.append({
                        'wildcard': {'description': '*%s*' % name.lower()}
                    })
                    keywords_query.append({
                        'match': {'name': name}
                    })
                    keywords_query.append({
                        'wildcard': {'name': '*%s*' % name.lower()}
                    })
                    keywords_query.append({
                        'nested': {
                            'path': 'columns',
                            'query': {
                                'match': {'columns.name': name},
                            },
                        },
                    })
                    keywords_query.append({
                        'nested': {
                            'path': 'columns',
                            'query': {
                                'wildcard': {'columns.name': '*%s*' % name.lower()},
                            },
                        },
                    })
                dataset_query.append({
                    'bool': {
                        'should': keywords_query,
                        'minimum_should_match': 1
                    }
                })

            # name
            if 'name' in query_json['dataset']:
                if not isinstance(query_json['dataset']['name'], list):
                    self.send_error(
                        status_code=400,
                        reason='dataset.name must be an array'
                    )
                    return
                name_query = list()
                for name in query_json['dataset']['name']:
                    name_query.append({
                        'match': {'name': name}
                    })
                    name_query.append({
                        'wildcard': {'name': '*%s*' % name.lower()}
                    })
                dataset_query.append({
                    'bool': {
                        'should': name_query,
                        'minimum_should_match': 1
                    }
                })

            # description
            if 'description' in query_json['dataset']:
                if not isinstance(query_json['dataset']['description'], list):
                    self.send_error(
                        status_code=400,
                        reason='dataset.description must be an array'
                    )
                    return
                desc_query = list()
                for name in query_json['dataset']['description']:
                    desc_query.append({
                        'match': {'description': name}
                    })
                    desc_query.append({
                        'wildcard': {'description': '*%s*' % name.lower()}
                    })
                dataset_query.append({
                    'bool': {
                        'should': desc_query,
                        'minimum_should_match': 1
                    }
                })

            # publisher
            if 'publisher' in query_json['dataset']:
                if not isinstance(query_json['dataset']['publisher'], list):
                    self.send_error(
                        status_code=400,
                        reason='dataset.publisher must be an array'
                    )
                    return
                pub_query = list()
                for pub in query_json['dataset']['publisher']:
                    pub_query.append({
                        'wildcard': {'materialize.identifier': '*%s*' % pub.lower()}
                    })
                dataset_query.append({
                    'bool': {
                        'should': pub_query,
                        'minimum_should_match': 1
                    }
                })

            # url
            if 'url' in query_json['dataset']:
                if not isinstance(query_json['dataset']['url'], list):
                    self.send_error(
                        status_code=400,
                        reason='dataset.url must be an array'
                    )
                    return
                url_query = list()
                for url in query_json['dataset']['url']:
                    url_query.append({
                        'wildcard': {'materialize.direct_url': '%s*' % url.lower()}
                    })
                dataset_query.append({
                    'bool': {
                        'should': url_query,
                        'minimum_should_match': 1
                    }
                })

        if dataset_query:
            query_args.append(dataset_query)

        # search columns
        search_columns = {'required': [],
                          'desired': []}

        # required variables
        required_query = dict()
        if 'required_variables' in query_json:
            required_query = self.parse_query_variables(
                query_json['required_variables'],
                search_columns=search_columns['required'],
                required=True
            )

        if required_query:
            query_args.append(required_query)

        # desired variables
        desired_query = dict()
        if 'desired_variables' in query_json:
            desired_query = self.parse_query_variables(
                query_json['desired_variables'],
                search_columns=search_columns['desired'],
                required=False
            )

        if desired_query:
            query_args.append(desired_query)

        search_columns['required'] = list(set(search_columns['required']))
        search_columns['desired'] = list(set(search_columns['desired']))

        return query_args, search_columns

    @prom_async_time(PROM_SEARCH_TIME)
    async def post(self):
        PROM_SEARCH.inc()
        self._cors()

        args = self.get_form_data('query')

        # Params are 'query' and 'data'
        query = data = None
        if 'query' in args:
            query = json.loads(args['query'].decode('utf-8'))
        if 'data' in args:
            data = args['data'].decode('utf-8')

        # parameter: data
        data_profile = dict()
        if data:
            data_path, data_profile, tmp = self.handle_data_parameter(data)

        # parameter: query
        query_args = list()
        search_columns = dict()
        if query:
            query_args, search_columns = self.parse_query(query)

        # At least one of them must be provided
        if not query_args and not data_profile:
            self.send_error(
                status_code=400,
                reason='At least one of the input parameters must be provided.'
            )
            return

        if not data_profile:
            # logger.info("Query: %r", query_args)
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
                materialize = meta.get('materialize', {})
                if 'description' in meta and len(meta['description']) > 100:
                    meta['description'] = meta['description'][:100] + "..."
                results.append(dict(
                    id=h['_id'],
                    score=h['_score'],
                    discoverer=materialize['identifier'],
                    metadata=meta,
                    join_columns=[],
                    union_columns=[],
                ))
            self.send_json({'results': results})
        else:
            join_results = get_joinable_datasets(
                es=self.application.elasticsearch,
                data_profile=data_profile,
                query_args=query_args,
                search_columns=search_columns
            )['results']
            union_results = get_unionable_datasets(
                es=self.application.elasticsearch,
                data_profile=data_profile,
                query_args=query_args,
                fuzzy=True,
                search_columns=search_columns
            )['results']

            results = []
            for r in join_results:
                if r['score'] < SCORE_THRESHOLD:
                    continue
                results.append(dict(
                    id=r['id'],
                    score=r['score'],
                    metadata=r['metadata'],
                    join_columns=r['columns'],
                ))
            for r in union_results:
                if r['score'] < SCORE_THRESHOLD:
                    continue
                results.append(dict(
                    id=r['id'],
                    score=r['score'],
                    metadata=r['metadata'],
                    union_columns=r['columns'],
                ))

            self.send_json({
                'results':
                    sorted(
                        results,
                        key=lambda item: item['score'],
                        reverse=True
                    )
            })


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


class Download(CorsHandler):
    TIMEOUT = 300

    @prom_async_time(PROM_DOWNLOAD_TIME)
    async def get(self, dataset_id):
        PROM_DOWNLOAD.inc()

        output_format = self.get_query_argument('format', 'csv')

        # Get materialization data from Elasticsearch
        try:
            metadata = self.application.elasticsearch.get(
                'datamart', '_doc', id=dataset_id
            )['_source']
        except elasticsearch.NotFoundError:
            raise HTTPError(404)
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
                self.set_status(500)
                self.send_json(dict(error="Materializer reports failure"))
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
                self.finish()
            finally:
                getter.__exit__(None, None, None)


class Metadata(CorsHandler):
    @PROM_METADATA_TIME.time()
    def get(self, dataset_id):
        PROM_METADATA.inc()

        es = self.application.elasticsearch
        try:
            metadata = es.get('datamart', '_doc', id=dataset_id)['_source']
        except elasticsearch.NotFoundError:
            raise HTTPError(404)

        self.send_json(metadata)


class Augment(QueryHandler):
    @prom_async_time(PROM_AUGMENT_TIME)
    async def post(self):
        PROM_AUGMENT.inc()
        self._cors()

        args = self.get_form_data('task')

        # Params are 'task', 'data', and 'destination'
        task = data = destination = None
        if 'task' in args:
            task = json.loads(args['task'].decode('utf-8'))
        if 'data' in args:
            data = args['data'].decode('utf-8')
        if 'destination' in args:
            destination = args['destination'].decode('utf-8')

        # both parameters must be provided
        if not task or not data:
            self.send_error(
                status_code=400,
                reason='Both "task" and "data" must be provided.'
            )
            return

        # data
        data_path, data_profile, tmp = self.handle_data_parameter(data)

        # materialize augmentation data
        metadata = task['metadata']
        with get_dataset(metadata, task['id'], format='csv') as newdata:
            # perform augmentation
            new_path = augment(
                data_path,
                newdata,
                data_profile,
                task,
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
            writer.write_recursive(new_path, '')
            writer.close()
            shutil.rmtree(os.path.abspath(os.path.join(new_path, '..')))
        self.finish()

        if tmp:
            os.remove(data_path)


class JoinUnion(QueryHandler):
    @prom_async_time(PROM_AUGMENT_TIME)
    async def post(self):
        PROM_AUGMENT.inc()
        self._cors()

        args = self.get_form_data('columns')

        left_data = right_data = columns = destination = None
        if 'left_data' in args:
            left_data = args['left_data'].decode('utf-8')
        if 'right_data' in args:
            right_data = args['right_data'].decode('utf-8')
        if 'columns' in args:
            columns = json.loads(args['columns'].decode('utf-8'))
        if 'destination' in args:
            destination = args['destination'].decode('utf-8')

        # both parameters must be provided
        if not left_data or not right_data or not columns:
            self.send_error(
                status_code=400,
                reason='"left_data", "right_data", and "columns" must be provided.'
            )
            return

        if 'left_columns' not in columns or 'right_columns' not in columns:
            self.send_error(
                status_code=400,
                reason='Incomplete "columns" information.'
            )
            return

        # data
        left_data_path, left_data_profile, left_tmp = \
            self.handle_data_parameter(left_data)
        right_data_path, right_data_profile, right_tmp = \
            self.handle_data_parameter(right_data)

        # augment
        try:
            new_path = augment_data(
                left_data_path,
                right_data_path,
                columns['left_columns'],
                columns['right_columns'],
                left_data_profile,
                right_data_profile,
                how=self.augmentation_type,
                destination=destination
            )
        except Exception as e:
            self.send_error(
                status_code=400,
                reason=str(e)
            )
            return

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
        self.finish()

        if left_tmp:
            os.remove(left_data_path)
        if right_tmp:
            os.remove(right_data_path)


class Health(QueryHandler):
    def get(self):
        self.finish('ok')


class Application(tornado.web.Application):
    def __init__(self, *args, es, **kwargs):
        super(Application, self).__init__(*args, **kwargs)

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


def make_app(debug=False):
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )

    return Application(
        [
            URLSpec('/search', Query, name='search'),
            URLSpec('/download/([^/]+)', Download, name='download'),
            URLSpec('/metadata/([^/]+)', Metadata, name='metadata'),
            URLSpec('/augment', Augment, name='augment'),
            URLSpec('/join', JoinUnion,
                    dict(augmentation_type='join'), name='join'),
            URLSpec('/union', JoinUnion,
                    dict(augmentation_type='union'), name='union'),
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
    app.listen(8002, xheaders=True)
    loop = tornado.ioloop.IOLoop.current()
    loop.start()


if __name__ == '__main__':
    main()
