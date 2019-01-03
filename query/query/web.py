import aio_pika
import asyncio
from datetime import datetime
from dateutil.parser import parse
import elasticsearch
import logging
import json
import os
import shutil
import tornado.ioloop
from tornado.routing import URLSpec
import tornado.web
from tornado.web import HTTPError, RequestHandler
import uuid

from datamart_core.augment import get_joinable_datasets, get_unionable_datasets
from datamart_core.common import log_future, json2msg, Type
from datamart_core.materialize import get_dataset
from datamart_profiler import process_dataset

logger = logging.getLogger(__name__)


MAX_CONCURRENT = 2


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


class Query(CorsHandler):

    def search_d3m_dataset_id(self, metadata):
        dataset_id = ''
        if isinstance(metadata, dict):
            if 'about' not in metadata:
                return dataset_id
            id_ = 'datamart.d3m.' + \
                  metadata['about']['datasetID'].replace('_dataset', '')
            hits = self.application.elasticsearch.search(
                index='datamart',
                body={
                    'query': {
                        'match': {
                            '_id': id_,
                        },
                    },
                }
            )['hits']['hits']
            if not hits:
                logger.warning("Data not in DataMart!")
            else:
                dataset_id = id_
        return dataset_id

    def parse_query_variables(self, data, required=False):
        output = list()

        if not data:
            return output

        for variable in data:
            if 'type' not in variable:
                continue
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
            # TODO: ignoring 'circle' and 'named_entities' for now
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

            # dataframe columns
            # TODO: ignoring this for now -- does not make much sense
            elif 'dataframe_columns' in variable['type']:
                pass

            # generic entity
            # TODO: ignoring 'about', 'variable_metadata',
            #  'variable_description', 'named_entities', and
            #  'column_values' for now
            elif 'generic_entity' in variable['type']:
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
                    variable_query.append({
                        'bool': {
                            'should': name_query
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
                            'should': structural_query
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
                            'should': semantic_query
                        }
                    })

            output.append({
                'bool': {
                    'must': variable_query,
                }
            })

        if required:
            return {
                'bool': {
                    'must': output,
                }
            }
        return {
            'bool': {
                'must': {
                    'match_all': {}
                },
                'should': output,
            }
        }

    def parse_query(self, query_json):
        query_args = list()

        # dataset
        # TODO: ignoring the following properties for now:
        #   keywords, creator, date_published, date_created, publisher, url
        dataset_query = list()
        if 'dataset' in query_json:
            if 'about' in query_json['dataset']:
                about_query = list()
                about_query.append({
                    'match': {
                        'description': {
                            'query': query_json['dataset']['about'],
                            'operator': 'or'
                        }
                    }
                })
                about_query.append({
                    'match': {
                        'name': {
                            'query': query_json['dataset']['about'],
                            'operator': 'or'
                        }
                    }
                })
                about_query.append({
                    'nested': {
                        'path': 'columns',
                        'query': {
                            'match': {
                                'columns.name': {
                                    'query': query_json['dataset']['about'],
                                    'operator': 'or'
                                }
                            }
                        }
                    }
                })
                dataset_query.append({
                    'bool': {
                        'should': about_query,
                        'minimum_should_match': 1
                    }
                })

            # name
            if 'name' in query_json['dataset']:
                name_query = list()
                for name in query_json['dataset']['name']:
                    name_query.append({
                        'match': {'name': name}
                    })
                dataset_query.append({
                    'bool': {
                        'should': name_query,
                        'minimum_should_match': 1
                    }
                })

            # description
            if 'description' in query_json['dataset']:
                desc_query = list()
                for name in query_json['dataset']['description']:
                    desc_query.append({
                        'match': {'description': name}
                    })
                dataset_query.append({
                    'bool': {
                        'should': desc_query,
                        'minimum_should_match': 1
                    }
                })

        if dataset_query:
            query_args.append(dataset_query)

        # required variables
        required_query = dict()
        if 'required_variables' in query_json:
            required_query = self.parse_query_variables(
                query_json['required_variables'],
                required=True
            )

        if required_query:
            query_args.append(required_query)

        # desired variables
        desired_query = dict()
        if 'desired_variables' in query_json:
            desired_query = self.parse_query_variables(
                query_json['desired_variables'],
                required=False
            )

        if desired_query:
            query_args.append(desired_query)

        return query_args

    async def post(self):
        self._cors()

        obj = self.get_json()
        logger.info("Query: %r", obj)

        # Params are 'query' and 'data'
        query = data = None
        if 'query' in obj:
            query = obj['query']
        if 'data' in obj:
            data = obj['data']

        # parameter: data
        dataset_id = ''
        if data:
            if isinstance(data, dict):
                # data is a D3M dataset
                dataset_id = self.search_d3m_dataset_id(data)

            elif isinstance(data, str):
                # data represents a file path
                if not os.path.exists(data):
                    logger.warning("Data does not exist!")
                else:
                    if 'datasetDoc.json' in data:
                        # path to a datasetDoc.json file
                        # extract id and check if data is in DataMart index
                        with open(data) as f:
                            dataset_doc = json.load(f)
                        dataset_id = self.search_d3m_dataset_id(dataset_doc)
                    else:
                        # assume path to a CSV file
                        # upload data first
                        dataset_id = 'datamart.upload.%s' % uuid.uuid4().hex

                        dataset_dir = os.path.join('/datasets', dataset_id)
                        os.mkdir(dataset_dir)
                        new_data_path = os.path.join(dataset_dir, 'main.csv')
                        try:
                            shutil.copy(data, new_data_path)
                        except Exception:
                            shutil.rmtree(dataset_dir)
                            raise

                        # profile data
                        metadata = dict(
                            name=os.path.splitext(os.path.basename(data))[0],
                            materialize=dict(identifier='datamart.upload',
                                             original_path=data,
                                             new_path=new_data_path)
                        )
                        data_profile = process_dataset(
                            new_data_path,
                            metadata=metadata
                        )

                        # insert results in Elasticsearch
                        body = dict(data_profile,
                                    date=datetime.utcnow().isoformat() + 'Z')
                        self.application.elasticsearch.index(
                            'datamart',
                            '_doc',
                            body,
                            id=dataset_id,
                        )

                        # publish to RabbitMQ
                        await self.application.work_tickets.acquire()
                        await self.application.datasets_exchange.publish(
                            json2msg(dict(body, id=dataset_id)),
                            dataset_id,
                        )
                        self.application.work_tickets.release()

        # parameter: query
        # TODO: figure out scoring
        query_args = list()
        if query:
            query_args = self.parse_query(query)

        # At least one of them must be provided
        if not query_args and not dataset_id:
            self.send_error(status_code=400)
            return

        if not dataset_id:
            logger.info("Query: %r", query_args)
            hits = self.application.elasticsearch.search(
                index='datamart',
                body={
                    'query': {
                        'bool': {
                            'must': query_args,
                        },
                    },
                },
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
            query_param = None
            if query_args:
                logger.info("Query: %r", query_args)
                query_param = query_args

            join_results = get_joinable_datasets(
                self.application.elasticsearch,
                dataset_id,
                query_param
            )['results']
            union_results = get_unionable_datasets(
                self.application.elasticsearch,
                dataset_id,
                query_param
            )['results']

            results = {}
            for r in join_results:
                results[r['id']] = dict(
                    id=r['id'],
                    score=r['score'],
                    discoverer=r['discoverer'],
                    metadata=r['metadata'],
                    join_columns=[],
                    union_columns=[],
                )
                for pair in r['columns']:
                    results[r['id']]['join_columns'].append(pair)
            for r in union_results:
                if r['id'] not in results:
                    results[r['id']] = dict(
                        id=r['id'],
                        score=r['score'],
                        discoverer=r['discoverer'],
                        metadata=r['metadata'],
                        join_columns=[],
                        union_columns=[],
                    )
                else:
                    results[r['id']]['score'] = max(results[r['id']]['score'], r['score'])
                for pair in r['columns']:
                    results[r['id']]['union_columns'].append(pair)

            self.send_json({
                'results':
                    sorted(
                        results.values(),
                        key=lambda item: item['score'],
                        reverse=True
                    )
                }
            )


class Download(CorsHandler):
    TIMEOUT = 300

    async def get(self, dataset_id):
        self._cors()

        # Get materialization data from Elasticsearch
        es = self.application.elasticsearch
        try:
            metadata = es.get('datamart', '_doc', id=dataset_id)['_source']
        except elasticsearch.NotFoundError:
            raise HTTPError(404)
        materialize = metadata.pop('materialize', {})

        # If there's a direct download URL
        if 'direct_url' in materialize:
            # Redirect the client to it
            self.redirect(materialize['direct_url'])
        else:
            getter = get_dataset(materialize, dataset_id)
            try:
                dataset_dir = getter.__enter__()
            except Exception:
                self.set_status(500)
                self.send_json(dict(error="Materializer reports failure"))
                raise
            try:
                self.set_header('Content-Type', 'application/octet-stream')
                self.set_header('X-Content-Type-Options', 'nosniff')
                self.set_header('Content-Disposition',
                                'attachment; filename="%s.csv"' % dataset_id)
                with open(os.path.join(dataset_dir, 'main.csv'), 'rb') as fp:
                    buf = fp.read(4096)
                    while buf:
                        self.write(buf)
                        if len(buf) != 4096:
                            break
                        buf = fp.read(4096)
                self.finish()
            finally:
                getter.__exit__()


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

        # Setup the datasets exchange
        self.datasets_exchange = await self.channel.declare_exchange(
            'datasets',
            aio_pika.ExchangeType.TOPIC)


def make_app(debug=False):
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )

    return Application(
        [
            URLSpec('/search', Query, name='search'),
            URLSpec('/download/([^/]+)', Download, name='download'),
        ],
        debug=debug,
        es=es,
    )


def main():
    logging.root.handlers.clear()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")

    app = make_app(debug=True)
    app.listen(8002)
    loop = tornado.ioloop.IOLoop.current()
    loop.start()


if __name__ == '__main__':
    main()
