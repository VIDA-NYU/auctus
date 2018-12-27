import aio_pika
import asyncio
from datetime import datetime
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

from datamart_core.common import log_future, json2msg
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

    async def post(self):
        self._cors()

        obj = self.get_json()

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
                        # profile data first
                        metadata = dict(
                            filename=data,
                            name=os.path.splitext(os.path.basename(data))[0],
                            materialize=dict(identifier='datamart.upload')
                        )
                        dataset_id = 'datamart.upload.%s' % uuid.uuid4().hex

                        # profile data
                        dataset_dir = os.path.join('/datasets', dataset_id)
                        os.mkdir(dataset_dir)
                        try:
                            shutil.copy(data, os.path.join(dataset_dir, 'main.csv'))
                        except Exception:
                            shutil.rmtree(dataset_dir)
                            raise
                        data_profile = process_dataset(data, metadata=metadata)

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
        query_args = []
        if query:

            # Search by keyword
            if 'keywords' in query:
                query_args.append({
                    'match': {
                        'description': {
                            'query': query['keywords'],
                            'operator': 'and',
                        },
                    },
                })

            # Search for columns with names
            if 'column_names' in query:
                for name in query['column_names']:
                    query_args.append({
                        'nested': {
                            'path': 'columns',
                            'query': {
                                'match': {'columns.name': name},
                            },
                        },
                    })

            # Search for columns with structural types
            if 'structural_types' in query:
                for type_ in query['structural_types']:
                    query_args.append({
                        'nested': {
                            'path': 'columns',
                            'query': {
                                'match': {'columns.structural_type': type_},
                            },
                        },
                    })

            # Search for columns with semantic types
            if 'semantic_types' in query:
                for type_ in query['semantic_types']:
                    query_args.append({
                        'nested': {
                            'path': 'columns',
                            'query': {
                                'term': {'columns.semantic_types': type_},
                            },
                        },
                    })

        # At least one of them must be provided
        if not query_args and not dataset_id:
            self.send_error(status_code=400)
            return

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
            ))
        self.send_json({'results': results})


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
