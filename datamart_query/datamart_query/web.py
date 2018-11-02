import aio_pika
import asyncio
import elasticsearch
import logging
import json
import os
import tornado.ioloop
from tornado.routing import URLSpec
import tornado.web
from tornado.web import HTTPError, RequestHandler

from datamart_core.common import json2msg, msg2json, log_future


logger = logging.getLogger(__name__)


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
    def post(self):
        self._cors()

        obj = self.get_json()
        query = []

        # Search by keyword
        if 'keywords' in obj:
            query.append({
                'match': {
                    'description': {
                        'query': obj['keywords'],
                        'operator': 'and',
                    },
                },
            })

        # Search for columns with structural types
        if 'structural_types' in obj:
            for type_ in obj['structural_types']:
                query.append({
                    'nested': {
                        'path': 'columns',
                        'query': {
                            'match': {'columns.structural_type': type_},
                        },
                    },
                })

        # Search for columns with semantic types
        if 'semantic_types' in obj:
            for type_ in obj['semantic_types']:
                query.append({
                    'nested': {
                        'path': 'columns',
                        'query': {
                            'term': {'columns.semantic_types': type_},
                        },
                    },
                })

        if not query:
            self.send_json({'results': []})

        logger.info("Query: %r", query)
        hits = self.application.elasticsearch.search(
            index='datamart',
            body={
                'query': {
                    'bool': {
                        'must': query,
                    },
                },
            },
        )['hits']['hits']

        results = []
        for h in hits:
            meta = h.pop('_source')
            materialize = meta.pop('materialize', {})
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
        metadata = es.get('datamart', '_doc', id=dataset_id)['_source']
        materialize = metadata.pop('materialize', {})

        # If there's a direct download URL
        if 'direct_url' in materialize:
            # Redirect the client to it
            self.redirect(materialize['direct_url'])
        elif 'identifier' in materialize:
            # Create queue for the reply
            reply_queue = await self.application.channel.declare_queue(
                exclusive=True)

            # Send materialization request
            await self.application.channel.default_exchange.publish(
                json2msg(dict(materialize=materialize),
                         reply_to=reply_queue.name),
                'materializes.%s' % materialize['identifier'],
            )

            # Get reply
            # reply = msg2json(await reply_queue.get(timeout=self.TIMEOUT))
            async for reply in reply_queue.iterator():
                reply = msg2json(reply)
                logger.info("Got reply %r", reply)
                if not reply.get('success'):
                    self.set_status(500)
                    self.send_json(dict(error="Materializer reports failure"))
                else:
                    self.set_header('Content-Type', 'application/octet-stream')
                    self.set_header('X-Content-Type-Options', 'nosniff')
                    self.set_header(
                        'Content-Disposition',
                        'attachment; filename="%s.csv"' % dataset_id)
                    with open(os.path.join(reply['storage']['path'], 'main.csv'),
                              'rb') as fp:
                        buf = fp.read(4096)
                        while buf:
                            self.write(buf)
                            if len(buf) != 4096:
                                break
                            buf = fp.read(4096)
                    self.finish()
                break
        else:
            self.set_status(500)
            self.send_json(dict(error="No materializer recorded for dataset"))


class Application(tornado.web.Application):
    def __init__(self, *args, es, **kwargs):
        super(Application, self).__init__(*args, **kwargs)

        self.elasticsearch = es

        log_future(asyncio.get_event_loop().create_task(self._amqp()), logger)

    async def _amqp(self):
        connection = await aio_pika.connect_robust(
            host=os.environ['AMQP_HOST'],
            login=os.environ['AMQP_USER'],
            password=os.environ['AMQP_PASSWORD'],
        )
        self.channel = await connection.channel()
        await self.channel.set_qos(prefetch_count=1)


def make_app(debug=False):
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )

    return Application(
        [
            URLSpec('/query', Query, name='query'),
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
