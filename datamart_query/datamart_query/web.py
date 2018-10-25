import elasticsearch
import logging
import json
import os
import tornado.ioloop
from tornado.routing import URLSpec
import tornado.web
from tornado.web import HTTPError, RequestHandler


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


class Query(BaseHandler):
    def _cors(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Methods', 'POST')
        self.set_header('Access-Control-Allow-Headers', 'Content-Type')

    def options(self):
        # CORS pre-flight
        self._cors()
        self.set_status(204)
        self.finish()

    def post(self):
        self._cors()

        obj = self.get_json()

        # Search by keyword
        keywords = obj.get('keywords', [])
        hits = self.application.elasticsearch.search(
            index='datamart',
            body={
                'query': {
                    'terms': {
                        'description': keywords,
                    },
                },
            },
        )['hits']['hits']

        result = []
        for h in hits:
            meta = h.pop('_source')
            materialize = meta.pop('materialize', {})
            if 'description' in meta and len(meta['description']) > 100:
                meta['description'] = meta['description'][:100] + "..."
            result.append(dict(
                id=h['_id'],
                score=h['_score'],
                discoverer=materialize['identifier'],
                meta=meta,
            ))
        self.send_json(result)


class Application(tornado.web.Application):
    def __init__(self, *args, es, **kwargs):
        super(Application, self).__init__(*args, **kwargs)

        self.elasticsearch = es


def make_app(debug=False):
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )

    return Application(
        [
            URLSpec('/query', Query, name='query'),
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
