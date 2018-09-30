import asyncio
import elasticsearch
import logging
import jinja2
import json
import os
import pkg_resources
import tornado.ioloop
from tornado.routing import URLSpec
import tornado.web
from tornado.web import HTTPError, RequestHandler

from .coordinator import Coordinator


logger = logging.getLogger(__name__)


class BaseHandler(RequestHandler):
    """Base class for all request handlers.
    """
    template_env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(
            [pkg_resources.resource_filename('datamart_coordinator',
                                             'templates')]
        ),
        autoescape=jinja2.select_autoescape(['html'])
    )

    @jinja2.contextfunction
    def _tpl_static_url(context, path):
        v = not context['handler'].application.settings.get('debug', False)
        return context['handler'].static_url(path, include_version=v)
    template_env.globals['static_url'] = _tpl_static_url

    @jinja2.contextfunction
    def _tpl_reverse_url(context, path, *args):
        return context['handler'].reverse_url(path, *args)
    template_env.globals['reverse_url'] = _tpl_reverse_url

    @jinja2.contextfunction
    def _tpl_xsrf_form_html(context):
        return jinja2.Markup(context['handler'].xsrf_form_html())
    template_env.globals['xsrf_form_html'] = _tpl_xsrf_form_html

    def render_string(self, template_name, **kwargs):
        template = self.template_env.get_template(template_name)
        return template.render(
            handler=self,
            current_user=self.current_user,
            **kwargs)

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

    @property
    def coordinator(self):
        return self.application.coordinator


class Index(BaseHandler):
    def get(self):
        self.render('index.html')


class Status(BaseHandler):
    def get(self):
        self.send_json({
            'discoverers': [[k, len(v)]
                            for k, v in self.coordinator.discoverers.items()],
            'ingesters': [[k, len(v)]
                          for k, v in self.coordinator.ingesters.items()],
            'recent_discoveries': self.coordinator.recent_discoveries,
            'storage': self.coordinator.storage,
        })


class Dataset(BaseHandler):
    def get(self, dataset_id):
        dataset_path = self.coordinator.storage_r.get(dataset_id)
        es = self.application.elasticsearch
        # Get dataset meta
        dataset_meta = es.get('datamart', '_doc', id=dataset_id)['_source']
        # Get ingested records for dataset
        ingest_metas = es.search(
            index='datamart',
            body={
                'query': {'parent_id': {'type': 'metadata', 'id': dataset_id}}
            },
        )['hits']['hits']
        ingest_metas = [e['_source'] for e in ingest_metas]
        self.render('dataset.html',
                    dataset_id=dataset_id, dataset_path=dataset_path,
                    dataset_meta=dataset_meta, ingest_metas=ingest_metas)


class PollDiscovery(BaseHandler):
    async def get(self):
        identifier = self.get_query_argument('id')
        logger.info("Discoverer %r polling...", identifier)
        self.coordinator.add_discoverer(identifier, self)
        try:
            self._close_event = asyncio.Event()
            await self._close_event.wait()
            # TODO: Send 'query' event to discoverer
            # TODO: Send 'materialize' event to discoverer
        finally:
            self.coordinator.remove_discoverer(identifier, self)

    def on_connection_close(self):
        logger.info("Discoverer connection closed")
        self._close_event.set()


class DatasetDiscovered(BaseHandler):
    def post(self):
        identifier = self.get_query_argument('id')
        obj = self.get_json()
        self.coordinator.discovered(identifier, obj['id'], obj['meta'])
        self.send_json({})


class DatasetDownloaded(BaseHandler):
    def post(self):
        identifier = self.get_query_argument('id')
        obj = self.get_json()
        self.coordinator.downloaded(identifier,
                                    obj['dataset_id'], obj['storage_path'])
        self.send_json({})


class AllocateDataset(BaseHandler):
    def get(self):
        identifier = self.get_query_argument('id')
        path = self.coordinator.allocate_shared(identifier)
        self.send_json({'path': path, 'max_size_bytes': 1 << 30})


class PollIngestion(BaseHandler):
    async def get(self):
        identifier = self.get_query_argument('id')
        logger.info("Ingester %r polling...", identifier)
        self.coordinator.add_ingester(identifier, self)
        try:
            self._close_event = asyncio.Event()
            await self._close_event.wait()
            # TODO: Send 'ingest' event to ingester
        finally:
            self.coordinator.remove_ingester(identifier, self)

    def on_connection_close(self):
        logger.info("Ingester connection closed")
        self._close_event.set()


class Ingested(BaseHandler):
    def post(self):
        identifier = self.get_query_argument('id')
        obj = self.get_json()
        self.coordinator.ingested(identifier,
                                  obj['dataset_id'], obj['id'], obj['meta'])
        self.send_json({})


class Application(tornado.web.Application):
    def __init__(self, *args, es_hosts, **kwargs):
        super(Application, self).__init__(*args, **kwargs)

        self.elasticsearch = elasticsearch.Elasticsearch(es_hosts)
        self.coordinator = Coordinator()


def make_app(debug=False):
    if 'XDG_CACHE_HOME' in os.environ:
        cache = os.environ['XDG_CACHE_HOME']
    else:
        cache = os.path.expanduser('~/.cache')
    os.makedirs(cache, 0o700, exist_ok=True)
    cache = os.path.join(cache, 'datamart.json')
    secret = None
    try:
        fp = open(cache)
    except IOError:
        pass
    else:
        try:
            secret = json.load(fp)['cookie_secret']
            fp.close()
        except Exception:
            logger.exception("Couldn't load cookie secret from cache file")
        if not isinstance(secret, str) or not 10 <= len(secret) < 2048:
            logger.error("Invalid cookie secret in cache file")
            secret = None
    if secret is None:
        secret = os.urandom(30).decode('iso-8859-15')
        try:
            fp = open(cache, 'w')
            json.dump({'cookie_secret': secret}, fp)
            fp.close()
        except IOError:
            logger.error("Couldn't open cache file, cookie secret won't be "
                         "persisted! Users will be logged out if you restart "
                         "the program.")

    return Application(
        [
            URLSpec('/', Index, name='index'),
            URLSpec('/status', Status),
            URLSpec('/dataset/([^/]+)', Dataset),

            # Used by discovery plugins
            URLSpec('/poll/discovery', PollDiscovery),
            URLSpec('/dataset_discovered', DatasetDiscovered),
            URLSpec('/dataset_downloaded', DatasetDownloaded),
            URLSpec('/allocate_dataset', AllocateDataset),

            # Used by ingestion plugins
            URLSpec('/poll/ingestion', PollIngestion),
            URLSpec('/ingested', Ingested),
        ],
        static_path=pkg_resources.resource_filename('datamart_coordinator',
                                                    'static'),
        debug=debug,
        cookie_secret=secret,
        es_hosts=os.environ['ELASTICSEARCH_HOSTS'].split(','),
    )


def main():
    logging.root.handlers.clear()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")

    app = make_app()
    app.listen(8001)
    loop = tornado.ioloop.IOLoop.current()
    loop.start()


if __name__ == '__main__':
    main()
