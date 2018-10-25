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

    template_env.globals['islist'] = lambda v: isinstance(v, (list, tuple))
    template_env.globals['isdict'] = lambda v: isinstance(v, dict)

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
            'recent_discoveries': self.coordinator.recent_discoveries,
        })


class Search(BaseHandler):
    def get(self):
        self.render('search.html')


class Dataset(BaseHandler):
    def get(self, dataset_id):
        # Get metadata from Elasticsearch
        es = self.application.elasticsearch
        metadata = es.get('datamart', '_doc', id=dataset_id)['_source']
        materialize = metadata.pop('materialize', {})
        discoverer = materialize.pop('identifier', '(unknown)')
        self.render('dataset.html',
                    dataset_id=dataset_id, discoverer=discoverer,
                    metadata=metadata, materialize=materialize)


class AllocateDataset(BaseHandler):
    def get(self):
        identifier = self.get_query_argument('id')
        path = self.coordinator.allocate_shared(identifier)
        self.send_json({'path': path, 'max_size_bytes': 1 << 30})


class Application(tornado.web.Application):
    def __init__(self, *args, es, **kwargs):
        super(Application, self).__init__(*args, **kwargs)

        self.elasticsearch = es
        self.coordinator = Coordinator(self.elasticsearch)


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

    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )

    return Application(
        [
            URLSpec('/', Index, name='index'),
            URLSpec('/status', Status),
            URLSpec('/search', Search, name='search'),
            URLSpec('/dataset/([^/]+)', Dataset),

            URLSpec('/allocate_dataset', AllocateDataset),
        ],
        static_path=pkg_resources.resource_filename('datamart_coordinator',
                                                    'static'),
        debug=debug,
        cookie_secret=secret,
        es=es,
    )


def main():
    logging.root.handlers.clear()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")

    app = make_app(debug=True)
    app.listen(8001)
    loop = tornado.ioloop.IOLoop.current()
    loop.start()


if __name__ == '__main__':
    main()
