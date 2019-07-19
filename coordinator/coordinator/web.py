import datetime
import elasticsearch
import logging
import jinja2
import json
import os
import pkg_resources
import prometheus_client
import shutil
from tornado.httpclient import AsyncHTTPClient
import tornado.ioloop
from tornado.routing import URLSpec
import tornado.web
from tornado.web import HTTPError, RequestHandler
import uuid

from .coordinator import Coordinator
from datamart_core.common import Type, json2msg

logger = logging.getLogger(__name__)


class BaseHandler(RequestHandler):
    """Base class for all request handlers.
    """
    template_env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(
            [pkg_resources.resource_filename('coordinator',
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

    def _tpl_json_table(table):
        lines = [[]]

        def to_lines(item):
            if isinstance(item, dict):
                if item:
                    items = item.items()
                else:
                    lines[-1].append([1, '{}'])
                    lines.append([])
                    return 1
            elif isinstance(item, (list, tuple)):
                if item:
                    items = enumerate(item)
                else:
                    lines[-1].append([1, '[]'])
                    lines.append([])
                    return 1
            else:
                lines[-1].append([1, item])
                lines.append([])
                return 1
            l = 0
            for k, v in items:
                key = [1, k]
                lines[-1].append(key)
                key[0] = to_lines(v)
                l += key[0]
            return l

        to_lines(table)
        return lines[:-1]
    template_env.globals['json_table'] = _tpl_json_table

    def render_string(self, template_name, **kwargs):
        template = self.template_env.get_template(template_name)
        return template.render(
            handler=self,
            current_user=self.current_user,
            query_host=os.environ.get('QUERY_HOST', ''),
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

    http_client = AsyncHTTPClient(defaults=dict(user_agent="DataMart"))

    @property
    def coordinator(self):
        return self.application.coordinator


class Index(BaseHandler):
    def get(self):
        self.render('index.html')


class Status(BaseHandler):
    def get(self):
        return self.send_json({
            'recent_discoveries': self.coordinator.recent_discoveries,
            'sources_counts': self.coordinator.sources_counts,
        })


class Search(BaseHandler):
    def get(self):
        self.render('search.html')


class Upload(BaseHandler):
    def get(self):
        self.render('upload.html')

    async def post(self):
        if 'file' in self.request.files:
            file = self.request.files['file'][0]
            metadata = dict(
                filename=file.filename,
                name=self.get_body_argument('name', None),
                description=self.get_body_argument('description', None),
                materialize=dict(identifier='datamart.upload'),
            )
            dataset_id = 'datamart.upload.%s' % uuid.uuid4().hex

            # Write file to shared storage
            dataset_dir = os.path.join('/datasets', dataset_id)
            os.mkdir(dataset_dir)
            try:
                with open(os.path.join(dataset_dir, 'main.csv'), 'wb') as fp:
                    fp.write(file.body)
            except Exception:
                shutil.rmtree(dataset_dir)
                raise
        elif self.get_body_argument('address', None):
            # Check the URL
            address = self.get_body_argument('address')
            response = await self.http_client.fetch(address, raise_error=False)
            if response.code != 200:
                return self.render(
                    'upload.html',
                    error="Error {} {}".format(response.code, response.reason),
                )

            # Metadata with 'direct_url' in materialization info
            metadata = dict(
                name=self.get_body_argument('name', None),
                description=self.get_body_argument('description', None),
                materialize=dict(identifier='datamart.url',
                                 direct_url=address),
            )
            dataset_id = 'datamart.url.%s' % (
                uuid.uuid5(uuid.NAMESPACE_URL, address).hex
            )
        else:
            return self.render('upload.html', error="No file entered")

        # Publish to the profiling queue
        await self.coordinator.profile_exchange.publish(
            json2msg(
                dict(
                    id=dataset_id,
                    metadata=metadata,
                ),
                # Lower priority than on-demand datasets, but higher than base
                priority=1,
            ),
            '',
        )

        self.redirect('/')


def format_size(bytes):
    units = [' B', ' kB', ' MB', ' GB', ' TB', ' PB', ' EB', ' ZB', ' YB']

    i = 0
    while bytes > 1000 and i + 1 < len(units):
        bytes = bytes / 1000.0
        i += 1

    return '%.1f%s' % (bytes, units[i])


class Dataset(BaseHandler):
    def get(self, dataset_id):
        # Get metadata from Elasticsearch
        es = self.application.elasticsearch
        try:
            metadata = es.get('datamart', '_doc', id=dataset_id)['_source']
        except elasticsearch.NotFoundError:
            raise HTTPError(404)
        # readable format for temporal and numerical coverage
        for column in metadata['columns']:
            if 'coverage' in column:
                if Type.DATE_TIME in column['semantic_types']:
                    column['temporal coverage'] = []
                    for range_ in column['coverage']:
                        from_ = \
                            datetime.datetime.utcfromtimestamp(int(range_['range']['gte'])).\
                            strftime('%Y-%m-%d %H:%M')
                        to_ = \
                            datetime.datetime.utcfromtimestamp(int(range_['range']['lte'])).\
                            strftime('%Y-%m-%d %H:%M')
                        column['temporal coverage'].append({
                            'from': from_,
                            'to': to_
                        })
                elif Type.INTEGER in column['structural_type']:
                    column['numerical coverage'] = [
                        {'from': int(range_['range']['gte']),
                         'to': int(range_['range']['lte'])
                         } for range_ in column['coverage']
                    ]
                else:
                    column['numerical coverage'] = [
                        {'from': float(range_['range']['gte']),
                         'to': float(range_['range']['lte'])
                         } for range_ in column['coverage']
                    ]
                del column['coverage']
        materialize = metadata.pop('materialize', {})
        discoverer = materialize.pop('identifier', '(unknown)')
        spatial_coverage = metadata.pop('spatial_coverage', [])
        self.render('dataset.html',
                    dataset_id=dataset_id, discoverer=discoverer,
                    metadata=metadata, materialize=materialize,
                    spatial_coverage=spatial_coverage,
                    size=format_size(metadata['size']))


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
            URLSpec('/search_form', Search, name='search'),
            URLSpec('/upload', Upload, name='upload'),
            URLSpec('/dataset/([^/]+)', Dataset, name='dataset'),
        ],
        static_path=pkg_resources.resource_filename('coordinator',
                                                    'static'),
        debug=debug,
        serve_traceback=True,
        cookie_secret=secret,
        es=es,
    )


def main():
    logging.root.handlers.clear()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")
    prometheus_client.start_http_server(8000)

    app = make_app()
    app.listen(8001, xheaders=True, max_buffer_size=2147483648)
    loop = tornado.ioloop.IOLoop.current()
    loop.start()
