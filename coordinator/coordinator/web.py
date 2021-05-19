import asyncio
from datetime import datetime
import elasticsearch
import logging
import jinja2
import json
import lazo_index_service
import os
import pkg_resources
import prometheus_client
import socket
from tornado.httpclient import AsyncHTTPClient
import tornado.ioloop
from tornado.routing import URLSpec
import tornado.web
from tornado.web import HTTPError
from urllib.parse import quote_plus

from datamart_core.common import PrefixedElasticsearch, json2msg, \
    delete_dataset_from_index, setup_logging

from .coordinator import Coordinator


SIZE = 10000


logger = logging.getLogger(__name__)


class BaseHandler(tornado.web.RequestHandler):
    """Base class for all request handlers.
    """
    application: 'Application'

    template_env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(
            [pkg_resources.resource_filename('coordinator',
                                             'templates')]
        ),
        autoescape=jinja2.select_autoescape(('html', 'htm')),
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

    def set_default_headers(self):
        self.set_header('Server', 'Auctus/%s' % os.environ['DATAMART_VERSION'])

    def render_string(self, template_name, **kwargs):
        template = self.template_env.get_template(template_name)
        return template.render(
            handler=self,
            current_user=self.current_user,
            api_url=self.application.api_url,
            frontend_url=self.application.frontend_url,
            **kwargs)

    def get_json(self):
        type_ = self.request.headers.get('Content-Type', '')
        if not type_.startswith('application/json'):
            self.send_error_json(400, "Expected JSON")
            raise HTTPError(400)
        try:
            return json.loads(self.request.body.decode('utf-8'))
        except UnicodeDecodeError:
            self.send_error_json(400, "Invalid character encoding")
            raise HTTPError(400)
        except json.JSONDecodeError:
            self.send_error_json(400, "Invalid JSON")
            raise HTTPError(400)

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

    http_client = AsyncHTTPClient(defaults=dict(user_agent="Auctus"))

    def get_current_user(self):
        return self.get_secure_cookie('user')

    @property
    def coordinator(self):
        return self.application.coordinator


class Index(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        frontend = self.application.frontend_url
        recent_uploads = [
            dict(
                id=upload['id'],
                name=upload['name'] or upload['id'],
                discovered=datetime.fromisoformat(upload['discovered'].rstrip('Z')).strftime('%Y-%m-%d %H:%M:%S'),
                link=(
                    frontend
                    + '/?q='
                    + quote_plus(json.dumps({'query': upload['id']}))
                )
            )
            for upload in self.coordinator.recent_uploads()
        ]
        return self.render(
            'index.html',
            recent_uploads=recent_uploads,
            error_counts=sorted(self.coordinator.error_counts.items()),
        )


class Query(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        return self.render('query.html')


class Errors(BaseHandler):
    def get(self, error_type):
        datasets = self.coordinator.get_datasets_with_error(error_type)
        return self.render(
            'errors.html',
            error_type=error_type,
            datasets=datasets,
        )


class Login(BaseHandler):
    def get(self):
        if self.current_user:
            return self._go_to_next()
        else:
            return self.render(
                'login.html',
                next=self.get_argument('next', ''),
            )

    def post(self):
        password = self.get_body_argument('password')
        if password == self.application.admin_password:
            logger.info("Admin logged in")
            self.set_secure_cookie('user', 'admin')
            return self._go_to_next()
        else:
            self.render(
                'login.html',
                next=self.get_argument('next', ''),
                error="Invalid password",
            )

    def _go_to_next(self):
        next_ = self.get_argument('next', '')
        if not next_:
            next_ = self.reverse_url('index')
        return self.redirect(next_)


class DeleteDataset(BaseHandler):
    @tornado.web.authenticated
    def post(self, dataset_id):
        delete_dataset_from_index(
            self.application.elasticsearch,
            dataset_id,
            lazo_client=self.application.lazo_client,
        )
        self.coordinator.delete_recent(dataset_id)
        self.set_status(204)
        return self.finish()


class ReprocessDataset(BaseHandler):
    @tornado.web.authenticated
    async def post(self, dataset_id):
        try:
            obj = self.application.elasticsearch.get('datasets', dataset_id)['_source']
        except elasticsearch.NotFoundError:
            obj = self.application.elasticsearch.get('pending', dataset_id)['_source']['metadata']

        metadata = dict(name=obj['name'],
                        materialize=obj['materialize'],
                        source=obj.get('source', 'unknown'))
        if obj.get('description'):
            metadata['description'] = obj['description']
        if obj.get('date'):
            metadata['date'] = obj['date']
        if obj.get('manual_annotations'):
            metadata['manual_annotations'] = obj['manual_annotations']
        await self.coordinator.profile_exchange.publish(
            json2msg(
                dict(id=dataset_id, metadata=metadata),
            ),
            '',
        )

        self.set_status(204)
        return await self.finish()


class QuerySearch(BaseHandler):
    @tornado.web.authenticated
    def post(self):
        body = self.request.body.decode('utf-8')
        try:
            body = json.loads(body)
            hits = self.application.elasticsearch.search(
                index='datasets,pending',
                body=body,
            )['hits']['hits']
        except Exception as e:
            return self.send_json({'error': repr(e)})
        self.send_json({'hits': hits})
        return self.finish()


class QueryReprocess(BaseHandler):
    @tornado.web.authenticated
    async def post(self):
        body = self.get_json()
        hits = self.application.elasticsearch.scan(
            index='datasets,pending',
            query=body,
        )

        reprocessed = 0
        for hit in hits:
            dataset_id = hit['_id']
            if hit['_index'] == self.application.elasticsearch.prefix + 'pending':
                obj = hit['_source']['metadata']
            else:
                obj = hit['_source']

            metadata = dict(name=obj['name'],
                            materialize=obj['materialize'],
                            source=obj.get('source', 'unknown'))
            if obj.get('description'):
                metadata['description'] = obj['description']
            if obj.get('date'):
                metadata['date'] = obj['date']
            if obj.get('manual_annotations'):
                metadata['manual_annotations'] = obj['manual_annotations']
            await self.coordinator.profile_exchange.publish(
                json2msg(
                    dict(id=dataset_id, metadata=metadata),
                ),
                '',
            )
            reprocessed += 1
        return await self.send_json({'number_reprocessed': reprocessed})


class PurgeSource(BaseHandler):
    @tornado.web.authenticated
    def post(self):
        source = self.get_json()['source']
        hits = self.application.elasticsearch.scan(
            index='datasets,pending',
            query={
                'query': {
                    'bool': {
                        'should': [
                            {
                                'term': {
                                    'materialize.identifier': source,
                                },
                            },
                            {
                                'term': {
                                    'source': source,
                                },
                            },
                        ],
                        'minimum_should_match': 1,
                    },
                },
            },
            _source=False,
            size=SIZE,
        )
        deleted = 0
        for h in hits:
            delete_dataset_from_index(
                self.application.elasticsearch,
                h['_id'],
                self.application.lazo_client,
            )
            deleted += 1
        return self.send_json({'number_deleted': deleted})


class Statistics(BaseHandler):
    def prepare(self):
        super(BaseHandler, self).prepare()
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Methods', 'GET')

    def get(self):
        return self.send_json({
            'recent_discoveries': list(self.coordinator.recent_discoveries()),
            'sources_counts': self.coordinator.sources_counts,
        })


class CustomErrorHandler(tornado.web.ErrorHandler, BaseHandler):
    pass


class Application(tornado.web.Application):
    def __init__(self, *args, es, lazo, **kwargs):
        super(Application, self).__init__(*args, **kwargs)

        self.api_url = os.environ['API_URL'].rstrip('/')
        self.frontend_url = os.environ['FRONTEND_URL'].rstrip('/')
        self.elasticsearch = es
        self.lazo_client = lazo
        self.coordinator = Coordinator(self.elasticsearch)
        self.admin_password = os.environ['ADMIN_PASSWORD']


def make_app(debug=False):
    cache = '/cache/secret-key.json'
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
            with open(cache, 'w') as fp:
                json.dump({'cookie_secret': secret}, fp)
        except IOError:
            logger.error("Couldn't open cache file, cookie secret won't be "
                         "persisted! Users will be logged out if you restart "
                         "the program.")

    es = PrefixedElasticsearch()
    lazo_client = lazo_index_service.LazoIndexClient(
        host=os.environ['LAZO_SERVER_HOST'],
        port=int(os.environ['LAZO_SERVER_PORT'])
    )

    return Application(
        [
            URLSpec('/api/statistics', Statistics),
            URLSpec('/api/delete_dataset/([^/]+)', DeleteDataset),
            URLSpec('/api/reprocess_dataset/([^/]+)', ReprocessDataset),
            URLSpec('/api/search', QuerySearch),
            URLSpec('/api/reprocess', QueryReprocess),
            URLSpec('/api/purge_source', PurgeSource),
            URLSpec('/', Index, name='index'),
            URLSpec('/query', Query, name='query'),
            URLSpec('/errors/([^/]+)', Errors, name='errors'),
            URLSpec('/login', Login, name='login'),
        ],
        static_path=pkg_resources.resource_filename('coordinator',
                                                    'static'),
        login_url='/login',
        xsrf_cookies=True,
        debug=debug,
        cookie_secret=secret,
        es=es,
        lazo=lazo_client,
        default_handler_class=CustomErrorHandler,
        default_handler_args={"status_code": 404},
    )


def main():
    setup_logging()
    debug = os.environ.get('AUCTUS_DEBUG') not in (
        None, '', 'no', 'off', 'false',
    )
    prometheus_client.start_http_server(8000)
    logger.info(
        "Startup: coordinator %s %s",
        os.environ['DATAMART_VERSION'],
        socket.gethostbyname(socket.gethostname()),
    )
    if debug:
        logger.error("Debug mode is ON")

    app = make_app(debug)
    app.listen(8003, xheaders=True, max_buffer_size=2147483648)
    loop = tornado.ioloop.IOLoop.current()
    if debug:
        asyncio.get_event_loop().set_debug(True)
    loop.start()
