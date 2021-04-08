import asyncio
from datetime import datetime
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
from urllib.parse import quote_plus

from datamart_core.common import PrefixedElasticsearch, \
    delete_dataset_from_index, setup_logging

from .coordinator import Coordinator


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
            api_url=os.environ.get('API_URL', ''),
            **kwargs)

    def send_json(self, obj):
        if isinstance(obj, list):
            obj = {'results': obj}
        elif not isinstance(obj, dict):
            raise ValueError("Can't encode %r to JSON" % type(obj))
        self.set_header('Content-Type', 'application/json; charset=utf-8')
        return self.finish(json.dumps(obj))

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
            URLSpec('/', Index, name='index'),
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
    debug = os.environ.get('DEBUG') not in (None, '', 'no', 'off', 'false')
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
