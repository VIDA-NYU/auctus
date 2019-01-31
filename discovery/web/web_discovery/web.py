import asyncio
import aiohttp
import logging
import jinja2
import json
import os
import pkg_resources
import tornado.ioloop
from tornado.routing import URLSpec
import tornado.web
from tornado.web import HTTPError, RequestHandler
import uuid

from datamart_core import AsyncDiscoverer

from . import crawl


logger = logging.getLogger(__name__)


class BaseHandler(RequestHandler):
    """Base class for all request handlers.
    """
    template_env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(
            [pkg_resources.resource_filename('web_discovery',
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


class Index(BaseHandler):
    def get(self):
        self.render('index.html',
                    coordinator_host=os.environ.get('COORDINATOR_HOST', ''))


class PagesWithDatasetsFinder(crawl.DatasetFinder):
    def __init__(self):
        super(PagesWithDatasetsFinder, self).__init__()

        self.pages = {}

    async def dataset_found(self, page, url, size=None):
        try:
            result = self.pages[page['url']]
        except KeyError:
            result = self.pages[page['url']] = {
                'title': page['name'],
                'url': page['url'],
                'files': [],
            }
        result['files'].append({
            'url': url,
            'format': 'CSV',
            'size': size,
        })


class Pages(BaseHandler):
    async def post(self):
        obj = self.get_json()
        keywords = obj['keywords']

        finder = PagesWithDatasetsFinder()

        async with aiohttp.ClientSession() as session:
            top_results = await crawl.bing_search(session, keywords)

            futures = []
            for page in top_results:
                futures.append(asyncio.get_event_loop().create_task(
                    finder.find_datasets(session, page)
                ))
            _, notdone = await asyncio.wait(futures, timeout=60)
            if notdone:
                logger.warning("%d top results timed out", len(notdone))
                for fut in notdone:
                    fut.cancel()

        for page in finder.pages.values():
            page['files'].sort(key=lambda f: f['url'])

        return self.send_json({'pages': list(finder.pages.values())})


class Ingest(BaseHandler):
    discoverer = AsyncDiscoverer('datamart.websearch')

    async def post(self):
        files = self.get_json()['files']
        logger.info("Profiling requested: %s", ' '.join(files))

        results = []
        for file_url in files:
            metadata = {}
            dataset_id = uuid.uuid5(uuid.NAMESPACE_URL, str(file_url)).hex
            await self.discoverer.record_dataset(dict(direct_url=file_url),
                                                 metadata,
                                                 dataset_id=dataset_id)
            results.append({
                'url': file_url,
                'format': 'CSV',
                'size': None,
                'status': 'ingested',
                'dataset_id': self.discoverer.identifier + '.' + dataset_id,
            })

        return self.send_json({'files': results})


def make_web_discovery_app(debug=False):
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

    return tornado.web.Application(
        [
            URLSpec('/', Index, name='index'),
            URLSpec('/pages', Pages, name='pages'),
            URLSpec('/ingest', Ingest, name='ingest'),
        ],
        static_path=pkg_resources.resource_filename('web_discovery',
                                                    'static'),
        debug=debug,
        serve_traceback=True,
        cookie_secret=secret,
    )
