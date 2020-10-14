import asyncio
import elasticsearch
import lazo_index_service
import logging
import os
import prometheus_client
import redis
import tornado.ioloop
from tornado.routing import Rule, PathMatches, URLSpec
import tornado.httputil
import tornado.web

from datamart_core.common import setup_logging
from datamart_core.prom import PromMeasureRequest
import datamart_profiler

from .augment import Augment, AugmentResult
from .base import BUCKETS, BaseHandler, Application
from .download import DownloadId, Download, Metadata
from .profile import Profile
from .search import Search
from .sessions import SessionNew, SessionGet
from .upload import Upload


logger = logging.getLogger(__name__)


PROM_LOCATION = PromMeasureRequest(
    count=prometheus_client.Counter(
        'req_location_count',
        "Location search requests",
    ),
    time=prometheus_client.Histogram(
        'req_location_seconds',
        "Location search request time",
        buckets=BUCKETS,
    ),
)
PROM_STATISTICS = PromMeasureRequest(
    count=prometheus_client.Counter(
        'req_statistics_count',
        "Statistics requests",
    ),
    time=prometheus_client.Histogram(
        'req_statistics_seconds',
        "Statistics request time",
        buckets=BUCKETS,
    ),
)
PROM_VERSION = PromMeasureRequest(
    count=prometheus_client.Counter(
        'req_version_count',
        "Version requests",
    ),
    time=prometheus_client.Histogram(
        'req_version_seconds',
        "Version request time",
        buckets=BUCKETS,
    ),
)


class LocationSearch(BaseHandler):
    @PROM_LOCATION.sync()
    def post(self):
        query = self.get_body_argument('q').strip()
        geo_data = self.application.geo_data
        area = geo_data.resolve_name(query)
        if area is not None:
            bounds = area.bounds
            logger.info("Resolved area %r to %r", query, area)
            return self.send_json({'results': [
                {
                    'id': area.id,
                    'name': area.name,
                    'boundingbox': bounds,
                }
            ]})
        else:
            return self.send_json({'results': []})


class Statistics(BaseHandler):
    @PROM_STATISTICS.sync()
    def get(self):
        return self.send_json({
            'recent_discoveries': self.application.recent_discoveries,
            'sources_counts': self.application.sources_counts,
            'custom_fields': self.application.custom_fields,
        })


class Version(BaseHandler):
    @PROM_VERSION.sync()
    def get(self):
        return self.send_json({
            'version': os.environ['DATAMART_VERSION'].lstrip('v'),
            'min_profiler_version': datamart_profiler.__version__,
        })


class Health(BaseHandler):
    def get(self):
        if self.application.is_closing:
            self.set_status(503, reason="Shutting down")
            return self.finish('shutting down')
        else:
            return self.finish('ok')


class CustomErrorHandler(tornado.web.ErrorHandler, BaseHandler):
    pass


class ApiRule(Rule):
    VERSIONS = {'1'}

    def __init__(self, pattern, versions, target):
        assert isinstance(versions, str)
        assert set(versions).issubset(self.VERSIONS)
        assert pattern[0] == '/'
        matcher = PathMatches(f'/api/v[{versions}]{pattern}')
        super(ApiRule, self).__init__(matcher, target)


def make_app(debug=False):
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
    redis_client = redis.Redis(host=os.environ['REDIS_HOST'])
    lazo_client = lazo_index_service.LazoIndexClient(
        host=os.environ['LAZO_SERVER_HOST'],
        port=int(os.environ['LAZO_SERVER_PORT'])
    )

    return Application(
        [
            ApiRule('/profile', '1', Profile),
            ApiRule('/search', '1', Search),
            ApiRule('/download/([^/]+)', '1', DownloadId),
            ApiRule('/download', '1', Download),
            ApiRule('/metadata/([^/]+)', '1', Metadata),
            ApiRule('/augment', '1', Augment),
            ApiRule('/augment/([^/]+)', '1', AugmentResult),
            ApiRule('/upload', '1', Upload),
            ApiRule('/session/new', '1', SessionNew),
            ApiRule('/session/([^/]+)', '1', SessionGet),
            ApiRule('/location', '1', LocationSearch),
            ApiRule('/statistics', '1', Statistics),
            ApiRule('/version', '1', Version),

            URLSpec('/health', Health),
        ],
        debug=debug,
        es=es,
        redis_client=redis_client,
        lazo=lazo_client,
        default_handler_class=CustomErrorHandler,
        default_handler_args={"status_code": 404},
    )


def main():
    setup_logging()
    debug = os.environ.get('DEBUG') not in (None, '', 'no', 'off', 'false')
    prometheus_client.start_http_server(8000)
    logger.info("Startup: apiserver %s", os.environ['DATAMART_VERSION'])
    if debug:
        logger.error("Debug mode is ON")

    app = make_app(debug)
    app.listen(8002, xheaders=True, max_buffer_size=2147483648)
    loop = tornado.ioloop.IOLoop.current()
    if debug:
        asyncio.get_event_loop().set_debug(True)
    loop.start()
