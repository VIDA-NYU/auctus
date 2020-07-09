import csv
import elasticsearch
import io
import lazo_index_service
import logging
import json
import os
import prometheus_client
import redis
import tornado.ioloop
from tornado.routing import URLSpec
import tornado.httputil
import tornado.web

from datamart_core.common import setup_logging
from datamart_core.prom import PromMeasureRequest
import datamart_profiler

from .augment import Augment, AugmentResult
from .base import BUCKETS, BaseHandler, Application
from .download import DownloadId, Download, Metadata
from .enhance_metadata import enhance_metadata
from .graceful_shutdown import GracefulHandler
from .profile import ProfilePostedData, Profile, get_data_profile_from_es, \
    profile_token_re
from .search import TOP_K_SIZE, ClientError, parse_query, \
    get_augmentation_search_results
from .sessions import SessionNew, SessionGet
from .upload import Upload


logger = logging.getLogger(__name__)


PROM_SEARCH = PromMeasureRequest(
    count=prometheus_client.Counter(
        'req_search_count',
        "Search requests",
    ),
    time=prometheus_client.Histogram(
        'req_search_seconds',
        "Search request time",
        buckets=BUCKETS,
    ),
)
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


class Search(BaseHandler, GracefulHandler, ProfilePostedData):
    @PROM_SEARCH.sync()
    def post(self):
        type_ = self.request.headers.get('Content-type', '')
        data = None
        data_id = None
        data_profile = None
        if type_.startswith('application/json'):
            query = self.get_json()
        elif (type_.startswith('multipart/form-data') or
                type_.startswith('application/x-www-form-urlencoded')):
            # Get the query document
            query = self.get_body_argument('query', None)
            if query is None and 'query' in self.request.files:
                query = self.request.files['query'][0].body.decode('utf-8')
            if query is not None:
                query = json.loads(query)

            # Get the data
            data = self.get_body_argument('data', None)
            if 'data' in self.request.files:
                data = self.request.files['data'][0].body
            elif data is not None:
                data = data.encode('utf-8')

            # Get a reference to a dataset in the index
            data_id = self.get_body_argument('data_id', None)
            if 'data_id' in self.request.files:
                data_id = self.request.files['data_id'][0].body.decode('utf-8')

            # Get the data sketch JSON
            data_profile = self.get_body_argument('data_profile', None)
            if data_profile is None and 'data_profile' in self.request.files:
                data_profile = self.request.files['data_profile'][0].body
                data_profile = data_profile.decode('utf-8')
            if data_profile is not None:
                # Data profile can optionally be just the hash
                if len(data_profile) == 40 and profile_token_re.match(data_profile):
                    data_profile = self.application.redis.get(
                        'profile:' + data_profile,
                    )
                    if data_profile:
                        data_profile = json.loads(data_profile)
                    else:
                        return self.send_error_json(
                            404,
                            "Data profile token expired",
                        )
                else:
                    data_profile = json.loads(data_profile)

        elif (type_.startswith('text/csv') or
                type_.startswith('application/csv')):
            query = None
            data = self.request.body
        else:
            return self.send_error_json(
                400,
                "Either use multipart/form-data to send the 'query' JSON and "
                "'data' file (or 'data_profile' JSON), or use "
                "application/json to send a query alone, or use text/csv to "
                "send data alone",
            )

        if sum(1 for e in [data, data_id, data_profile] if e is not None) > 1:
            return self.send_error_json(
                400,
                "Please only provide one input dataset (either 'data', " +
                "'data_id', or  'data_profile')",
            )

        logger.info("Got search, content-type=%r%s%s%s%s",
                    type_.split(';')[0],
                    ', query' if query else '',
                    ', data' if data else '',
                    ', data_id' if data_id else '',
                    ', data_profile' if data_profile else '')

        # parameter: data
        if data is not None:
            data_profile, _ = self.handle_data_parameter(data)

        # parameter: data_id
        if data_id:
            data_profile = get_data_profile_from_es(
                self.application.elasticsearch,
                data_id,
            )
            if data_profile is None:
                return self.send_error_json(400, "No such dataset")

        # parameter: query
        query_args_main = list()
        query_sup_functions = list()
        query_sup_filters = list()
        tabular_variables = list()
        if query:
            try:
                (
                    query_args_main,
                    query_sup_functions, query_sup_filters,
                    tabular_variables,
                ) = parse_query(query, self.application.geo_data)
            except ClientError as e:
                return self.send_error_json(400, str(e))

        # At least one of them must be provided
        if not query_args_main and not data_profile:
            return self.send_error_json(
                400,
                "At least one of 'data' or 'query' must be provided",
            )

        if not data_profile:
            hits = self.application.elasticsearch.search(
                index='datamart',
                body={
                    'query': {
                        'bool': {
                            'must': query_args_main,
                        },
                    },
                },
                size=TOP_K_SIZE,
            )['hits']['hits']

            results = []
            for h in hits:
                meta = h.pop('_source')
                results.append(dict(
                    id=h['_id'],
                    score=h['_score'],
                    metadata=meta,
                    augmentation={
                        'type': 'none',
                        'left_columns': [],
                        'left_columns_names': [],
                        'right_columns': [],
                        'right_columns_names': []
                    },
                    supplied_id=None,
                    supplied_resource_id=None
                ))
        else:
            results = get_augmentation_search_results(
                self.application.elasticsearch,
                self.application.lazo_client,
                data_profile,
                query_args_main,
                query_sup_functions,
                query_sup_filters,
                tabular_variables,
                ignore_datasets=[data_id] if data_id is not None else [],
            )
        results = [enhance_metadata(result) for result in results]

        # Private API for the frontend, don't want clients to rely on it
        if self.get_query_argument('_parse_sample', ''):
            for result in results:
                sample = result['metadata'].get('sample', None)
                if sample:
                    result['sample'] = list(csv.reader(io.StringIO(sample)))

        return self.send_json(results)


class LocationSearch(BaseHandler):
    @PROM_LOCATION.sync()
    def post(self):
        query = self.get_body_argument('q').strip()
        geo_data = self.application.geo_data
        areas = geo_data.resolve_names([query.lower()])
        areas = [area for area in areas if area is not None]
        if areas and areas[0]:
            bounds = geo_data.get_bounds(areas[0].area)
            logger.info("Resolved area %r to %r", query, areas[0].area)
            return self.send_json({'results': [
                {
                    'area': areas[0].area,
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
            URLSpec('/profile', Profile, name='profile'),
            URLSpec('/search', Search, name='search'),
            URLSpec('/download/([^/]+)', DownloadId, name='download_id'),
            URLSpec('/download', Download, name='download'),
            URLSpec('/metadata/([^/]+)', Metadata, name='metadata'),
            URLSpec('/augment', Augment, name='augment'),
            URLSpec('/augment/([^/]+)', AugmentResult, name='augment_result'),
            URLSpec('/upload', Upload, name='upload'),
            URLSpec('/session/new', SessionNew, name='session_new'),
            URLSpec('/session/([^/]+)', SessionGet, name='session_get'),
            URLSpec('/location', LocationSearch, name='location_search'),
            URLSpec('/statistics', Statistics, name='statistics'),
            URLSpec('/version', Version, name='version'),
            URLSpec('/health', Health, name='health'),
        ],
        debug=debug,
        es=es,
        redis_client=redis_client,
        lazo=lazo_client,
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
    loop.start()
