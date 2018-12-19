import aio_pika
import datetime
import distance
import elasticsearch
import logging
import jinja2
import json
import os
import pkg_resources
import shutil
import tornado.ioloop
from tornado.routing import URLSpec
import tornado.web
from tornado.web import HTTPError, RequestHandler
import uuid

from .coordinator import Coordinator
from datamart_core.common import Type


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


class Upload(BaseHandler):
    def get(self):
        self.render('upload.html')

    async def post(self):
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

        # Publish to the profiling queue
        await self.coordinator.profile_exchange.publish(
            aio_pika.Message(
                json.dumps(dict(
                    id=dataset_id,
                    metadata=metadata,
                )).encode('utf-8'),
            ),
            '',
        )

        self.redirect(self.reverse_url('dataset', dataset_id))


def format_size(bytes):
    units = [' B', ' kB', ' MB', ' GB', ' TB', 'PB', 'EB', 'ZB', 'YB']

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
        # readable format for temporal coverage
        for column in metadata['columns']:
            if Type.DATE_TIME in column['semantic_types']:
                for range_ in column['coverage']:
                    range_['range']['gte'] = \
                        datetime.datetime.utcfromtimestamp(int(range_['range']['gte'])).\
                        strftime('%Y-%m-%d %H:%M')
                    range_['range']['lte'] = \
                        datetime.datetime.utcfromtimestamp(int(range_['range']['lte'])). \
                            strftime('%Y-%m-%d %H:%M')
        materialize = metadata.pop('materialize', {})
        discoverer = materialize.pop('identifier', '(unknown)')
        self.render('dataset.html',
                    dataset_id=dataset_id, discoverer=discoverer,
                    metadata=metadata, materialize=materialize,
                    size=format_size(metadata['size']))


# TODO: move to a better place?
def compute_levenshtein_sim(str1, str2):
    if len(str1) < 3:
        str1_set = [str1]
    else:
        str1_set = [str1[i:i + 3] for i in range(len(str1) - 2)]

    if len(str2) < 3:
        str2_set = [str2]
    else:
        str2_set = [str2[i:i + 3] for i in range(len(str2) - 2)]

    return 1 - distance.nlevenshtein(str1_set, str2_set, method=2)


# TODO: move to a better place?
def get_column_coverage(es, dataset_id):
    column_coverage = dict()

    index_query = \
        '''
            {
                "query" : {
                    "match":{
                        "_id": "%s"
                    }
                }
            }
        ''' % dataset_id

    result = es.search(index='datamart', body=index_query)

    hit = result['hits']['hits'][0]
    for column in hit['_source']['columns']:
        if 'coverage' not in column:
            continue
        column_name = column['name']
        if column['structural_type'] in (Type.INTEGER, Type.FLOAT):
            type_ = 'structural_type'
            type_value = column['structural_type']
        elif Type.DATE_TIME in column['semantic_types']:
            type_ = 'semantic_types'
            type_value = Type.DATE_TIME
        else:
            continue
        column_coverage[column_name] = {
            'type':       type_,
            'type_value': type_value,
            'ranges':     []
        }
        for range_ in column['coverage']:
            column_coverage[column_name]['ranges'].\
                append([float(range_['range']['gte']),
                        float(range_['range']['lte'])])

    if 'spatial_coverage' in hit['_source']:
        for spatial in hit['_source']['spatial_coverage']:
            names = '(' + spatial['lat'] + ', ' + spatial['lon'] + ')'
            column_coverage[names] = {
                'type':      'spatial',
                'type_value': Type.LATITUDE + ', ' + Type.LONGITUDE,
                'ranges':     []
            }
            for range_ in spatial['ranges']:
                column_coverage[names]['ranges'].\
                    append(range_['range']['coordinates'])

    return column_coverage


# TODO: move to a better place?
def get_numerical_coverage_intersections(es, dataset_id, type_, type_value, ranges):

    intersections = dict()
    column_total_coverage = 0

    for range_ in ranges:
        logging.warning("Range: " + str(range_))
        column_total_coverage += (range_[1] - range_[0] + 1)

        query = '''
            {
                "query" : {
                    "nested" : {
                        "path" : "columns",
                        "query" : {
                            "bool" : {
                                "must_not": {
                                    "match": { "_id": "%s" }
                                },
                                "must" : [
                                    {
                                        "match" : { "columns.%s" : "%s" }
                                    },
                                    {
                                        "nested" : {
                                            "path" : "columns.coverage",
                                            "query" : {
                                                "range" : {
                                                    "columns.coverage.range" : {
                                                        "gte" : %.6f,
                                                        "lte" : %.6f,
                                                        "relation" : "intersects"
                                                    }
                                                }
                                            },
                                            "inner_hits": {
                                              "_source" : false
                                            }
                                        }
                                    }
                                ]
                            }
                        },
                        "inner_hits": {
                            "_source" : false
                        }
                    }
                }
            }''' % (dataset_id, type_, type_value, range_[0], range_[1])

        result = es.search(
            index='datamart',
            body=query,
            scroll='2m',
            size=10000
        )

        sid = result['_scroll_id']
        scroll_size = result['hits']['total']

        while scroll_size > 0:
            for hit in result['hits']['hits']:

                dataset_name = hit['_id']
                columns = hit['_source']['columns']
                inner_hits = hit['inner_hits']

                for column_hit in inner_hits['columns']['hits']['hits']:
                    column_offset = int(column_hit['_nested']['offset'])
                    column_name = columns[column_offset]['name']
                    name = '%s$$%s' % (dataset_name, column_name)
                    if name not in intersections:
                        intersections[name] = 0

                    # ranges from column
                    for range_hit in column_hit['inner_hits']['columns.coverage']['hits']['hits']:
                        # compute intersection
                        range_offset = int(range_hit['_nested']['_nested']['offset'])
                        start_result = columns[column_offset]['coverage'][range_offset]['range']['gte']
                        end_result = columns[column_offset]['coverage'][range_offset]['range']['lte']

                        start = max(start_result, range_[0])
                        end = min(end_result, range_[1])

                        intersections[name] += (end - start + 1)

            # scrolling
            result = es.scroll(
                scroll_id=sid,
                scroll='2m'
            )
            sid = result['_scroll_id']
            scroll_size = len(result['hits']['hits'])

    return intersections, column_total_coverage


# TODO: move to a better place?
def get_spatial_coverage_intersections(es, dataset_id, ranges):

    intersections = dict()
    column_total_coverage = 0

    for range_ in ranges:
        column_total_coverage += (range_[1][0] - range_[0][0])*(range_[0][1] - range_[1][1])

        query = '''
            {
                "query" : {
                    "nested" : {
                        "path" : "spatial_coverage.ranges",
                        "query" : {
                            "bool" : {
                                "must_not": {
                                    "match": { "_id": "%s" }
                                },
                                "filter": {
                                    "geo_shape": {
                                        "spatial_coverage.ranges.range": {
                                            "shape": {
                                                "type": "envelope",
                                                "coordinates" : [[%.6f, %.6f], [%.6f, %.6f]]
                                            },
                                            "relation": "intersects"
                                        }
                                    }
                                }
                            }
                        },
                        "inner_hits": {
                            "_source" : false
                        }
                    }
                }
            }''' % (dataset_id, range_[0][0], range_[0][1], range_[1][0], range_[1][1])

        result = es.search(
            index='datamart',
            body=query,
            scroll='2m',
            size=10000
        )

        sid = result['_scroll_id']
        scroll_size = result['hits']['total']

        while scroll_size > 0:
            for hit in result['hits']['hits']:

                dataset_name = hit['_id']
                spatial_coverages = hit['_source']['spatial_coverage']
                inner_hits = hit['inner_hits']

                for coverage_hit in inner_hits['spatial_coverage.ranges']['hits']['hits']:
                    spatial_coverage_offset = int(coverage_hit['_nested']['offset'])
                    spatial_coverage_name = \
                        '(' + spatial_coverages[spatial_coverage_offset]['lat'] + ', ' \
                        + spatial_coverages[spatial_coverage_offset]['lon'] + ')'
                    name = '%s$$%s' % (dataset_name, spatial_coverage_name)
                    if name not in intersections:
                        intersections[name] = 0

                    # compute intersection
                    range_offset = int(coverage_hit['_nested']['_nested']['offset'])
                    min_lon = \
                        spatial_coverages[spatial_coverage_offset]['ranges'][range_offset]['range']['coordinates'][0][0]
                    max_lat = \
                        spatial_coverages[spatial_coverage_offset]['ranges'][range_offset]['range']['coordinates'][0][1]
                    max_lon = \
                        spatial_coverages[spatial_coverage_offset]['ranges'][range_offset]['range']['coordinates'][1][0]
                    min_lat = \
                        spatial_coverages[spatial_coverage_offset]['ranges'][range_offset]['range']['coordinates'][1][1]

                    n_min_lon = max(min_lon, range_[0][0])
                    n_max_lat = min(max_lat, range_[0][1])
                    n_max_lon = max(max_lon, range_[1][0])
                    n_min_lat = min(min_lat, range_[1][1])

                    intersections[name] += (n_max_lon - n_min_lon)*(n_max_lat - n_min_lat)

            # scrolling
            result = es.scroll(
                scroll_id=sid,
                scroll='2m'
            )
            sid = result['_scroll_id']
            scroll_size = len(result['hits']['hits'])

    return intersections, column_total_coverage


# TODO: move to a better place?
def get_coverage_intersections(es, dataset_id):

    intersections = dict()
    types = dict()
    column_coverage = get_column_coverage(es, dataset_id)

    if not column_coverage:
        return intersections, types

    for column in column_coverage:
        type_ = column_coverage[column]['type']
        type_value = column_coverage[column]['type_value']
        if type_ == 'spatial':
            intersections_column, column_total_coverage = \
                get_spatial_coverage_intersections(
                    es,
                    dataset_id,
                    column_coverage[column]['ranges']
                )
        else:
            intersections_column, column_total_coverage = \
                get_numerical_coverage_intersections(
                    es,
                    dataset_id,
                    type_,
                    type_value,
                    column_coverage[column]['ranges']
                )

        if not intersections_column:
            continue

        types[column] = type_value

        intersections[column] = []
        for name, size in intersections_column.items():
            sim = compute_levenshtein_sim(
                column.lower(),
                name.split("$$")[1].lower()
            )
            score = size/column_total_coverage
            if type_value not in (Type.DATE_TIME,
                                  Type.LATITUDE + ', ' + Type.LONGITUDE):
                score *= sim
            intersections[column].append((name, score))

        intersections[column] = sorted(
            intersections[column],
            key=lambda item: item[1],
            reverse=True
        )

    return intersections, types


class JoinQuery(BaseHandler):
    def get(self, dataset_id):
        join_intersections, column_types = get_coverage_intersections(
            self.application.elasticsearch,
            dataset_id
        )
        self.render(
            'join_query.html',
            join_intersections=sorted(join_intersections.items()),
            types=column_types
        )


# TODO: move to a better place?
def get_all_datasets_columns(es):
    dataset_columns = dict()
    query = \
        '''
            {
                "query" : {
                    "match_all" : { }
                }
            }
        '''

    result = es.search(
        index='datamart',
        body=query,
        scroll='2m',
        size=10000
    )

    sid = result['_scroll_id']
    scroll_size = result['hits']['total']

    while scroll_size > 0:
        for hit in result['hits']['hits']:
            dataset = hit['_id']
            dataset_columns[dataset] = dict()

            for column in hit['_source']['columns']:
                name = column['name']
                for semantic_type in column['semantic_types']:
                    if semantic_type not in dataset_columns[dataset]:
                        dataset_columns[dataset][semantic_type] = []
                    dataset_columns[dataset][semantic_type].append(name)
                if not column['semantic_types']:
                    if column['structural_type'] not in dataset_columns[dataset]:
                        dataset_columns[dataset][column['structural_type']] = []
                    dataset_columns[dataset][column['structural_type']].append(name)

        # scrolling
        result = es.scroll(
            scroll_id=sid,
            scroll='2m'
        )
        sid = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])

    return dataset_columns


# TODO: move to a better place?
def get_unionable_datasets(es, dataset_id):
    dataset_columns = get_all_datasets_columns(es)
    main_dataset_columns = dataset_columns[dataset_id]
    del dataset_columns[dataset_id]

    n_columns = 0
    for type_ in main_dataset_columns:
        n_columns += len(main_dataset_columns[type_])

    column_pairs = dict()
    scores = dict()
    for dataset in dataset_columns:

        # check all pairs of attributes
        pairs = []
        for type_ in main_dataset_columns:
            if type_ not in dataset_columns[dataset]:
                continue
            for att_1 in main_dataset_columns[type_]:
                for att_2 in dataset_columns[dataset][type_]:
                    sim = compute_levenshtein_sim(att_1.lower(), att_2.lower())
                    pairs.append((att_1, att_2, sim))

        # choose pairs with higher Jaccard distance
        seen_1 = set()
        seen_2 = set()
        column_pairs[dataset] = []
        for att_1, att_2, sim in sorted(pairs,
                                        key=lambda item: item[2],
                                        reverse=True):
            if att_1 in seen_1 or att_2 in seen_2:
                continue
            seen_1.add(att_1)
            seen_2.add(att_2)
            column_pairs[dataset].append([att_1, att_2, sim])

        if len(column_pairs[dataset]) <= 1:
            column_pairs[dataset] = []
            continue

        scores[dataset] = 1

        for i in range(len(column_pairs[dataset])):
            sim = column_pairs[dataset][i][2]
            scores[dataset] += sim

        scores[dataset] = scores[dataset] / n_columns

    return column_pairs, sorted(
        scores.items(),
        key=lambda item: item[1],
        reverse=True
    )


class UnionQuery(BaseHandler):
    def get(self, dataset_id):
        column_pairs, scores = get_unionable_datasets(
            self.application.elasticsearch,
            dataset_id
        )
        self.render(
            'union_query.html',
            pairs=column_pairs,
            scores=scores
        )


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
            URLSpec('/upload', Upload, name='upload'),
            URLSpec('/dataset/([^/]+)', Dataset, name='dataset'),
            URLSpec('/join_query/([^/]+)', JoinQuery),
            URLSpec('/union_query/([^/]+)', UnionQuery),

            URLSpec('/allocate_dataset', AllocateDataset),
        ],
        static_path=pkg_resources.resource_filename('coordinator',
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
