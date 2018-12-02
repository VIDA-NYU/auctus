import distance
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
        metadata = es.get('datamart', '_doc', id=dataset_id)['_source']
        materialize = metadata.pop('materialize', {})
        discoverer = materialize.pop('identifier', '(unknown)')
        self.render('dataset.html',
                    dataset_id=dataset_id, discoverer=discoverer,
                    metadata=metadata, materialize=materialize,
                    size=format_size(metadata['size']))


# TODO: move to a better place?
def get_column_ranges(es, dataset_id):
    column_ranges = dict()

    index_query = \
        '''
            {
                "query" : {
                    "match" : {
                        "id" : "%s"
                    }
                }
            }
        ''' % dataset_id

    result = es.search(
        index='datamart_numerical_index',
        body=index_query,
        scroll='2m',
        size=10000
    )

    sid = result['_scroll_id']
    scroll_size = result['hits']['total']

    while scroll_size > 0:
        for hit in result['hits']['hits']:
            column_name = hit['_source']['name']
            if column_name not in column_ranges:
                column_ranges[column_name] = {
                    'type':   hit['_source']['type'],
                    'ranges': []
                }
            column_ranges[column_name]['ranges'].\
                append([float(hit['_source']['numerical_range']['gte']),
                        float(hit['_source']['numerical_range']['lte'])])

        # scrolling
        result = es.scroll(
            scroll_id=sid,
            scroll='2m'
        )
        sid = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])

    return column_ranges


# TODO: move to a better place?
def get_numerical_range_intersections(es, dataset_id):

    intersections = dict()
    types = dict()
    column_ranges = get_column_ranges(es, dataset_id)

    if not column_ranges:
        return intersections, types

    for column in column_ranges:
        type_ = column_ranges[column]['type']
        intersections_column = dict()
        total_size = 0
        for range_ in column_ranges[column]['ranges']:
            total_size += (range_[1] - range_[0] + 1)

            query = '''
                {
                  "query" : {
                    "bool": {
                      "must_not": {
                        "match": { "id": "%s" }
                      },
                      "must": [
                        {
                          "match": { "type": "%s" }
                        },
                        {
                          "range" : {
                            "numerical_range" : {
                              "gte" : %.8f,
                              "lte" : %.8f,
                              "relation" : "intersects"
                            }
                          }
                        }
                      ]
                    }
                  }
                }''' % (dataset_id, type_, range_[0], range_[1])

            result = es.search(
                index='datamart_numerical_index',
                body=query,
                scroll='2m',
                size=10000
            )

            sid = result['_scroll_id']
            scroll_size = result['hits']['total']

            while scroll_size > 0:
                for hit in result['hits']['hits']:

                    name = '%s$$%s' % (hit['_source']['id'], hit['_source']['name'])
                    if name not in intersections_column:
                        intersections_column[name] = 0

                    # Compute intersection
                    start_result = float(hit['_source']['numerical_range']['gte'])
                    end_result = float(hit['_source']['numerical_range']['lte'])

                    start = max(start_result, range_[0])
                    end = min(end_result, range_[1])

                    intersections_column[name] += (end - start + 1)

                # scrolling
                result = es.scroll(
                    scroll_id=sid,
                    scroll='2m'
                )
                sid = result['_scroll_id']
                scroll_size = len(result['hits']['hits'])

        if not intersections_column:
            continue

        if type_ == 'integer':
            types[column] = 'http://schema.org/Integer'
        elif type_ == 'float':
            types[column] = 'http://schema.org/Float'
        else:
            types[column] = 'http://schema.org/DateTime'

        intersections[column] = []
        for name, size in intersections_column.items():
            sim = distance.jaccard(
                column.lower(),
                name.split("$$")[1].lower()
            )
            score = size/total_size
            if type_ != 'datetime':
                score *= (1-sim)
            intersections[column].append((name, score))

        intersections[column] = sorted(
            intersections[column],
            key=lambda item: item[1],
            reverse=True
        )

    return intersections, types


class JoinQuery(BaseHandler):
    def get(self, dataset_id):
        join_intersections, column_types = get_numerical_range_intersections(
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


def get_intersection_size(es, dt_ranges_1, dt_1, att_1, dt_2, att_2):

    total_size_att_1 = 0
    intersection_size = 0
    if att_1 not in dt_ranges_1:
        return 0
    for range_ in dt_ranges_1[att_1]['ranges']:
        total_size_att_1 += (range_[1] - range_[0] + 1)

        query = '''
                    {
                      "query" : {
                        "bool": {
                          "must_not": {
                            "match": { "id": "%s" }
                          },
                          "must": [
                            {
                              "match": { "id": "%s" }
                            },
                            {
                              "match": { "name": "%s" }
                            },
                            {
                              "range" : {
                                "numerical_range" : {
                                  "gte" : %.8f,
                                  "lte" : %.8f,
                                  "relation" : "intersects"
                                }
                              }
                            }
                          ]
                        }
                      }
                    }''' % (dt_1, dt_2, att_2, range_[0], range_[1])

        result = es.search(
            index='datamart_numerical_index',
            body=query,
            scroll='2m',
            size=10000
        )

        sid = result['_scroll_id']
        scroll_size = result['hits']['total']

        while scroll_size > 0:
            for hit in result['hits']['hits']:

                # Compute intersection
                start_result = float(hit['_source']['numerical_range']['gte'])
                end_result = float(hit['_source']['numerical_range']['lte'])

                start = max(start_result, range_[0])
                end = min(end_result, range_[1])

                intersection_size += (end - start + 1)

            # scrolling
            result = es.scroll(
                scroll_id=sid,
                scroll='2m'
            )
            sid = result['_scroll_id']
            scroll_size = len(result['hits']['hits'])

    return intersection_size / total_size_att_1


# TODO: move to a better place?
def get_unionable_datasets(es, dataset_id):
    dataset_columns = get_all_datasets_columns(es)
    main_dataset_columns = dataset_columns[dataset_id]
    del dataset_columns[dataset_id]

    main_dataset_numerical_ranges = get_column_ranges(es, dataset_id)

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
                    sim = 1 - distance.jaccard(att_1.lower(), att_2.lower())
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

        # evaluate intersection for numerical attributes
        # intuition: the lower the intersection, the more the union is useful
        # TODO: how to use this?
        for i in range(len(column_pairs[dataset])):
            att_1 = column_pairs[dataset][i][0]
            att_2 = column_pairs[dataset][i][1]
            sim = column_pairs[dataset][i][2]

            intersection_size = get_intersection_size(
                es,
                main_dataset_numerical_ranges,
                dataset_id,
                att_1,
                dataset,
                att_2
            )

            column_pairs[dataset][i].append(intersection_size)

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
            URLSpec('/dataset/([^/]+)', Dataset),
            URLSpec('/join_query/([^/]+)', JoinQuery),
            URLSpec('/union_query/([^/]+)', UnionQuery),

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
