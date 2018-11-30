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

    result = es.search(index='datamart_numerical_index', body=index_query)

    if result['hits']['total'] > 0:
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
            total_size += (range_[1] - range_[0])
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
            result = es.search(index='datamart_numerical_index', body=query)
            if result['hits']['total'] == 0:
                continue
            for hit in result['hits']['hits']:

                name = '%s$$%s' % (hit['_source']['id'], hit['_source']['name'])
                if name not in intersections_column:
                    intersections_column[name] = 0

                # Compute intersection
                start_result = float(hit['_source']['numerical_range']['gte'])
                end_result = float(hit['_source']['numerical_range']['lte'])

                start = max(start_result, range_[0])
                end = min(end_result, range_[1])

                intersections_column[name] += (end - start)

        if type_ == 'integer':
            types[column] = 'http://schema.org/Integer'
        elif type_ == 'float':
            types[column] = 'http://schema.org/Float'
        else:
            types[column] = 'http://schema.org/DateTime'
        intersections[column] = [
            (name, size/total_size) for name, size in sorted(
                intersections_column.items(),
                key=lambda item: item[1],
                reverse=True
            )
        ]

    return intersections, types


class JoinQuery(BaseHandler):
    def get(self, dataset_id):
        join_intersections, column_types = get_numerical_range_intersections(
            self.application.elasticsearch,
            dataset_id
        )
        # for column in join_intersections:
        #     logger.warning("[JOIN] Column: " + column)
        #     for intersection in join_intersections[column][:5]:
        #         dataset_j, column_j = intersection[0].split('$$')
        #         logger.warning("[JOIN]   Intersects %s, %s" % (dataset_j, column_j))
        #         logger.warning("[JOIN]   > Size: %.2f" % intersection[1])
        self.render('join_query.html',
                    intersections=join_intersections,
                    types=column_types)


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
