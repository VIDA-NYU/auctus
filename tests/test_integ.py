import elasticsearch
import io
import json
import jsonschema
import os
import pkg_resources
import re
import requests
import tempfile
import time
from urllib.parse import urlparse, urlunparse, parse_qs
import yaml
import zipfile

import datamart_materialize
from datamart_core.common import PrefixedElasticsearch

from .test_profile import check_ranges, check_geo_ranges, check_geohashes, \
    check_plot
from .utils import DataTestCase, data


schemas = os.path.join(os.path.dirname(__file__), '..', 'docs', 'schemas')
schemas = os.path.abspath(schemas)


# https://github.com/Julian/jsonschema/issues/343
def _fix_refs(obj, name):
    if isinstance(obj, dict):
        return {
            k: (
                _fix_refs(v, name) if k != '$ref'
                else 'file://%s/%s%s' % (schemas, name, v) if v.startswith('#')
                else 'file://%s/%s' % (schemas, v)
            )
            for k, v in obj.items()
        }
    elif isinstance(obj, list):
        return [_fix_refs(v, name) for v in obj]
    else:
        return obj


with open(os.path.join(schemas, 'query_result_schema.json')) as fp:
    result_schema = json.load(fp)
with open(os.path.join(schemas, 'restapi.yaml')) as fp:
    restapi_schema = yaml.safe_load(fp)
with open(os.path.join(schemas, 'restapi.json'), 'w') as fp:
    json.dump(restapi_schema, fp)
result_schema = _fix_refs(result_schema, 'query_result_schema.json')
restapi_schema = _fix_refs(restapi_schema, 'restapi.json')
result_list_schema = (
    restapi_schema['paths']['/search']['post']
    ['responses'][200]['content']
    ['application/json; charset=utf-8']['schema']
)
metadata_schema = (
    restapi_schema['paths']['/metadata/{dataset_id}']['get']
    ['responses'][200]['content']
    ['application/json; charset=utf-8']['schema']
)
new_session_schema = (
    restapi_schema['paths']['/session/new']['post']
    ['responses'][200]['content']
    ['application/json; charset=utf-8']['schema']
)
get_session_schema = (
    restapi_schema['paths']['/session/{session_id}']['get']
    ['responses'][200]['content']
    ['application/json; charset=utf-8']['schema']
)


class DatamartTest(DataTestCase):
    @classmethod
    def setUpClass(cls):
        cls.es = PrefixedElasticsearch()

    @classmethod
    def tearDownClass(cls):
        cls.es.close()

    def setUp(self):
        self.requests_session = requests.Session()

    def tearDown(self):
        self.requests_session.close()

    def datamart_get(self, url, **kwargs):
        return self._request('get', url, **kwargs)

    def datamart_post(self, url, **kwargs):
        return self._request('post', url, **kwargs)

    def _request(self, method, url, schema=None, check_status=True,
                 **kwargs):
        if 'files' in kwargs:
            # Read files now
            # If we retry, requests would read un-rewinded files
            files = {}
            for k, v in kwargs['files'].items():
                if isinstance(v, (list, tuple)):
                    v = (
                        v[0],
                        v[1].read() if hasattr(v[1], 'read') else v[1],
                    ) + v[2:]
                elif hasattr(v, 'read'):
                    v = v.read()
                files[k] = v
            kwargs['files'] = files

        if 'data' in kwargs:
            if hasattr(kwargs['data'], 'read'):
                kwargs['data'] = kwargs['data'].read()

        response = requests.request(
            method,
            os.environ['API_URL'] + url,
            **kwargs,
        )
        for _ in range(5):
            if response.status_code != 503:
                break
            time.sleep(0.5)
            response = requests.request(
                method,
                os.environ['API_URL'] + url,
                **kwargs
            )
        else:
            if response.status_code == 503:
                response.raise_for_status()
        if check_status:
            self.assert_response(response)
        if schema is not None:
            jsonschema.validate(response.json(), schema)
        return response

    def assert_response(self, response):
        if response.status_code == 400:  # pragma: no cover
            try:
                error = response.json()['error']
            except (KeyError, ValueError):
                error = "(not JSON)"
            self.fail("Error 400 from server: %s" % error)
        response.raise_for_status()


class TestProfiler(DatamartTest):
    def test_basic(self):
        """Check the profiler results"""
        hits = self.es.search(
            index='datasets',
            body={
                'query': {
                    'match_all': {},
                },
            },
            size=100,
        )['hits']['hits']
        hits = {h['_id']: h['_source'] for h in hits}

        expected = {
            'datamart.test.basic': basic_metadata,
            'datamart.test.geo': geo_metadata,
            'datamart.test.geo_wkt': geo_wkt_metadata,
            'datamart.test.agg': agg_metadata,
            'datamart.test.lazo': lazo_metadata,
            'datamart.test.daily': daily_metadata,
            'datamart.test.hourly': hourly_metadata,
            'datamart.test.dates_pivoted': dates_pivoted_metadata,
            'datamart.test.years_pivoted': years_pivoted_metadata,
            'datamart.test.excel': other_formats_metadata('xlsx'),
            'datamart.test.excel97': other_formats_metadata('xls'),
            'datamart.test.parquet': other_formats_metadata('parquet'),
            'datamart.test.spss': other_formats_metadata('spss'),
            'datamart.test.stata114': other_formats_metadata('stata'),
            'datamart.test.stata118': other_formats_metadata('stata'),
        }

        # Those fields are returned through the API but are not actually stored
        # (they come from temporal_coverage)
        def remove_enhanced_fields(col):
            if 'http://schema.org/DateTime' in col['semantic_types']:
                col = dict(col)
                col.pop('coverage', None)
                col.pop('temporal_resolution', None)
            return col

        expected = {
            dataset_id: dict(
                meta,
                columns=[
                    remove_enhanced_fields(col)
                    for col in meta['columns']
                ],
            )
            for dataset_id, meta in expected.items()
        }

        self.assertJson(
            hits,
            expected,
        )

    def test_alternate(self):
        """Check that the broken datasets are in the alternate index"""
        hits = self.es.search(
            index='pending',
            body={
                'query': {
                    'match_all': {},
                },
            },
        )['hits']['hits']
        hits = {h['_id']: h['_source'] for h in hits}

        self.assertJson(
            hits,
            {
                'datamart.test.empty': {
                    'status': 'error',
                    'error': "Dataset has no rows",
                    'source': 'remi',
                    'date': lambda d: isinstance(d, str),
                    'metadata': {
                        'description': "A CSV with no rows to test " +
                                       "alternate index",
                        'source': 'remi',
                        'name': 'empty',
                        'types': [],
                        'size': 28,
                        'nb_rows': 0,
                        'nb_profiled_rows': 0,
                        'nb_columns': 2,
                        'columns': [
                            {'name': 'important features'},
                            {'name': 'not here'},
                        ],
                        'materialize': {
                            'identifier': 'datamart.test',
                            'direct_url': 'http://test-discoverer:8080' +
                                          '/empty.csv',
                            'date': lambda d: isinstance(d, str),
                        },
                    },
                    'materialize': {
                        'identifier': 'datamart.test',
                        'direct_url': 'http://test-discoverer:8080/empty.csv',
                        'date': lambda d: isinstance(d, str),
                    },
                },
                'datamart.test.invalid': {
                    'status': 'error',
                    'error': 'Error profiling dataset',
                    'error_details': {
                        'exception_type': '_csv.Error',
                        'exception': 'line contains NUL',
                        'traceback': lambda s: (
                            s.startswith('Traceback') and
                            s.endswith('\n_csv.Error: line contains NUL')
                        ),
                    },
                    'metadata': {
                        'name': 'Invalid, binary',
                        'description': "Some binary data that can't be parsed",
                        'source': 'remi',
                        'materialize': {
                            'identifier': 'datamart.test',
                            'direct_url': 'http://test-discoverer:8080/invalid.bin',
                            'date': lambda d: isinstance(d, str),
                        },
                    },
                    'date': lambda d: isinstance(d, str),
                    'source': 'remi',
                    'materialize': {
                        'identifier': 'datamart.test',
                        'direct_url': 'http://test-discoverer:8080/invalid.bin',
                        'date': lambda d: isinstance(d, str),
                    },
                },
            },
        )

    def test_indexes(self):
        """Check the mapping (schema) of the indexes"""
        def hide_default_analyzers(value):
            if isinstance(value, dict):
                if value.get('analyzer') == 'default':
                    del value['analyzer']
                if value.get('search_analyzer') == 'default_search':
                    del value['search_analyzer']
                for v in value.values():
                    hide_default_analyzers(v)
            elif isinstance(value, list):
                for v in value:
                    hide_default_analyzers(v)

        response = requests.get(
            'http://' + os.environ['ELASTICSEARCH_HOSTS'].split(',')[0] +
            '/_all'
        )
        response.raise_for_status()
        actual = response.json()
        with pkg_resources.resource_stream(
                'coordinator', 'elasticsearch.yml') as stream:
            expected = yaml.safe_load(stream)
        expected.pop('_refs', None)

        # Remove 'lazo' index
        actual.pop(os.environ['ELASTICSEARCH_PREFIX'] + 'lazo', None)

        # Remove variable sections
        for index in expected.values():
            index.setdefault('aliases', {})
            hide_default_analyzers(index)
        for index in actual.values():
            hide_default_analyzers(index)
            settings = index['settings']['index']
            settings.pop('creation_date', None)
            settings.pop('number_of_replicas', None)
            settings.pop('number_of_shards', None)
            settings.pop('provided_name', None)
            settings.pop('uuid', None)
            settings.pop('version', None)
            settings.pop('blocks', None)
            settings.pop('routing', None)

        # Add custom fields
        for idx, prefix in [
            ('datasets', ''),
            ('columns', 'dataset_'),
            ('spatial_coverage', 'dataset_'),
        ]:
            props = expected[idx]['mappings']['properties']
            props[prefix + 'specialId'] = {'type': 'integer'}
            props[prefix + 'dept'] = {'type': 'keyword'}

        expected = {
            os.environ['ELASTICSEARCH_PREFIX'] + k: v
            for k, v in expected.items()
        }

        self.assertJson(actual, expected)


class TestProfileQuery(DatamartTest):
    def check_result(self, response, metadata, token, fast=False):
        # Some fields like 'name', 'description' won't be there
        metadata = {k: v for k, v in metadata.items()
                    if k not in {'id', 'name', 'description',
                                 'source', 'source_url', 'date'}}
        metadata['materialize'] = {k: v
                                   for k, v in metadata['materialize'].items()
                                   if k == 'convert'}
        # Plots are not computed, remove them too
        metadata['columns'] = [
            {k: v for k, v in col.items() if k != 'plot'}
            for col in metadata['columns']
        ]
        # Handle lazo data
        check_lazo = lambda dct: (
            dct.keys() == {'cardinality', 'hash_values', 'n_permutations'}
        )
        for i in range(len(metadata['columns'])):
            column = metadata['columns'][i]
            if (
                not fast
                and column['structural_type'] == 'http://schema.org/Text'
                and 'http://schema.org/DateTime' not in column['semantic_types']
            ):
                column['lazo'] = check_lazo
            if fast:
                metadata.pop('temporal_coverage', None)
                metadata['columns'][i] = {
                    k: v
                    for k, v in column.items()
                    if k not in ('mean', 'stddev', 'coverage')
                }

        # Expect token
        metadata['token'] = token

        self.assertJson(response.json(), metadata)

    def test_basic(self):
        """Profile the basic.csv file via the API"""
        with data('basic.csv') as basic_fp:
            response = self.datamart_post(
                '/profile',
                files={'data': basic_fp},
            )
        self.check_result(
            response,
            basic_metadata,
            'd99a8e42e65fb84e2ad800be35a8834b30828227',
        )

    def test_excel(self):
        """Profile the excel.xlsx file via the API"""
        with data('excel.xlsx') as excel_fp:
            response = self.datamart_post(
                '/profile',
                files={'data': excel_fp},
            )
        self.check_result(
            response,
            other_formats_metadata('xlsx'),
            'c6e8b9c5f634cb3b1c47b158d569a4f70462fca4',
        )

    def test_spss_fast(self):
        """Profile the spss.sav file via the API, in fast mode"""
        with data('spss.sav') as spss_fp:
            response = self.datamart_post(
                '/profile/fast',
                files={'data': spss_fp},
            )
        self.check_result(
            response,
            other_formats_metadata('spss'),
            'd9c170a8e19884d64b52948eab5871cc4b8477ec',
            fast=True,
        )


class TestSearch(DatamartTest):
    def test_basic_search_json(self):
        """Basic search, posting the query as JSON"""
        @self.do_test_basic_search
        def query():
            response = self.datamart_post(
                '/search',
                json={'keywords': ['people']},
                schema=result_list_schema,
            )
            self.assertEqual(response.request.headers['Content-Type'],
                             'application/json')
            return response

    def test_basic_search_form_urlencoded(self):
        """Basic search, posting the query as form-urlencoded"""
        @self.do_test_basic_search
        def query():
            response = self.datamart_post(
                '/search',
                data={'query': json.dumps({'keywords': ['people']})},
                schema=result_list_schema,
            )
            self.assertEqual(response.request.headers['Content-Type'],
                             'application/x-www-form-urlencoded')
            return response

    def test_basic_search_file(self):
        """Basic search, posting the query as a file in multipart/form-data"""
        @self.do_test_basic_search
        def query():
            response = self.datamart_post(
                '/search',
                files={'query': json.dumps({'keywords': ['people']})
                       .encode('utf-8')},
                schema=result_list_schema,
            )
            self.assertEqual(
                response.request.headers['Content-Type'].split(';', 1)[0],
                'multipart/form-data',
            )
            return response

    def do_test_basic_search(self, query_func):
        response = query_func()
        results = response.json()['results']
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['id'], 'datamart.test.basic')
        self.assertJson(
            results[0],
            {
                'id': 'datamart.test.basic',
                'augmentation': {
                    'type': 'none',
                    'left_columns': [],
                    'right_columns': [],
                    'left_columns_names': [],
                    'right_columns_names': []
                },
                'score': lambda n: isinstance(n, float),
                'metadata': basic_metadata,
                'd3m_dataset_description': basic_metadata_d3m('4.0.0'),
                'supplied_id': None,
                'supplied_resource_id': None
            },
        )

    def test_search_pagination(self):
        """Do basic search with pagination"""
        page_size = 2
        response = self.datamart_post(
            '/search',
            params={'page': 1, 'size': page_size},
            json={'source': ['remi']},
            schema=result_list_schema,
        )
        self.assertEqual(response.request.headers['Content-Type'],
                         'application/json')
        results = response.json()['results']
        # Got page of the requested size
        self.assertEqual(len(results), page_size)

        # Get total size
        total_pages = int(response.headers['X-Total-Pages'])
        self.assertGreater(total_pages, 1)

        # Get all pages and one extra
        all_results = set()
        for page_nb in range(2, 2 + total_pages):
            response = self.datamart_post(
                '/search',
                params={'page': page_nb, 'size': page_size},
                json={'source': ['remi']},
                schema=result_list_schema,
            )
            self.assertEqual(response.request.headers['Content-Type'],
                             'application/json')
            results = response.json()['results']
            if page_nb < total_pages:
                self.assertEqual(len(results), page_size)
            elif page_nb == total_pages:
                self.assertTrue(1 <= len(results) <= page_size)
            else:
                self.assertEqual(len(results), 0)
            self.assertEqual(
                response.headers['X-Total-Pages'],
                str(total_pages),
            )

            # No repeats
            new_ids = {r['id'] for r in results}
            self.assertFalse(new_ids & all_results)
            all_results.update(new_ids)

    def test_search_with_source(self):
        """Search restricted by source"""
        response = self.datamart_post(
            '/search',
            json={'keywords': ['people'], 'source': ['remi']},
            schema=result_list_schema,
        )
        results = response.json()['results']
        self.assertEqual(
            {r['id'] for r in results},
            {'datamart.test.basic'},
        )

        # Wrong source
        response = self.datamart_post(
            '/search',
            json={'keywords': ['people'], 'source': ['fernando']},
            schema=result_list_schema,
        )
        results = response.json()['results']
        self.assertEqual(
            {r['id'] for r in results},
            set(),
        )

        # All datasets from given source
        response = self.datamart_post(
            '/search',
            json={'source': 'fernando'},
            schema=result_list_schema,
        )
        results = response.json()['results']
        self.assertEqual(
            {r['id'] for r in results},
            {'datamart.test.agg', 'datamart.test.lazo'},
        )

    def test_search_temporal_resolution(self):
        """Search restricted on temporal resolution"""
        response = self.datamart_post(
            '/search',
            json={
                'keywords': 'daily',
                'variables': [{
                    'type': 'temporal_variable',
                    'granularity': 'hour',
                }],
            },
            schema=result_list_schema,
        )
        results = response.json()['results']
        self.assertEqual({r['id'] for r in results}, set())

        response = self.datamart_post(
            '/search',
            json={
                'keywords': 'daily',
                'variables': [{
                    'type': 'temporal_variable',
                    'granularity': 'day',
                }],
            },
            schema=result_list_schema,
        )
        results = response.json()['results']
        self.assertEqual({r['id'] for r in results}, {'datamart.test.daily'})


class TestDataSearch(DatamartTest):
    def test_basic_join(self):
        """Search for joins for basic_aug.csv (integer keys), with a query"""
        query = {'keywords': ['people']}

        with data('basic_aug.csv') as basic_aug:
            response = self.datamart_post(
                '/search',
                files={
                    'query': json.dumps(query).encode('utf-8'),
                    'data': basic_aug,
                },
                schema=result_list_schema,
            )
        results = response.json()['results']
        self.assertJson(
            results,
            [
                {
                    'id': 'datamart.test.basic',
                    'metadata': basic_metadata,
                    'd3m_dataset_description': basic_metadata_d3m('4.0.0'),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0]],
                        'left_columns_names': [['number']],
                        'right_columns': [[2]],
                        'right_columns_names': [['number']],
                        'type': 'join'
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None
                }
            ]
        )

    def test_basic_join_only_data(self):
        """Search for joins for basic_aug.csv, from only the data file"""
        with data('basic_aug.csv') as basic_aug:
            response = self.datamart_post(
                '/search',
                files={
                    'data': basic_aug,
                },
                schema=result_list_schema,
            )
        results = response.json()['results']
        self.assertJson(
            results,
            [
                {
                    'id': 'datamart.test.basic',
                    'metadata': basic_metadata,
                    'd3m_dataset_description': basic_metadata_d3m('4.0.0'),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0]],
                        'left_columns_names': [['number']],
                        'right_columns': [[2]],
                        'right_columns_names': [['number']],
                        'type': 'join'
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None
                }
            ]
        )

    def test_basic_join_only_data_csv(self):
        """Search for joins for basic_aug.csv, posting the data"""
        with data('basic_aug.csv') as basic_aug:
            response = self.datamart_post(
                '/search',
                data=basic_aug,
                headers={'Content-Type': 'text/csv'},
                schema=result_list_schema,
            )
        results = response.json()['results']
        self.assertJson(
            results,
            [
                {
                    'id': 'datamart.test.basic',
                    'metadata': basic_metadata,
                    'd3m_dataset_description': basic_metadata_d3m('4.0.0'),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0]],
                        'left_columns_names': [['number']],
                        'right_columns': [[2]],
                        'right_columns_names': [['number']],
                        'type': 'join'
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None
                }
            ]
        )

    def test_basic_join_only_profile(self):
        """Search for joins for basic_aug.csv, from only the profile"""
        with data('basic_aug.csv') as basic_aug:
            response = self.datamart_post(
                '/profile',
                files={'data': basic_aug},
            )
        profile = response.json()

        response = self.datamart_post(
            '/search',
            files={
                'data_profile': json.dumps(profile).encode('utf-8'),
            },
            schema=result_list_schema,
        )
        results = response.json()['results']
        self.assertJson(
            results,
            [
                {
                    'id': 'datamart.test.basic',
                    'metadata': basic_metadata,
                    'd3m_dataset_description': basic_metadata_d3m('4.0.0'),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0]],
                        'left_columns_names': [['number']],
                        'right_columns': [[2]],
                        'right_columns_names': [['number']],
                        'type': 'join'
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None
                }
            ]
        )

    def test_basic_join_only_token(self):
        """Search for joins for basic_aug.csv, from a token"""
        with data('basic_aug.csv') as basic_aug:
            response = self.datamart_post(
                '/profile',
                files={'data': basic_aug},
            )
        token = response.json()['token']
        self.assertEqual(len(token), 40)

        response = self.datamart_post(
            '/search',
            data={'data_profile': token},
            schema=result_list_schema,
        )
        results = response.json()['results']
        self.assertJson(
            results,
            [
                {
                    'id': 'datamart.test.basic',
                    'metadata': basic_metadata,
                    'd3m_dataset_description': basic_metadata_d3m('4.0.0'),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0]],
                        'left_columns_names': [['number']],
                        'right_columns': [[2]],
                        'right_columns_names': [['number']],
                        'type': 'join'
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None
                }
            ]
        )

    def test_both_data_profile(self):
        """Check that providing both profile and token is an error"""
        with data('basic_aug.csv') as basic_aug:
            response = self.datamart_post(
                '/profile',
                files={'data': basic_aug},
            )
            profile = response.json()

            response = self.datamart_post(
                '/search',
                files={
                    'data': basic_aug,
                    'data_profile': json.dumps(profile).encode('utf-8'),
                },
                check_status=False,
            )
            self.assertEqual(response.status_code, 400)

    def test_lazo_join(self):
        """Search for joins for lazo_aug.csv (categorical keys)"""
        with data('lazo_aug.csv') as lazo_aug:
            response = self.datamart_post(
                '/search',
                files={
                    'data': lazo_aug,
                },
                schema=result_list_schema,
            )
        results = response.json()['results']
        self.assertJson(
            results,
            [
                {
                    'id': 'datamart.test.lazo',
                    'metadata': lazo_metadata,
                    'd3m_dataset_description': lambda d: isinstance(d, dict),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0]],
                        'left_columns_names': [['favorite']],
                        'right_columns': [[0]],
                        'right_columns_names': [['dessert']],
                        'type': 'join'
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None
                }
            ]
        )

    def test_geo_union(self):
        """Search for unions for geo_aug.csv"""
        query = {'keywords': ['places']}

        with data('geo_aug.csv') as geo_aug:
            response = self.datamart_post(
                '/search',
                files={
                    'query': json.dumps(query).encode('utf-8'),
                    'data': geo_aug,
                },
                schema=result_list_schema,
            )
        results = response.json()['results']
        results = [r for r in results if r['augmentation']['type'] == 'union']
        self.assertJson(
            results,
            [
                {
                    'id': 'datamart.test.geo',
                    'metadata': geo_metadata,
                    'd3m_dataset_description': geo_metadata_d3m('4.0.0'),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0], [1], [2]],
                        'left_columns_names': [['lat'], ['long'], ['id']],
                        'right_columns': [[1], [2], [0]],
                        'right_columns_names': [['lat'], ['long'], ['id']],
                        'type': 'union'
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None
                }
            ]
        )

    def test_geo_union_only_data(self):
        """Search for unions for geo_aug.csv, from only the data file"""
        with data('geo_aug.csv') as geo_aug:
            response = self.datamart_post(
                '/search',
                files={
                    'data': geo_aug,
                },
                schema=result_list_schema,
            )
        results = response.json()['results']
        results = [r for r in results if r['augmentation']['type'] == 'union']
        self.assertJson(
            results,
            [
                {
                    'id': 'datamart.test.geo',
                    'metadata': geo_metadata,
                    'd3m_dataset_description': geo_metadata_d3m('4.0.0'),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0], [1], [2]],
                        'left_columns_names': [['lat'], ['long'], ['id']],
                        'right_columns': [[1], [2], [0]],
                        'right_columns_names': [['lat'], ['long'], ['id']],
                        'type': 'union'
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None
                }
            ]
        )

    def test_geo_join(self):
        """Search for joins for geo_aug.csv (lat,long spatial keys)"""
        with data('geo_aug.csv') as geo_aug:
            response = self.datamart_post(
                '/search',
                files={
                    'data': geo_aug,
                },
                schema=result_list_schema,
            )
        results = response.json()['results']
        results = [r for r in results if r['augmentation']['type'] == 'join']
        results = sorted(results, key=lambda r: r['id'])
        self.assertJson(
            results,
            [
                {
                    'id': 'datamart.test.geo',
                    'metadata': geo_metadata,
                    'd3m_dataset_description': geo_metadata_d3m('4.0.0'),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0, 1]],
                        'left_columns_names': [['lat', 'long']],
                        'right_columns': [[1, 2]],
                        'right_columns_names': [['lat', 'long']],
                        'type': 'join',
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None,
                },
                {
                    'id': 'datamart.test.geo_wkt',
                    'metadata': geo_wkt_metadata,
                    'd3m_dataset_description': lambda d: isinstance(d, dict),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0, 1]],
                        'left_columns_names': [['lat', 'long']],
                        'right_columns': [[1]],
                        'right_columns_names': [['coords']],
                        'type': 'join',
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None,
                }
            ],
        )

    def test_geo_join_restrict_variables(self):
        """Search for joins for geo_wkt.csv, restricting columns (spatial)"""
        query = {
            'variables': [{
                'type': 'tabular_variable',
                'columns': [0],
                'relationship': 'contains',
            }],
        }

        with data('geo_wkt.csv') as geo_wkt:
            response = self.datamart_post(
                '/search',
                files={
                    'query': json.dumps(query).encode('utf-8'),
                    'data': geo_wkt,
                },
                schema=result_list_schema,
            )
        results = response.json()['results']
        self.assertJson(
            results,
            [
                {
                    'id': 'datamart.test.geo',
                    'metadata': geo_metadata,
                    'd3m_dataset_description': geo_metadata_d3m('4.0.0'),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0]],
                        'left_columns_names': [['id']],
                        'right_columns': [[0]],
                        'right_columns_names': [['id']],
                        'type': 'join'
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None
                },
                {
                    'id': 'datamart.test.geo_wkt',
                    'metadata': geo_wkt_metadata,
                    'd3m_dataset_description': lambda d: isinstance(d, dict),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0]],
                        'left_columns_names': [['id']],
                        'right_columns': [[0]],
                        'right_columns_names': [['id']],
                        'type': 'join',
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None,
                },
            ],
        )

        query = {
            'variables': [{
                'type': 'tabular_variable',
                'columns': [2],
                'relationship': 'contains',
            }],
        }

        with data('geo_wkt.csv') as geo_wkt:
            response = self.datamart_post(
                '/search',
                files={
                    'query': json.dumps(query).encode('utf-8'),
                    'data': geo_wkt,
                },
                schema=result_list_schema,
            )
        results = response.json()['results']
        self.assertJson(
            results,
            [],
        )

    def test_temporal_daily_join(self):
        """Search for joins for daily_aug.csv (temporal keys)"""
        with data('daily_aug.csv') as daily_aug:
            response = self.datamart_post(
                '/search',
                files={
                    'data': daily_aug,
                },
                schema=result_list_schema,
            )
        results = response.json()['results']
        self.assertJson(
            results,
            [
                {
                    'id': 'datamart.test.daily',
                    'metadata': daily_metadata,
                    'd3m_dataset_description': lambda d: isinstance(d, dict),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0]],
                        'left_columns_names': [['orig_date']],
                        'right_columns': [[0]],
                        'right_columns_names':[['aug_date']],
                        'type':'join',
                        'temporal_resolution': 'day',
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None,
                },
            ],
        )

    def test_temporal_hourly_join(self):
        """Search for joins for hourly_aug.csv (temporal keys)"""
        with data('hourly_aug.csv') as hourly_aug:
            response = self.datamart_post(
                '/search',
                files={
                    'data': hourly_aug,
                },
                schema=result_list_schema,
            )
        results = response.json()['results']
        self.assertJson(
            results,
            [
                {
                    'id': 'datamart.test.hourly',
                    'metadata': hourly_metadata,
                    'd3m_dataset_description': lambda d: isinstance(d, dict),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0]],
                        'left_columns_names': [['orig_date']],
                        'right_columns': [[0]],
                        'right_columns_names': [['aug_date']],
                        'type': 'join',
                        'temporal_resolution': 'hour',
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None,
                },
            ],
        )

    def test_temporal_hourly_daily_join(self):
        """Search for joins for hourly_aug_days.csv (temporal keys)"""
        with data('hourly_aug_days.csv') as hourly_aug_days:
            response = self.datamart_post(
                '/search',
                files={
                    'data': hourly_aug_days,
                },
                schema=result_list_schema,
            )
        results = response.json()['results']
        self.assertJson(
            results,
            [
                {
                    'id': 'datamart.test.hourly',
                    'metadata': hourly_metadata,
                    'd3m_dataset_description': lambda d: isinstance(d, dict),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0]],
                        'left_columns_names': [['orig_date']],
                        'right_columns': [[0]],
                        'right_columns_names':[['aug_date']],
                        'type':'join',
                        'temporal_resolution': 'day',
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None,
                },
            ],
        )


class TestDownload(DatamartTest):
    def test_get_id(self):
        """Download datasets via GET /download/{dataset_id}"""
        # Basic dataset, materialized via direct_url
        response = self.datamart_get('/download/' + 'datamart.test.basic',
                                     # format defaults to csv
                                     allow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers['Location'],
                         'http://test-discoverer:8080/basic.csv')

        response = self.datamart_get('/download/' + 'datamart.test.basic',
                                     # explicit format
                                     params={'format': 'csv'},
                                     allow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers['Location'],
                         'http://test-discoverer:8080/basic.csv')

        response = self.datamart_get('/download/' + 'datamart.test.basic',
                                     params={'format': 'd3m'},
                                     allow_redirects=False)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'], 'application/zip')
        zip_ = zipfile.ZipFile(io.BytesIO(response.content))
        self.assertEqual(set(zip_.namelist()),
                         {'datasetDoc.json', 'tables/learningData.csv'})
        self.assertEqual(
            json.load(zip_.open('datasetDoc.json')),
            basic_metadata_d3m('4.0.0'),
        )

        response = self.datamart_get(
            '/download/' + 'datamart.test.basic',
            params={'format': 'd3m', 'format_version': '3.2.0'},
            allow_redirects=False,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'], 'application/zip')
        zip_ = zipfile.ZipFile(io.BytesIO(response.content))
        self.assertEqual(set(zip_.namelist()),
                         {'datasetDoc.json', 'tables/learningData.csv'})
        self.assertEqual(
            json.load(zip_.open('datasetDoc.json')),
            basic_metadata_d3m('3.2.0'),
        )

        # Geo dataset, materialized via /datasets storage
        response = self.datamart_get('/download/' + 'datamart.test.geo',
                                     # format defaults to csv
                                     allow_redirects=True)
        self.assertEqual(len(response.history), 1)
        self.assertEqual(response.history[0].status_code, 302)
        self.assertTrue(response.history[0].headers['Location'].startswith(
            os.environ['S3_CLIENT_URL'] + '/dev-datasets/datamart.test.geo'
        ))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'],
                         'application/octet-stream')
        self.assertTrue(response.content.startswith(b'id,lat,long,height\n'))

    def test_get_id_convert(self):
        """Download a dataset by ID, which has converters set"""
        response = self.datamart_get('/download/' + 'datamart.test.lazo')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'],
                         'application/octet-stream')
        self.assertTrue(response.content.startswith(b'dessert,year\r\n'))

    def test_post(self):
        """Download datasets via POST /download"""
        # Basic dataset, materialized via direct_url
        basic_meta = self.datamart_get(
            '/metadata/' + 'datamart.test.basic',
            schema=metadata_schema,
        )
        basic_meta = basic_meta.json()['metadata']

        response = self.datamart_post(
            '/download', allow_redirects=False,
            params={'format': 'd3m', 'format_version': '3.2.0'},
            files={
                'task': json.dumps({
                    'id': 'datamart.test.basic',
                    'score': 1.0,
                    'metadata': basic_meta
                }).encode('utf-8'),
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'], 'application/zip')
        zip_ = zipfile.ZipFile(io.BytesIO(response.content))
        self.assertEqual(set(zip_.namelist()),
                         {'datasetDoc.json', 'tables/learningData.csv'})
        self.assertEqual(
            json.load(zip_.open('datasetDoc.json')),
            basic_metadata_d3m('3.2.0'),
        )

        response = self.datamart_post(
            '/download', allow_redirects=False,
            files={
                'task': json.dumps({
                    'id': 'datamart.test.basic',
                }).encode('utf-8'),
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers['Location'],
                         'http://test-discoverer:8080/basic.csv')

        response = self.datamart_post(
            '/download', allow_redirects=False,
            params={'format': 'csv'},
            json={
                'id': 'datamart.test.basic',
                'score': 1.0,
                'metadata': basic_meta
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers['Location'],
                         'http://test-discoverer:8080/basic.csv')

        # Geo dataset, materialized via /datasets storage
        geo_meta = self.datamart_get(
            '/metadata/' + 'datamart.test.geo',
            schema=metadata_schema,
        )
        geo_meta = geo_meta.json()['metadata']

        response = self.datamart_post(
            '/download', allow_redirects=True,
            # format defaults to csv
            files={
                'task': json.dumps({
                    'id': 'datamart.test.geo',
                    'score': 1.0,
                    'metadata': geo_meta
                }).encode('utf-8'),
            },
        )
        self.assertEqual(len(response.history), 1)
        self.assertEqual(response.history[0].status_code, 302)
        self.assertTrue(response.history[0].headers['Location'].startswith(
            os.environ['S3_CLIENT_URL'] + '/dev-datasets/datamart.test.geo'
        ))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'],
                         'application/octet-stream')
        self.assertTrue(response.content.startswith(b'id,lat,long,height\n'))

        response = self.datamart_post(
            '/download', allow_redirects=False,
            params={'format': 'd3m'},
            json={
                'id': 'datamart.test.geo',
                'score': 1.0,
                'metadata': geo_meta
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'], 'application/zip')
        zip_ = zipfile.ZipFile(io.BytesIO(response.content))
        self.assertEqual(set(zip_.namelist()),
                         {'datasetDoc.json', 'tables/learningData.csv'})
        self.assertEqual(
            json.load(zip_.open('datasetDoc.json')),
            geo_metadata_d3m('4.0.0'),
        )

    def test_post_invalid(self):
        """Post invalid materialization information"""
        response = self.datamart_post(
            '/download', allow_redirects=False,
            files={
                'task': json.dumps({
                    'id': 'datamart.nonexistent',
                    'score': 0.0,
                    'metadata': {
                        'name': "Non-existent dataset",
                        'materialize': {
                            'identifier': 'datamart.nonexistent',
                        }
                    }
                }).encode('utf-8'),
            },
            check_status=False,
        )
        self.assertEqual(response.status_code, 500)
        self.assertEqual(
            response.json(),
            {'error': "Materializer reports failure"},
        )

        response = self.datamart_post(
            '/download', allow_redirects=False,
            files={},
            check_status=False,
        )
        self.assertEqual(response.status_code, 400)

    def test_get_id_invalid(self):
        """Test downloading an invalid ID gives 404"""
        response = self.datamart_get(
            '/download/datamart.nonexistent',
            check_status=False,
        )
        self.assertEqual(response.status_code, 404)

        response = self.datamart_get(
            '/metadata/datamart.nonexistent',
            check_status=False,
        )
        self.assertEqual(response.status_code, 404)

    def test_materialize(self):
        """Test datamart_materialize"""
        def assert_same_files(a, b):
            with open(a, 'r') as f_a:
                with open(b, 'r') as f_b:
                    self.assertEqual(f_a.read(), f_b.read())

        with tempfile.TemporaryDirectory() as tempdir:
            df = datamart_materialize.download(
                'datamart.test.agg',
                None,
                os.environ['API_URL'],
                'pandas',
            )
            self.assertEqual(df.shape, (8, 3))

            datamart_materialize.download(
                'datamart.test.geo',
                os.path.join(tempdir, 'geo.csv'),
                os.environ['API_URL'],
            )
            assert_same_files(
                os.path.join(tempdir, 'geo.csv'),
                os.path.join(os.path.dirname(__file__), 'data/geo.csv'),
            )

            datamart_materialize.download(
                'datamart.test.agg',
                os.path.join(tempdir, 'agg'),
                os.environ['API_URL'],
                'd3m',
            )
            assert_same_files(
                os.path.join(tempdir, 'agg/tables/learningData.csv'),
                os.path.join(os.path.dirname(__file__), 'data/agg.csv'),
            )

    def test_basic_add_index(self):
        """Test adding d3mIndex automatically"""
        response = self.datamart_get(
            '/download/' + 'datamart.test.basic',
            params={'format': 'd3m', 'format_need_d3mindex': '1'},
            allow_redirects=False,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'], 'application/zip')
        zip_ = zipfile.ZipFile(io.BytesIO(response.content))
        self.assertEqual(set(zip_.namelist()),
                         {'datasetDoc.json', 'tables/learningData.csv'})
        meta = basic_metadata_d3m('4.0.0')
        index_meta = {
            'colIndex': 0,
            'colName': 'd3mIndex',
            'colType': 'integer',
            'role': ['index'],
        }
        meta['dataResources'][0]['columns'] = [index_meta] + [
            dict(col, colIndex=col['colIndex'] + 1)
            for col in meta['dataResources'][0]['columns']
        ]
        self.assertEqual(
            json.load(zip_.open('datasetDoc.json')),
            meta,
        )
        with data('basic.d3m.csv') as f_ref:
            self.assertEqual(
                zip_.open('tables/learningData.csv').read(),
                f_ref.read(),
            )


class TestAugment(DatamartTest):
    def check_basic_join(self, response):
        self.assertEqual(response.headers['Content-Type'], 'application/zip')
        self.assertTrue(
            response.headers['Content-Disposition'].startswith('attachment')
        )
        zip_ = zipfile.ZipFile(io.BytesIO(response.content))
        zip_.testzip()
        self.assertEqual(
            set(zip_.namelist()),
            {'datasetDoc.json', 'tables/learningData.csv'},
        )
        with zip_.open('tables/learningData.csv') as table:
            self.assertCsvEqualNoOrder(
                table.read().decode('utf-8'),
                'number,desk_faces,name,color,what',
                [
                    '5,west,james,green,False',
                    '4,south,john,blue,False',
                    '7,west,michael,blue,True',
                    '6,east,robert,blue,False',
                    '11,,christopher,green,True',
                ],
            )
        with zip_.open('datasetDoc.json') as meta_fp:
            meta = json.load(meta_fp)
            self.assertJson(
                meta,
                {
                    'about': {
                        'approximateSize': '166 B',
                        'datasetID': lambda s: len(s) == 32,
                        'datasetName': lambda s: len(s) == 32,
                        'datasetSchemaVersion': '4.0.0',
                        'datasetVersion': '1.0',
                        'license': 'unknown',
                        'redacted': False,
                    },
                    'dataResources': [
                        {
                            'columns': [
                                {
                                    'colIndex': 0,
                                    'colName': 'number',
                                    'colType': 'integer',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 1,
                                    'colName': 'desk_faces',
                                    'colType': 'categorical',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 2,
                                    'colName': 'name',
                                    'colType': 'string',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 3,
                                    'colName': 'color',
                                    'colType': 'categorical',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 4,
                                    'colName': 'what',
                                    'colType': 'boolean',
                                    'role': ['attribute'],
                                },
                            ],
                            'isCollection': False,
                            'resFormat': {'text/csv': ["csv"]},
                            'resID': 'learningData',
                            'resPath': 'tables/learningData.csv',
                            'resType': 'table',
                        },
                    ],
                    'qualities': [
                        {
                            'qualName': 'augmentation_info',
                            'qualValue': {
                                'augmentation_type': 'join',
                                'nb_rows_after': 5,
                                'nb_rows_before': 5,
                                'new_columns': ['name', 'color', 'what'],
                                'removed_columns': [],
                            },
                            'qualValueType': 'dict',
                        },
                    ],
                },
            )

    def test_basic_join(self):
        """Join (integer keys)"""
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.basic',
            schema=metadata_schema,
        )
        meta = meta.json()['metadata']

        task = {
            'id': 'datamart.test.basic',
            'metadata': meta,
            'score': 1.0,
            'augmentation': {
                'left_columns': [[0]],
                'left_columns_names': [['number']],
                'right_columns': [[2]],
                'right_columns_names': [['number']],
                'type': 'join'
            },
            'supplied_id': None,
            'supplied_resource_id': None
        }

        with data('basic_aug.csv') as basic_aug:
            response = self.datamart_post(
                '/augment',
                files={
                    'task': json.dumps(task).encode('utf-8'),
                    'data': basic_aug,
                },
            )
        self.check_basic_join(response)

    def test_basic_join_data_token(self):
        """Join using a token (integer keys)"""
        # Build task dictionary
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.basic',
            schema=metadata_schema,
        )
        meta = meta.json()['metadata']
        task = {
            'id': 'datamart.test.basic',
            'metadata': meta,
            'score': 1.0,
            'augmentation': {
                'left_columns': [[0]],
                'left_columns_names': [['number']],
                'right_columns': [[2]],
                'right_columns_names': [['number']],
                'type': 'join'
            },
            'supplied_id': None,
            'supplied_resource_id': None
        }

        # Get data token
        with data('basic_aug.csv') as basic_aug:
            response = self.datamart_post(
                '/profile',
                files={
                    'data': basic_aug,
                },
            )
        self.assertTrue(
            response.headers['Content-Type'].startswith('application/json')
        )
        token = response.json()['token']
        self.assertEqual(len(token), 40)

        response = self.datamart_post(
            '/augment',
            files={
                'task': json.dumps(task).encode('utf-8'),
                'data': token.encode('ascii'),
            }
        )
        self.check_basic_join(response)

    def test_basic_join_auto(self):
        """Join automatically (no task provided, integer keys)"""
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.basic',
            schema=metadata_schema,
        )
        meta = meta.json()['metadata']

        task = {
            'id': 'datamart.test.basic',
            'metadata': meta,
            'score': 1.0,
            'augmentation': {
                'type': 'none'
            },
            'supplied_id': None,
            'supplied_resource_id': None
        }

        with data('basic_aug.csv') as basic_aug:
            response = self.datamart_post(
                '/augment',
                files={
                    'task': json.dumps(task).encode('utf-8'),
                    'data': basic_aug,
                },
            )
        self.check_basic_join(response)

    def test_agg_join(self):
        """Join and aggregate (integer keys)"""
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.agg',
            schema=metadata_schema,
        )
        meta = meta.json()['metadata']

        task = {
            'id': 'datamart.test.agg',
            'metadata': meta,
            'score': 1.0,
            'augmentation': {
                'left_columns': [[0]],
                'left_columns_names': [['id']],
                'right_columns': [[0]],
                'right_columns_names': [['id']],
                'type': 'join'
            },
            'supplied_id': None,
            'supplied_resource_id': None
        }

        with data('agg_aug.csv') as agg_aug:
            response = self.datamart_post(
                '/augment',
                params={'format': 'csv'},
                files={
                    'task': json.dumps(task).encode('utf-8'),
                    'data': agg_aug,
                },
            )
        self.assertEqual(
            response.headers['Content-Type'],
            'application/octet-stream',
        )
        self.assertTrue(
            response.headers['Content-Disposition'].startswith('attachment')
        )
        self.assertCsvEqualNoOrder(
            response.content.decode('utf-8'),
            'id,location,work,mean salary,sum salary,max salary,min salary',
            [
                '30,south korea,True,150.0,300.0,200.0,100.0',
                '40,brazil,False,,,,',
                '70,usa,True,600.0,600.0,600.0,600.0',
                '80,canada,True,200.0,200.0,200.0,200.0',
                '100,france,False,250.0,500.0,300.0,200.0',
            ],
        )

    def test_agg_join_specific_functions(self):
        """Join and aggregate (integer keys, specific functions)"""
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.agg',
            schema=metadata_schema,
        )
        meta = meta.json()['metadata']

        task = {
            'id': 'datamart.test.agg',
            'metadata': meta,
            'score': 1.0,
            'augmentation': {
                'left_columns': [[0]],
                'left_columns_names': [['id']],
                'right_columns': [[0]],
                'right_columns_names': [['id']],
                'type': 'join',
                'agg_functions': {
                    'work': 'count',
                    'salary': ['first', 'sum', 'max'],
                }
            },
            'supplied_id': None,
            'supplied_resource_id': None
        }

        with data('agg_aug.csv') as agg_aug:
            response = self.datamart_post(
                '/augment',
                files={
                    'task': json.dumps(task).encode('utf-8'),
                    'data': agg_aug,
                },
            )
        self.assertEqual(response.headers['Content-Type'], 'application/zip')
        self.assertTrue(
            response.headers['Content-Disposition'].startswith('attachment')
        )
        zip_ = zipfile.ZipFile(io.BytesIO(response.content))
        zip_.testzip()
        self.assertEqual(
            set(zip_.namelist()),
            {'datasetDoc.json', 'tables/learningData.csv'},
        )
        with zip_.open('tables/learningData.csv') as table:
            self.assertCsvEqualNoOrder(
                table.read().decode('utf-8'),
                'id,location,count work,first salary,sum salary,max salary',
                [
                    '30,south korea,2,200.0,300.0,200.0',
                    '40,brazil,1,,,',
                    '70,usa,2,,600.0,600.0',
                    '80,canada,1,200.0,200.0,200.0',
                    '100,france,2,300.0,500.0,300.0',
                ],
            )
        with zip_.open('datasetDoc.json') as meta_fp:
            meta = json.load(meta_fp)
            self.assertJson(
                meta,
                {
                    'about': {
                        'approximateSize': '197 B',
                        'datasetID': lambda s: len(s) == 32,
                        'datasetName': lambda s: len(s) == 32,
                        'datasetSchemaVersion': '4.0.0',
                        'datasetVersion': '1.0',
                        'license': 'unknown',
                        'redacted': False,
                    },
                    'dataResources': [
                        {
                            'columns': [
                                {
                                    'colIndex': 0,
                                    'colName': 'id',
                                    'colType': 'integer',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 1,
                                    'colName': 'location',
                                    'colType': 'categorical',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 2,
                                    'colName': 'count work',
                                    'colType': 'integer',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 3,
                                    'colName': 'first salary',
                                    'colType': 'integer',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 4,
                                    'colName': 'sum salary',
                                    'colType': 'real',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 5,
                                    'colName': 'max salary',
                                    'colType': 'integer',
                                    'role': ['attribute'],
                                },
                            ],
                            'isCollection': False,
                            'resFormat': {'text/csv': ["csv"]},
                            'resID': 'learningData',
                            'resPath': 'tables/learningData.csv',
                            'resType': 'table',
                        },
                    ],
                    'qualities': [
                        {
                            'qualName': 'augmentation_info',
                            'qualValue': {
                                'augmentation_type': 'join',
                                'nb_rows_after': 5,
                                'nb_rows_before': 5,
                                'new_columns': [
                                    'count work', 'first salary',
                                    'sum salary', 'max salary',
                                ],
                                'removed_columns': [],
                            },
                            'qualValueType': 'dict',
                        },
                    ],
                },
            )

    def test_lazo_join(self):
        """Join (categorical keys)"""
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.lazo',
            schema=metadata_schema,
        )
        meta = meta.json()['metadata']

        task = {
            'id': 'datamart.test.lazo',
            'metadata': meta,
            'score': 1.0,
            'augmentation': {
                'left_columns': [[0]],
                'left_columns_names': [['favorite']],
                'right_columns': [[0]],
                'right_columns_names': [['state']],
                'type': 'join'
            },
            'supplied_id': None,
            'supplied_resource_id': None
        }

        with data('lazo_aug.csv') as lazo_aug:
            response = self.datamart_post(
                '/augment',
                files={
                    'task': json.dumps(task).encode('utf-8'),
                    'data': lazo_aug,
                },
            )
        self.assertEqual(response.headers['Content-Type'], 'application/zip')
        self.assertTrue(
            response.headers['Content-Disposition'].startswith('attachment')
        )
        zip_ = zipfile.ZipFile(io.BytesIO(response.content))
        zip_.testzip()
        self.assertEqual(
            set(zip_.namelist()),
            {'datasetDoc.json', 'tables/learningData.csv'},
        )
        with zip_.open('tables/learningData.csv') as table:
            self.assertCsvEqualNoOrder(
                table.read().decode('utf-8'),
                'favorite,mean year,sum year,max year,min year',
                [
                    'Peanut Butter,1990.0,1990.0,1990.0,1990.0',
                    'Ice cream,1990.0,1990.0,1990.0,1990.0',
                    'flan,,,,',
                    'orange,1990.0,1990.0,1990.0,1990.0',
                    'kiwi,,,,',
                    'coconut,1990.0,1990.0,1990.0,1990.0',
                    'liquorICE,1990.0,1990.0,1990.0,1990.0',
                    'MACaron,1990.0,1990.0,1990.0,1990.0',
                    'pear,1990.0,1990.0,1990.0,1990.0',
                    'CANDY,1990.0,1990.0,1990.0,1990.0',
                    'pudding,1990.0,1990.0,1990.0,1990.0',
                    'doughnut,1990.0,1990.0,1990.0,1990.0',
                    'marzipan,1990.0,1990.0,1990.0,1990.0',
                    'tart,1990.0,1990.0,1990.0,1990.0',
                    'pecan pie,,,,',
                    'souffle,,,,',
                    'Pastry,1990.0,1990.0,1990.0,1990.0',
                    'banana,1990.0,1990.0,1990.0,1990.0',
                    'caramel,1991.0,1991.0,1991.0,1991.0',
                    'milkshake,1991.0,1991.0,1991.0,1991.0',
                    'Chocolate,1990.0,1990.0,1990.0,1990.0',
                    'tiramisu,1990.0,1990.0,1990.0,1990.0',
                    'tres leches,1990.0,1990.0,1990.0,1990.0',
                    'calisson,1990.0,1990.0,1990.0,1990.0',
                    'taffy,1990.0,1990.0,1990.0,1990.0',
                    'lemon,1990.0,1990.0,1990.0,1990.0',
                ],
            )
        with zip_.open('datasetDoc.json') as meta_fp:
            meta = json.load(meta_fp)
            self.assertJson(
                meta,
                {
                    'about': {
                        'approximateSize': '916 B',
                        'datasetID': lambda s: len(s) == 32,
                        'datasetName': lambda s: len(s) == 32,
                        'datasetSchemaVersion': '4.0.0',
                        'datasetVersion': '1.0',
                        'license': 'unknown',
                        'redacted': False,
                    },
                    'dataResources': [
                        {
                            'columns': [
                                {
                                    'colIndex': 0,
                                    'colName': 'favorite',
                                    'colType': 'string',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 1,
                                    'colName': 'mean year',
                                    'colType': 'real',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 2,
                                    'colName': 'sum year',
                                    'colType': 'real',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 3,
                                    'colName': 'max year',
                                    'colType': 'dateTime',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 4,
                                    'colName': 'min year',
                                    'colType': 'dateTime',
                                    'role': ['attribute'],
                                },
                            ],
                            'isCollection': False,
                            'resFormat': {'text/csv': ["csv"]},
                            'resID': 'learningData',
                            'resPath': 'tables/learningData.csv',
                            'resType': 'table',
                        },
                    ],
                    'qualities': [
                        {
                            'qualName': 'augmentation_info',
                            'qualValue': {
                                'augmentation_type': 'join',
                                'nb_rows_after': 26, 'nb_rows_before': 26,
                                'new_columns': [
                                    'mean year', 'sum year',
                                    'max year', 'min year',
                                ],
                                'removed_columns': [],
                            },
                            'qualValueType': 'dict',
                        },
                    ],
                },
            )

    def test_geo_union(self):
        """Union"""
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.geo',
            schema=metadata_schema,
        )
        meta = meta.json()['metadata']

        task = {
            'id': 'datamart.test.geo',
            'metadata': meta,
            'score': 1.0,
            'augmentation': {
                'left_columns': [[0], [1], [2]],
                'left_columns_names': [['lat'], ['long'], ['id']],
                'right_columns': [[1], [2], [0]],
                'right_columns_names': [['lat'], ['long'], ['id']],
                'type': 'union'
            },
            'supplied_id': None,
            'supplied_resource_id': None
        }

        with data('geo_aug.csv') as geo_aug:
            response = self.datamart_post(
                '/augment',
                files={
                    'task': json.dumps(task).encode('utf-8'),
                    'data': geo_aug,
                },
            )
        self.assertEqual(response.headers['Content-Type'], 'application/zip')
        self.assertTrue(
            response.headers['Content-Disposition'].startswith('attachment')
        )
        zip_ = zipfile.ZipFile(io.BytesIO(response.content))
        zip_.testzip()
        self.assertEqual(
            set(zip_.namelist()),
            {'datasetDoc.json', 'tables/learningData.csv'},
        )
        with zip_.open('tables/learningData.csv') as table:
            table_lines = table.read().decode('utf-8').splitlines(False)
            # Truncate fields to work around rounding errors
            # FIXME: Deal with rounding errors
            table_lines = [
                ','.join(e[:8] for e in line.split(','))
                for line in table_lines
            ]
            self.assertCsvEqualNoOrder(
                '\n'.join(table_lines[0:6]),
                'lat,long,id,letter',
                [
                    '40.73119,-74.0026,place100,a',
                    '40.72887,-73.9993,place101,b',
                    '40.73717,-73.9998,place102,c',
                    '40.72910,-73.9966,place103,d',
                    '40.73019,-74.0042,place104,e',
                ],
            )
        with zip_.open('datasetDoc.json') as meta_fp:
            meta = json.load(meta_fp)
            self.assertJson(
                meta,
                {
                    'about': {
                        'approximateSize': '3443 B',
                        'datasetID': lambda s: len(s) == 32,
                        'datasetName': lambda s: len(s) == 32,
                        'datasetSchemaVersion': '4.0.0',
                        'datasetVersion': '1.0',
                        'license': 'unknown',
                        'redacted': False,
                    },
                    'dataResources': [
                        {
                            'columns': [
                                {
                                    'colIndex': 0,
                                    'colName': 'lat',
                                    'colType': 'real',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 1,
                                    'colName': 'long',
                                    'colType': 'real',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 2,
                                    'colName': 'id',
                                    'colType': 'string',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 3,
                                    'colName': 'letter',
                                    'colType': 'string',
                                    'role': ['attribute'],
                                },
                            ],
                            'isCollection': False,
                            'resFormat': {'text/csv': ["csv"]},
                            'resID': 'learningData',
                            'resPath': 'tables/learningData.csv',
                            'resType': 'table',
                        },
                    ],
                    'qualities': [
                        {
                            'qualName': 'augmentation_info',
                            'qualValue': {
                                'augmentation_type': 'union',
                                'nb_rows_after': 110,
                                'nb_rows_before': 10,
                                'new_columns': [],
                                'removed_columns': [],
                            },
                            'qualValueType': 'dict',
                        },
                    ],
                },
            )

    def test_geo_join(self):
        """Join (lat,long spatial keys)"""
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.geo',
            schema=metadata_schema,
        )
        meta = meta.json()['metadata']

        task = {
            'id': 'datamart.test.geo',
            'metadata': meta,
            'score': 1.0,
            'augmentation': {
                'left_columns': [[0, 1]],
                'left_columns_names': [['lat', 'long']],
                'right_columns': [[1, 2]],
                'right_columns_names': [['lat', 'long']],
                'type': 'join'
            },
            'supplied_id': None,
            'supplied_resource_id': None
        }

        with data('geo_aug.csv') as geo_aug:
            response = self.datamart_post(
                '/augment',
                files={
                    'task': json.dumps(task).encode('utf-8'),
                    'data': geo_aug,
                },
            )
        self.assertEqual(response.headers['Content-Type'], 'application/zip')
        self.assertTrue(
            response.headers['Content-Disposition'].startswith('attachment')
        )
        zip_ = zipfile.ZipFile(io.BytesIO(response.content))
        zip_.testzip()
        self.assertEqual(
            set(zip_.namelist()),
            {'datasetDoc.json', 'tables/learningData.csv'},
        )
        with zip_.open('tables/learningData.csv') as table:
            table_lines = table.read().decode('utf-8').splitlines(False)
            # Truncate fields to work around rounding errors
            # FIXME: Deal with rounding errors
            table_lines = [
                ','.join(
                    e[:8] if e[0] < 'a' or e[0] > 'z' else e
                    for e in line.split(',')
                )
                for line in table_lines
            ]
            self.assertCsvEqualNoOrder(
                '\n'.join(table_lines[0:6]),
                'lat,long,id,letter,id_r,mean height,sum height,max height,min height',
                [
                    '40.73119,-74.0026,place100,a,'
                    + 'place08,41.41971,248.5182,69.64734,5.034845',
                    '40.72887,-73.9993,place101,b,'
                    + 'place01,43.43270,608.0579,67.62636,17.53429',
                    '40.73717,-73.9998,place102,c,'
                    + 'place06,49.46972,98.93944,50.59427,48.34517',
                    '40.72910,-73.9966,place103,d,'
                    + 'place22,53.20234,159.6070,79.72296,32.52235',
                    '40.73019,-74.0042,place104,e,'
                    + 'place02,39.79917,238.7950,51.92994,25.11753'
                ],
            )
        with zip_.open('datasetDoc.json') as meta_fp:
            meta = json.load(meta_fp)
            self.assertJson(
                meta,
                {
                    'about': {
                        'approximateSize': '939 B',
                        'datasetID': lambda s: len(s) == 32,
                        'datasetName': lambda s: len(s) == 32,
                        'datasetSchemaVersion': '4.0.0',
                        'datasetVersion': '1.0',
                        'license': 'unknown',
                        'redacted': False,
                    },
                    'dataResources': [
                        {
                            'columns': [
                                {
                                    'colIndex': 0,
                                    'colName': 'lat',
                                    'colType': 'real',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 1,
                                    'colName': 'long',
                                    'colType': 'real',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 2,
                                    'colName': 'id',
                                    'colType': 'string',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 3,
                                    'colName': 'letter',
                                    'colType': 'string',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 4,
                                    'colName': 'id_r',
                                    'colType': 'string',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 5,
                                    'colName': 'mean height',
                                    'colType': 'real',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 6,
                                    'colName': 'sum height',
                                    'colType': 'real',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 7,
                                    'colName': 'max height',
                                    'colType': 'real',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 8,
                                    'colName': 'min height',
                                    'colType': 'real',
                                    'role': ['attribute'],
                                }
                            ],
                            'isCollection': False,
                            'resFormat': {'text/csv': ["csv"]},
                            'resID': 'learningData',
                            'resPath': 'tables/learningData.csv',
                            'resType': 'table',
                        },
                    ],
                    'qualities': [
                        {
                            'qualName': 'augmentation_info',
                            'qualValue': {
                                'augmentation_type': 'join',
                                'nb_rows_after': 10,
                                'nb_rows_before': 10,
                                'new_columns': [
                                    'id_r', 'mean height', 'sum height',
                                    'max height', 'min height',
                                ],
                                'removed_columns': [],
                            },
                            'qualValueType': 'dict',
                        },
                    ],
                },
            )

    def test_temporal_daily_join(self):
        """Join (temporal keys, daily-daily)"""
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.daily',
            schema=metadata_schema,
        )
        meta = meta.json()['metadata']

        task = {
            'id': 'datamart.test.daily',
            'metadata': meta,
            'score': 1.0,
            'augmentation': {
                'left_columns': [[0]],
                'left_columns_names': [['orig_date']],
                'right_columns': [[0]],
                'right_columns_names': [['aug_date']],
                'type': 'join'
            },
            'supplied_id': None,
            'supplied_resource_id': None
        }

        with data('daily_aug.csv') as daily_aug:
            response = self.datamart_post(
                '/augment',
                files={
                    'task': json.dumps(task).encode('utf-8'),
                    'data': daily_aug,
                },
            )
        self.assertEqual(response.headers['Content-Type'], 'application/zip')
        self.assertTrue(
            response.headers['Content-Disposition'].startswith('attachment')
        )
        zip_ = zipfile.ZipFile(io.BytesIO(response.content))
        zip_.testzip()
        self.assertEqual(
            set(zip_.namelist()),
            {'datasetDoc.json', 'tables/learningData.csv'},
        )
        with zip_.open('tables/learningData.csv') as table:
            self.assertCsvEqualNoOrder(
                table.read().decode('utf-8'),
                'orig_date,n_people,rain',
                [
                    '2019-04-28,3,yes',
                    '2019-04-29,5,yes',
                    '2019-04-30,0,yes',
                    '2019-05-01,1,no',
                    '2019-05-02,3,no',
                    '2019-05-03,2,yes',
                ],
            )

    def test_temporal_hourly_join(self):
        """Join (temporal keys, hourly-hourly)"""
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.hourly',
            schema=metadata_schema,
        )
        meta = meta.json()['metadata']

        task = {
            'id': 'datamart.test.hourly',
            'metadata': meta,
            'score': 1.0,
            'augmentation': {
                'left_columns': [[0]],
                'left_columns_names': [['orig_date']],
                'right_columns': [[0]],
                'right_columns_names': [['aug_date']],
                'type': 'join'
            },
            'supplied_id': None,
            'supplied_resource_id': None
        }

        with data('hourly_aug.csv') as hourly_aug:
            response = self.datamart_post(
                '/augment',
                files={
                    'task': json.dumps(task).encode('utf-8'),
                    'data': hourly_aug,
                },
            )
        self.assertEqual(response.headers['Content-Type'], 'application/zip')
        self.assertTrue(
            response.headers['Content-Disposition'].startswith('attachment')
        )
        zip_ = zipfile.ZipFile(io.BytesIO(response.content))
        zip_.testzip()
        self.assertEqual(
            set(zip_.namelist()),
            {'datasetDoc.json', 'tables/learningData.csv'},
        )
        with zip_.open('tables/learningData.csv') as table:
            self.assertCsvEqualNoOrder(
                table.read().decode('utf-8'),
                'orig_date,color,rain',
                [
                    '2019-06-13T01:00:00,yellow,no',
                    '2019-06-13T02:00:00,yellow,no',
                    '2019-06-13T03:00:00,brown,no',
                    '2019-06-13T04:00:00,brown,yes',
                    '2019-06-13T05:00:00,yellow,no',
                ],
            )

    def test_temporal_hourly_days_join(self):
        """Join daily data with hourly (= aggregate down to daily)"""
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.hourly',
            schema=metadata_schema,
        )
        meta = meta.json()['metadata']

        task = {
            'id': 'datamart.test.hourly',
            'metadata': meta,
            'score': 1.0,
            'augmentation': {
                'left_columns': [[0]],
                'left_columns_names': [['orig_date']],
                'right_columns': [[0]],
                'right_columns_names': [['aug_date']],
                'type': 'join'
            },
            'supplied_id': None,
            'supplied_resource_id': None
        }

        with data('hourly_aug_days.csv') as hourly_aug_days:
            response = self.datamart_post(
                '/augment',
                files={
                    'task': json.dumps(task).encode('utf-8'),
                    'data': hourly_aug_days,
                },
            )
        self.assertEqual(response.headers['Content-Type'], 'application/zip')
        self.assertTrue(
            response.headers['Content-Disposition'].startswith('attachment')
        )
        zip_ = zipfile.ZipFile(io.BytesIO(response.content))
        zip_.testzip()
        self.assertEqual(
            set(zip_.namelist()),
            {'datasetDoc.json', 'tables/learningData.csv'},
        )
        with zip_.open('tables/learningData.csv') as table:
            self.assertCsvEqualNoOrder(
                table.read().decode('utf-8'),
                'orig_date,color,rain',
                [
                    '2019-06-12,pink,no',
                    '2019-06-13,grey,no',
                ],
            )

    def test_temporal_daily_hours_join(self):
        """Join hourly data with daily (= repeat for each hour)"""
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.daily',
            schema=metadata_schema,
        )
        meta = meta.json()['metadata']

        task = {
            'id': 'datamart.test.daily',
            'metadata': meta,
            'score': 1.0,
            'augmentation': {
                'left_columns': [[0]],
                'left_columns_names': [['orig_date']],
                'right_columns': [[0]],
                'right_columns_names': [['aug_date']],
                'type': 'join'
            },
            'supplied_id': None,
            'supplied_resource_id': None
        }

        with data('daily_aug_hours.csv') as daily_aug_hours:
            response = self.datamart_post(
                '/augment',
                files={
                    'task': json.dumps(task).encode('utf-8'),
                    'data': daily_aug_hours,
                },
            )
        self.assertEqual(response.headers['Content-Type'], 'application/zip')
        self.assertTrue(
            response.headers['Content-Disposition'].startswith('attachment')
        )
        zip_ = zipfile.ZipFile(io.BytesIO(response.content))
        zip_.testzip()
        self.assertEqual(
            set(zip_.namelist()),
            {'datasetDoc.json', 'tables/learningData.csv'},
        )
        with zip_.open('tables/learningData.csv') as table:
            self.assertCsvEqualNoOrder(
                table.read().decode('utf-8'),
                'orig_date,n_people,rain',
                [
                    '2019-04-25T21:00:00Z,3,yes',
                    '2019-04-26T01:00:00Z,5,no',
                    '2019-04-26T05:00:00Z,6,no',
                    '2019-04-26T09:00:00Z,7,no',
                    '2019-04-26T13:00:00Z,6,no',
                    '2019-04-26T17:00:00Z,8,no',
                    '2019-04-26T21:00:00Z,7,no',
                    '2019-04-27T01:00:00Z,0,yes',
                    '2019-04-27T05:00:00Z,1,yes',
                    '2019-04-27T09:00:00Z,0,yes',
                    '2019-04-27T13:00:00Z,3,yes',
                    '2019-04-27T17:00:00Z,0,yes',
                    '2019-04-27T13:00:00Z,0,yes',
                ],
            )


class TestUpload(DatamartTest):
    def test_upload(self):
        """Test uploading a file for ingestion"""
        response = self.datamart_post(
            '/upload',
            data={
                'address': 'http://test-discoverer:8080/basic.csv',
                'name': 'basic reupload',
                'description': "sent through upload endpoint",
                'specialId': 12,
                'dept': "internal",
            },
            schema={
                'type': 'object',
                'properties': {
                    'id': {'type': 'string'},
                },
                'required': ['id'],
                'additionalProperties': False,
            },
        )
        record = response.json()
        self.assertEqual(record.keys(), {'id'})
        dataset_id = record['id']
        self.assertTrue(dataset_id.startswith('datamart.url.'))

        try:
            # Check it's in the alternate index
            try:
                pending = self.es.get('pending', dataset_id)['_source']
                self.assertJson(
                    pending,
                    {
                        'status': 'queued',
                        'date': lambda d: isinstance(d, str),
                        'source': 'upload',
                        'metadata': {
                            'name': 'basic reupload',
                            'description': 'sent through upload endpoint',
                            'specialId': 12,
                            'dept': "internal",
                            'source': 'upload',
                            'materialize': {
                                'identifier': 'datamart.url',
                                'direct_url': 'http://test-discoverer:8080/basic.csv',
                                'date': lambda d: isinstance(d, str),
                            },
                        },
                        'materialize': {
                            'identifier': 'datamart.url',
                            'direct_url': 'http://test-discoverer:8080/basic.csv',
                            'date': lambda d: isinstance(d, str),
                        },
                    },
                )
            finally:
                # Wait for it to be indexed
                for _ in range(10):
                    try:
                        record = self.es.get('datasets', dataset_id)['_source']
                    except elasticsearch.NotFoundError:
                        pass
                    else:
                        break
                    time.sleep(2)
                else:
                    self.fail("Dataset didn't make it to index")

            self.assertJson(
                record,
                dict(
                    basic_metadata,
                    id=dataset_id,
                    name='basic reupload',
                    description="sent through upload endpoint",
                    specialId=12,
                    dept="internal",
                    source='upload',
                    materialize=dict(
                        basic_metadata['materialize'],
                        identifier='datamart.url',
                    ),
                ),
            )

            # Check it's no longer in alternate index
            time.sleep(1)
            with self.assertRaises(elasticsearch.NotFoundError):
                self.es.get('pending', dataset_id)
        finally:
            import lazo_index_service
            from datamart_core.common import delete_dataset_from_index

            time.sleep(3)  # Deleting won't work immediately
            lazo_client = lazo_index_service.LazoIndexClient(
                host=os.environ['LAZO_SERVER_HOST'],
                port=int(os.environ['LAZO_SERVER_PORT'])
            )
            delete_dataset_from_index(
                self.es,
                dataset_id,
                lazo_client,
            )

    def test_upload_human_in_the_loop(self):
        """Test uploading a file with manual annotations for ingestion"""
        with data('annotated.csv') as annotated:
            response = self.datamart_post(
                '/upload',
                files={
                    'file': annotated,
                },
                data={
                    'name': 'basic annotated features',
                    'description': "Simple CSV file sent through upload endpoint. Support type annotations made by users.",
                    'specialId': 12,
                    'dept': "internal",
                    'manual_annotations': json.dumps(annotated_annotations),
                },
                schema={
                    'type': 'object',
                    'properties': {
                        'id': {'type': 'string'},
                    },
                    'required': ['id'],
                    'additionalProperties': False,
                },
            )
            record = response.json()
            self.assertEqual(record.keys(), {'id'})
            dataset_id = record['id']
            self.assertTrue(dataset_id.startswith('datamart.upload.'))

            try:
                # Check it's in the alternate index
                try:
                    pending = self.es.get('pending', dataset_id)['_source']
                    self.assertJson(
                        pending,
                        {
                            'status': 'queued',
                            'date': lambda d: isinstance(d, str),
                            'source': 'upload',
                            'metadata': {
                                'name': 'basic annotated features',
                                'description': 'Simple CSV file sent through upload endpoint. Support type annotations made by users.',
                                'specialId': 12,
                                'dept': "internal",
                                'source': 'upload',
                                'materialize': {
                                    'identifier': 'datamart.upload',
                                    'date': lambda d: isinstance(d, str),
                                },
                                'filename': 'file',
                                'manual_annotations': annotated_annotations,
                            },
                            'materialize': {
                                'identifier': 'datamart.upload',
                                'date': lambda d: isinstance(d, str),
                            },
                        },
                    )
                finally:
                    # Wait for it to be indexed
                    for _ in range(10):
                        try:
                            record = self.es.get('datasets', dataset_id)['_source']
                        except elasticsearch.NotFoundError:
                            pass
                        else:
                            break
                        time.sleep(2)
                    else:
                        self.fail("Dataset didn't make it to index")

                self.assertJson(
                    record,
                    dict(
                        annotated_metadata,
                        id=dataset_id,
                        name='basic annotated features',
                        description="Simple CSV file sent through upload endpoint. Support type annotations made by users.",
                        specialId=12,
                        dept="internal",
                        source='upload',
                        materialize=dict(
                            annotated_metadata['materialize'],
                            identifier='datamart.upload',
                        ),
                    ),
                )

                # Check it's no longer in alternate index
                time.sleep(1)
                with self.assertRaises(elasticsearch.NotFoundError):
                    self.es.get('pending', dataset_id)
            finally:
                import lazo_index_service
                from datamart_core.common import delete_dataset_from_index

                time.sleep(3)  # Deleting won't work immediately
                lazo_client = lazo_index_service.LazoIndexClient(
                    host=os.environ['LAZO_SERVER_HOST'],
                    port=int(os.environ['LAZO_SERVER_PORT'])
                )
                delete_dataset_from_index(
                    self.es,
                    dataset_id,
                    lazo_client,
                )


class TestSession(DatamartTest):
    def test_session_new(self):
        """Test creating a system session"""
        def new_session(obj):
            response = self.datamart_post(
                '/session/new',
                json=obj,
                schema=new_session_schema,
            )
            obj = response.json()
            session_id = obj.pop('session_id')
            link_url = obj.pop('link_url')
            self.assertFalse(obj.keys())
            link_url = urlparse(link_url)
            self.assertEqual(
                urlunparse(link_url[:4] + (('',) * 2)),
                os.environ['FRONTEND_URL'] + '/',
            )
            query = parse_qs(link_url.query)
            session, = query['session']
            self.assertFalse(obj.keys())
            return session_id, json.loads(session)

        session_id, session_obj = new_session({})
        self.assertEqual(
            session_obj,
            {
                'format': 'csv',
                'format_options': {},
                'session_id': session_id,
                'system_name': 'TA3',
            },
        )

        session_id, session_obj = new_session({'data_token': 'a94a8fe5ccb19ba61c4c0873d391e987982fbbd3'})
        self.assertEqual(
            session_obj,
            {
                'format': 'csv',
                'format_options': {},
                'session_id': session_id,
                'data_token': 'a94a8fe5ccb19ba61c4c0873d391e987982fbbd3',
                'system_name': 'TA3',
            },
        )

        session_id, session_obj = new_session({
            'system_name': 'Modeler',
            'format': 'd3m',
        })
        self.assertEqual(
            session_obj,
            {
                'format': 'd3m',
                'format_options': {'need_d3mindex': False, 'version': '4.0.0'},
                'session_id': session_id,
                'system_name': 'Modeler',
            },
        )

        response = self.datamart_post(
            '/session/new',
            json={'unknown_key': 'value'},
            check_status=False,
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(),
            {'error': "Unrecognized key 'unknown_key'"},
        )

    def test_download_csv(self):
        """Test downloading a CSV into a system session"""
        session_id = self.datamart_post(
            '/session/new',
            json={'format': 'csv'},
            schema=new_session_schema,
        ).json()['session_id']

        response = self.datamart_get(
            '/download/' + 'datamart.test.basic',
            params={'session_id': session_id},
        )
        self.assertEqual(response.json(), {'success': "attached to session"})

        response = self.datamart_get(
            '/download/' + 'datamart.test.agg',
            params={'session_id': session_id},
        )
        self.assertEqual(response.json(), {'success': "attached to session"})

        response = self.datamart_get(
            '/session/' + session_id,
            schema=get_session_schema,
        )
        self.assertEqual(
            response.json(),
            {
                'results': [
                    {
                        'url': (os.environ['API_URL']
                                + '/download/datamart.test.basic'
                                + '?format=csv'),
                        'type': 'download',
                    },
                    {
                        'url': (os.environ['API_URL']
                                + '/download/datamart.test.agg'
                                + '?format=csv'),
                        'type': 'download',
                    },
                ],
            },
        )

    def test_download_d3m(self):
        """Test downloading in D3M format into a system session"""
        session_id = self.datamart_post(
            '/session/new',
            json={'format': 'd3m'},
            schema=new_session_schema,
        ).json()['session_id']

        response = self.datamart_get(
            '/download/' + 'datamart.test.basic',
            params={'session_id': session_id, 'format': 'd3m'},
        )
        self.assertEqual(response.json(), {'success': "attached to session"})

        response = self.datamart_get(
            '/download/' + 'datamart.test.agg',
            params={'session_id': session_id, 'format': 'd3m'},
        )
        self.assertEqual(response.json(), {'success': "attached to session"})

        response = self.datamart_get(
            '/session/' + session_id,
            schema=get_session_schema,
        )
        format_query = (
            'format=d3m'
            + '&format_version=4.0.0'
            + '&format_need_d3mindex=False'
        )
        self.assertEqual(
            response.json(),
            {
                'results': [
                    {
                        'url': (
                            os.environ['API_URL']
                            + '/download/datamart.test.basic'
                            + '?' + format_query
                        ),
                        'type': 'download',
                    },
                    {
                        'url': (os.environ['API_URL']
                                + '/download/datamart.test.agg'
                                + '?' + format_query),
                        'type': 'download',
                    },
                ],
            },
        )

    def test_augment_csv(self):
        """Test augmenting as a CSV into a system session"""
        session_id = self.datamart_post(
            '/session/new',
            json={'format': 'csv'},
            schema=new_session_schema,
        ).json()['session_id']

        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.basic',
            schema=metadata_schema,
        )
        meta = meta.json()['metadata']

        task = {
            'id': 'datamart.test.basic',
            'metadata': meta,
            'score': 1.0,
            'augmentation': {
                'left_columns': [[0]],
                'left_columns_names': [['number']],
                'right_columns': [[2]],
                'right_columns_names': [['number']],
                'type': 'join'
            },
            'supplied_id': None,
            'supplied_resource_id': None
        }

        with data('basic_aug.csv') as basic_aug:
            response = self.datamart_post(
                '/augment',
                params={'session_id': session_id, 'format': 'csv'},
                files={
                    'task': json.dumps(task).encode('utf-8'),
                    'data': basic_aug,
                },
            )
        self.assertEqual(response.json(), {'success': "attached to session"})

        response = self.datamart_get(
            '/session/' + session_id,
            schema=get_session_schema,
        )
        self.assertJson(
            response.json(),
            {
                'results': [
                    {
                        'url': lambda u: u.startswith(
                            os.environ['API_URL'] + '/augment/'
                        ),
                        'type': 'join',
                    },
                ],
            },
        )
        result_id = response.json()['results'][0]['url'][-40:]

        response = self.datamart_get('/augment/' + result_id)
        self.assertEqual(
            response.headers['Content-Type'],
            'application/octet-stream',
        )

    def test_augment_d3m(self):
        """Test augmenting in D3M format into a system session"""
        session_id = self.datamart_post(
            '/session/new',
            json={'format': 'd3m'},
            schema=new_session_schema,
        ).json()['session_id']

        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.basic',
            schema=metadata_schema,
        )
        meta = meta.json()['metadata']

        task = {
            'id': 'datamart.test.basic',
            'metadata': meta,
            'score': 1.0,
            'augmentation': {
                'left_columns': [[0]],
                'left_columns_names': [['number']],
                'right_columns': [[2]],
                'right_columns_names': [['number']],
                'type': 'join'
            },
            'supplied_id': None,
            'supplied_resource_id': None
        }

        with data('basic_aug.csv') as basic_aug:
            response = self.datamart_post(
                '/augment',
                params={'session_id': session_id, 'format': 'd3m'},
                files={
                    'task': json.dumps(task).encode('utf-8'),
                    'data': basic_aug,
                },
            )
        self.assertEqual(response.json(), {'success': "attached to session"})

        response = self.datamart_get(
            '/session/' + session_id,
            schema=get_session_schema,
        )
        self.assertJson(
            response.json(),
            {
                'results': [
                    {
                        'url': lambda u: u.startswith(
                            os.environ['API_URL'] + '/augment/'
                        ),
                        'type': 'join',
                    },
                ],
            },
        )
        result_id = response.json()['results'][0]['url'][-40:]

        response = self.datamart_get('/augment/' + result_id)
        self.assertEqual(
            response.headers['Content-Type'],
            'application/zip',
        )


class TestLocation(DatamartTest):
    def test_search(self):
        """Test searching for locations"""
        response = self.datamart_post(
            '/location',
            data={'q': 'Italy'},
        )
        self.assertJson(
            response.json(),
            {
                'results': [
                    {
                        'id': 3175395,
                        'name': 'Italian Republic',
                        'boundingbox': [
                            lambda n: round(n, 4) == 6.6273,
                            lambda n: round(n, 4) == 18.7845,
                            lambda n: round(n, 4) == 35.2890,
                            lambda n: round(n, 4) == 47.0921,
                        ],
                    }
                ]
            },
        )


version = os.environ['DATAMART_VERSION']
assert re.match(r'^v[0-9]+(\.[0-9]+)+(-[0-9]+-g[0-9a-f]{7,8})?$', version)


basic_metadata = {
    "id": "datamart.test.basic",
    "name": "basic",
    "description": "This is a very simple CSV with people",
    'source': 'remi',
    'types': ['categorical', 'numerical'],
    "size": 427,
    "nb_rows": 20,
    "nb_profiled_rows": 20,
    "nb_columns": 4,
    "nb_categorical_columns": 2,
    "nb_numerical_columns": 1,
    "average_row_size": lambda n: round(n, 2) == 21.35,
    "attribute_keywords": ["name", "color", "number", "what"],
    "columns": [
        {
            "name": "name",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [],
            "num_distinct_values": 20
        },
        {
            "name": "color",
            "structural_type": "http://schema.org/Text",
            "semantic_types": ["http://schema.org/Enumeration"],
            "num_distinct_values": 2,
            "plot": check_plot('histogram_categorical'),
        },
        {
            "name": "number",
            "structural_type": "http://schema.org/Integer",
            "semantic_types": [],
            "unclean_values_ratio": 0.0,
            'num_distinct_values': 5,
            "mean": lambda n: round(n, 3) == 6.150,
            "stddev": lambda n: round(n, 3) == 1.526,
            "coverage": (
                lambda l: sorted(l, key=lambda e: e['range']['gte']) == [
                    {
                        "range": {
                            "gte": 4.0,
                            "lte": 5.0
                        }
                    },
                    {
                        "range": {
                            "gte": 6.0,
                            "lte": 7.0
                        }
                    },
                ]
            ),
            "plot": check_plot('histogram_numerical'),
        },
        {
            "name": "what",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [
                "http://schema.org/Boolean",
                "http://schema.org/Enumeration"
            ],
            "unclean_values_ratio": 0.0,
            "num_distinct_values": 2,
            "plot": check_plot('histogram_categorical'),
        }
    ],
    "materialize": {
        "direct_url": "http://test-discoverer:8080/basic.csv",
        "identifier": "datamart.test",
        "date": lambda d: isinstance(d, str)
    },
    "sample": "name,color,number,what\r\njames,green,5,false\r\njohn,blue,4," +
              "false\r\nrobert,blue,6,false\r\nmichael,blue,7,true\r\nwillia" +
              "m,blue,7,true\r\ndavid,green,5,false\r\nrichard,green,7,true" +
              "\r\njoseph,blue,6,true\r\nthomas,blue,6,false\r\ncharles,blue" +
              ",7,false\r\nchristopher,green,11,true\r\ndaniel,blue,5,false" +
              "\r\nmatthew,green,7,true\r\nanthony,green,7,true\r\ndonald,bl" +
              "ue,6,true\r\nmark,blue,4,false\r\npaul,blue,4,false\r\nsteven" +
              ",blue,6,false\r\nandrew,green,6,false\r\nkenneth,green,7,true" +
              "\r\n",
    "date": lambda d: isinstance(d, str),
    "version": version
}


basic_metadata_d3m = lambda v: {
    'about': {
        'datasetID': 'datamart.test.basic',
        'datasetName': 'basic',
        'description': 'This is a very simple CSV with people',
        'license': 'unknown',
        'approximateSize': '427 B',
        'datasetSchemaVersion': v,
        'redacted': False,
        'datasetVersion': '1.0',
    },
    'dataResources': [
        {
            'resID': 'learningData',
            'resPath': 'tables/learningData.csv',
            'resType': 'table',
            'resFormat': ({'text/csv': ["csv"]} if v == '4.0.0'
                          else ['text/csv']),
            'isCollection': False,
            'columns': [
                {
                    'colIndex': 0,
                    'colName': 'name',
                    'colType': 'string',
                    'role': ['attribute'],
                },
                {
                    'colIndex': 1,
                    'colName': 'color',
                    'colType': 'categorical',
                    'role': ['attribute'],
                },
                {
                    'colIndex': 2,
                    'colName': 'number',
                    'colType': 'integer',
                    'role': ['attribute'],
                },
                {
                    'colIndex': 3,
                    'colName': 'what',
                    'colType': 'boolean',
                    'role': ['attribute'],
                },
            ],
        },
    ],
}


agg_metadata = {
    "id": "datamart.test.agg",
    "name": "agg",
    "description": "Simple CSV with ids and salaries to test aggregation for numerical attributes",
    'source': 'fernando',
    'types': ['categorical', 'numerical'],
    "size": 110,
    "nb_rows": 8,
    "nb_profiled_rows": 8,
    "nb_columns": 3,
    "nb_categorical_columns": 1,
    "nb_numerical_columns": 2,
    "average_row_size": lambda n: round(n, 2) == 13.75,
    "attribute_keywords": ["id", "work", "salary"],
    "columns": [
        {
            "name": "id",
            "structural_type": "http://schema.org/Integer",
            "semantic_types": [
                "http://schema.org/identifier"
            ],
            "unclean_values_ratio": 0.0,
            'num_distinct_values': 5,
            "mean": 65.0,
            "stddev": lambda n: round(n, 3) == 26.926,
            "coverage": (
                lambda l: sorted(l, key=lambda e: e['range']['gte']) == [
                    {
                        "range": {
                            "gte": 30.0,
                            "lte": 40.0
                        }
                    },
                    {
                        "range": {
                            "gte": 70.0,
                            "lte": 80.0
                        }
                    },
                    {
                        "range": {
                            "gte": 100.0,
                            "lte": 100.0
                        }
                    }
                ]
            ),
            "plot": check_plot('histogram_numerical'),
        },
        {
            "name": "work",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [
                "http://schema.org/Boolean",
                'http://schema.org/Enumeration',
            ],
            "unclean_values_ratio": 0.0,
            "num_distinct_values": 2,
            "plot": check_plot('histogram_categorical'),
        },
        {
            "name": "salary",
            "structural_type": "http://schema.org/Integer",
            "semantic_types": [],
            'missing_values_ratio': 0.25,
            "unclean_values_ratio": 0.0,
            'num_distinct_values': 4,
            "mean": lambda n: round(n, 2) == 266.67,
            "stddev": lambda n: round(n, 3) == 159.861,
            "coverage": (
                lambda l: sorted(l, key=lambda e: e['range']['gte']) == [
                    {
                        "range": {
                            "gte": 100.0,
                            "lte": 100.0
                        }
                    },
                    {
                        "range": {
                            "gte": 200.0,
                            "lte": 300.0
                        }
                    },
                    {
                        "range": {
                            "gte": 600.0,
                            "lte": 600.0
                        }
                    }
                ]
            ),
            "plot": check_plot('histogram_numerical'),
        }
    ],
    "materialize": {
        "identifier": "datamart.test",
        "date": lambda d: isinstance(d, str)
    },
    "sample": "id,work,salary\r\n40,false,\r\n30,true,200\r\n70,true,\r\n80," +
              "true,200\r\n100,false,300\r\n100,true,200\r\n30,false,100\r\n" +
              "70,false,600\r\n",
    "date": lambda d: isinstance(d, str),
    "version": version
}


annotated_annotations = {
    "columns": [
        {
            "coverage": [
                {
                    "range": {
                        "gte": 40.722948,
                        "lte": 40.723674
                    }
                },
                {
                    "range": {
                        "gte": 40.726559,
                        "lte": 40.730824
                    }
                },
                {
                    "range": {
                        "gte": 40.732466,
                        "lte": 40.735108
                    }
                }
            ],
            "mean": 40.729443687499995,
            "name": "lt_coord",
            "semantic_types": [
                "http://schema.org/latitude"
            ],
            "stddev": 0.0036102731149926445,
            "structural_type": "http://schema.org/Float",
            "unclean_values_ratio": 0.0,
            "latlong_pair": "1"
        },
        {
            "coverage": [
                {
                    "range": {
                        "gte": -74.005837,
                        "lte": -74.000678
                    }
                },
                {
                    "range": {
                        "gte": -74.000077,
                        "lte": -73.996833
                    }
                },
                {
                    "range": {
                        "gte": -73.993186,
                        "lte": -73.991001
                    }
                }
            ],
            "mean": -73.999644625,
            "name": "lg_coord",
            "semantic_types": [
                "http://schema.org/longitude"
            ],
            "stddev": 0.0038596233604310352,
            "structural_type": "http://schema.org/Float",
            "unclean_values_ratio": 0.0,
            "latlong_pair": "1"
        }
    ]
}


annotated_metadata = {
    "id": "datamart.upload.updatedcolumn",
    "name": "basic annotated features",
    "description": "Simple CSV file sent through upload endpoint. Support type annotations made by users.",
    "source": "upload",
    "size": 696,
    "nb_rows": 16,
    "nb_profiled_rows": 16,
    'nb_columns': 5,
    'nb_spatial_columns': 2,
    'nb_numerical_columns': 2,
    "average_row_size": lambda n: round(n, 2) == 43.5,
    "specialId": 12,
    "dept": "internal",
    "filename": "file",
    "types": ['numerical', 'spatial'],
    "manual_annotations": annotated_annotations,
    "attribute_keywords": ["id", "lt_coord", "lt", "coord",
                           "lg_coord", "lg", "coord", "height", "stmo"],
    "columns": [
        {
            "name": "id",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [],
            "num_distinct_values": 16
        },
        {
            "name": "lt_coord",
            "structural_type": "http://schema.org/Float",
            "semantic_types": lambda l: "http://schema.org/latitude" in l,
            "unclean_values_ratio": 0.0,
            "mean": lambda n: round(n, 3) == 40.729,
            "stddev": lambda n: round(n, 4) == 0.0036,
            "coverage": check_ranges(40.68, 40.78),
            "plot": check_plot('histogram_numerical'),
        },
        {
            "name": "lg_coord",
            "structural_type": "http://schema.org/Float",
            "semantic_types": lambda l: "http://schema.org/longitude" in l,
            "unclean_values_ratio": 0.0,
            "mean": lambda n: round(n, 3) == -74.000,
            "stddev": lambda n: round(n, 5) == 0.00386,
            "coverage": check_ranges(-74.05, -73.95),
            "plot": check_plot('histogram_numerical'),
        },
        {
            "name": "height",
            "structural_type": "http://schema.org/Float",
            "semantic_types": [],
            "unclean_values_ratio": 0.0,
            "mean": lambda n: round(n, 3) == 50.503,
            "stddev": lambda n: round(n, 2) == 18.75,
            "plot": check_plot('histogram_numerical'),
            "coverage": check_ranges(12.0, 86.0),
        },
        {
            "name": "stmo",
            "structural_type": "http://schema.org/Integer",
            "semantic_types": [],
            "unclean_values_ratio": 0.0,
            'num_distinct_values': 11,
            "mean": lambda n: round(n, 3) == 7.875,
            "stddev": lambda n: round(n, 2) == 3.48,
            "plot": check_plot('histogram_numerical'),
            "coverage": (
                lambda l: sorted(l, key=lambda e: e['range']['gte']) == [
                    {
                        "range": {
                            "gte": 1,
                            "lte": 4
                        },
                    },
                    {
                        "range": {
                            "gte": 5,
                            "lte": 8
                        },
                    },
                    {
                        "range": {
                            "gte": 9,
                            "lte": 12
                        },
                    },
                ]
            ),
        }
    ],
    "spatial_coverage": [
        {
            "type": "latlong",
            "column_names": ["lt_coord", "lg_coord"],
            "column_indexes": [1, 2],
            "geohashes4": check_geohashes('1211302313'),
            "ranges": check_geo_ranges(-74.006, 40.7229, -73.990, 40.7352),
            "number": 16,
        }
    ],
    "sample": "id,lt_coord,lg_coord,height,stmo\r\nplace00,40.734746,-74.000077,85.772569,10\r\n" +
              "place01,40.728026,-73.998869,58.730197,10\r\nplace02,40.728278,-74.005837,51.929949,11\r\n" +
              "place03,40.726640,-73.993186,12.730146,9\r\nplace04,40.732466,-74.004689,44.452236,5\r\n" +
              "place05,40.722948,-74.001501,42.904820,12\r\nplace06,40.735108,-73.996996,48.345170,1\r\n" +
              "place07,40.727577,-74.002853,37.459986,2\r\nplace08,40.730824,-74.002225,49.123637,4\r\n" +
              "place09,40.729115,-74.001726,40.455639,6\r\nplace10,40.734259,-73.996833,23.722705,6\r\n" +
              "place11,40.723674,-73.991001,67.692448,7\r\nplace12,40.728896,-73.998542,67.626361,8\r\n" +
              "place13,40.728711,-74.002426,84.191461,12\r\nplace14,40.733272,-73.996875,51.000673,12\r\n" +
              "place15,40.726559,-74.000678,41.906452,11\r\n",
    "materialize": {
        "identifier": "datamart.upload",
        "date": lambda d: isinstance(d, str)
    },
    "date": lambda d: isinstance(d, str),
    "version": version
}


geo_metadata = {
    "id": "datamart.test.geo",
    "name": "geo",
    "description": "Another simple CSV with places",
    "source": "remi",
    'types': ['numerical', 'spatial'],
    "size": 3910,
    "nb_rows": 100,
    "nb_profiled_rows": 100,
    "nb_columns": 4,
    "nb_spatial_columns": 2,
    "nb_numerical_columns": 1,
    "average_row_size": lambda n: round(n, 2) == 39.1,
    "attribute_keywords": ["id", "lat", "long", "height"],
    "columns": [
        {
            "name": "id",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [],
            "missing_values_ratio": 0.01,
            "num_distinct_values": 99
        },
        {
            "name": "lat",
            "structural_type": "http://schema.org/Float",
            "semantic_types": ["http://schema.org/latitude"],
            "unclean_values_ratio": 0.0,
            "mean": lambda n: round(n, 3) == 40.711,
            "stddev": lambda n: round(n, 4) == 0.0186,
            "coverage": check_ranges(40.68, 40.78),
            "plot": check_plot('histogram_numerical'),
        },
        {
            "name": "long",
            "structural_type": "http://schema.org/Float",
            "semantic_types": ["http://schema.org/longitude"],
            "unclean_values_ratio": 0.0,
            "mean": lambda n: round(n, 3) == -73.993,
            "stddev": lambda n: round(n, 5) == 0.00684,
            "coverage": check_ranges(-74.05, -73.95),
            "plot": check_plot('histogram_numerical'),
        },
        {
            "name": "height",
            "structural_type": "http://schema.org/Float",
            "semantic_types": [],
            "unclean_values_ratio": 0.0,
            "mean": lambda n: round(n, 3) == 47.827,
            "stddev": lambda n: round(n, 2) == 21.28,
            "coverage": check_ranges(1.0, 90.0),
            "plot": check_plot('histogram_numerical'),
        }
    ],
    "spatial_coverage": [
        {
            "type": "latlong",
            "column_names": ["lat", "long"],
            "column_indexes": [1, 2],
            "geohashes4": [
                {'hash': '1211302313301111', 'number': 3},
                {'hash': '1211302313301113', 'number': 1},
                {'hash': '1211302313301112', 'number': 4},
                {'hash': '1211302313301110', 'number': 2},
                {'hash': '1211302313301100', 'number': 11},
                {'hash': '1211302313301102', 'number': 6},
                {'hash': '1211302313301101', 'number': 11},
                {'hash': '1211302313301103', 'number': 4},
                {'hash': '1211302313301120', 'number': 1},
                {'hash': '1211302313301131', 'number': 1},
                {'hash': '1211302313301010', 'number': 1},
                {'hash': '1211302313301031', 'number': 1},
                {'hash': '1211302313300022', 'number': 9},
                {'hash': '1211302313300020', 'number': 8},
                {'hash': '1211302313300023', 'number': 1},
                {'hash': '1211302313123322', 'number': 3},
                {'hash': '1211302313123332', 'number': 1},
                {'hash': '1211302313211133', 'number': 25},
                {'hash': '1211302313211132', 'number': 2},
                {'hash': '1211302313211130', 'number': 1},
                {'hash': '1211302313211131', 'number': 4},
            ],
            "ranges": check_geo_ranges(-74.006, 40.6905, -73.983, 40.7352),
            "number": 100,
        }
    ],
    "materialize": {
        "identifier": "datamart.test",
        "date": lambda d: isinstance(d, str)
    },
    "sample": "id,lat,long,height\r\nplace05,40.722948,-74.001501,42.904820" +
              "\r\nplace06,40.735108,-73.996996,48.345170\r\nplace14,40.7332" +
              "72,-73.996875,51.000673\r\nplace21,40.733305,-73.999205,45.88" +
              "7002\r\nplace25,40.727810,-73.999472,35.740136\r\nplace39,40." +
              "732095,-73.996864,47.361715\r\nplace41,40.727197,-73.996098,6" +
              "2.933509\r\nplace44,40.730017,-73.993764,38.067007\r\nplace46" +
              ",40.730439,-73.996633,32.522354\r\nplace47,40.736176,-73.9985" +
              "20,50.594276\r\nplace48,40.730226,-74.001459,5.034845\r\nplac" +
              "e51,40.692165,-73.987300,67.055957\r\nplace55,40.693658,-73.9" +
              "84096,27.633986\r\nplace60,40.691525,-73.987374,70.962950\r\n" +
              "place65,40.692605,-73.986475,53.012337\r\nplace72,40.692980,-" +
              "73.987301,46.909863\r\nplace74,40.693227,-73.988686,59.675767" +
              "\r\nplace85,40.692914,-73.989237,73.357646\r\nplace87,40.6933" +
              "26,-73.984213,32.226852\r\nplace97,40.692794,-73.986984,32.89" +
              "1257\r\n",
    "date": lambda d: isinstance(d, str),
    "version": version
}


geo_metadata_d3m = lambda v: {
    'about': {
        'datasetID': 'datamart.test.geo',
        'datasetName': 'geo',
        'description': 'Another simple CSV with places',
        'license': 'unknown',
        'approximateSize': '3910 B',
        'datasetSchemaVersion': v,
        'redacted': False,
        'datasetVersion': '1.0',
    },
    'dataResources': [
        {
            'resID': 'learningData',
            'resPath': 'tables/learningData.csv',
            'resType': 'table',
            'resFormat': ({'text/csv': ["csv"]} if v == '4.0.0'
                          else ['text/csv']),
            'isCollection': False,
            'columns': [
                {
                    'colIndex': 0,
                    'colName': 'id',
                    'colType': 'string',
                    'role': ['attribute'],
                },
                {
                    'colIndex': 1,
                    'colName': 'lat',
                    'colType': 'real',
                    'role': ['attribute'],
                },
                {
                    'colIndex': 2,
                    'colName': 'long',
                    'colType': 'real',
                    'role': ['attribute'],
                },
                {
                    'colIndex': 3,
                    'colName': 'height',
                    'colType': 'real',
                    'role': ['attribute'],
                },
            ],
        },
    ],
}


geo_wkt_metadata = {
    "id": "datamart.test.geo_wkt",
    "name": "geo_wkt",
    "description": "Simple CSV in WKT (https://en.wikipedia.org/wiki/Well-know"
                   + "n_text_representation_of_geometry) format",
    'source': 'remi',
    'types': ['numerical', 'spatial'],
    "size": 4708,
    "nb_rows": 100,
    "nb_profiled_rows": 100,
    "nb_columns": 3,
    "nb_spatial_columns": 1,
    "nb_numerical_columns": 1,
    "average_row_size": lambda n: round(n, 2) == 47.08,
    "attribute_keywords": ["id", "coords", "height"],
    "columns": [
        {
            "name": "id",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [],
            "missing_values_ratio": 0.01,
            "num_distinct_values": 99
        },
        {
            "name": "coords",
            "structural_type": "http://schema.org/GeoCoordinates",
            "semantic_types": [],
            "unclean_values_ratio": 0.0,
            "point_format": "long,lat",
        },
        {
            "name": "height",
            "structural_type": "http://schema.org/Float",
            "semantic_types": [],
            "unclean_values_ratio": 0.0,
            "mean": lambda n: round(n, 3) == 47.827,
            "stddev": lambda n: round(n, 2) == 21.28,
            "coverage": check_ranges(1.0, 90.0),
            "plot": check_plot('histogram_numerical'),
        }
    ],
    "spatial_coverage": [
        {
            "type": "point",
            "column_names": ["coords"],
            "column_indexes": [1],
            "geohashes4": [
                {'hash': '1211302313301111', 'number': 3},
                {'hash': '1211302313301113', 'number': 1},
                {'hash': '1211302313301112', 'number': 4},
                {'hash': '1211302313301110', 'number': 2},
                {'hash': '1211302313301100', 'number': 11},
                {'hash': '1211302313301102', 'number': 6},
                {'hash': '1211302313301101', 'number': 11},
                {'hash': '1211302313301103', 'number': 4},
                {'hash': '1211302313301120', 'number': 1},
                {'hash': '1211302313301131', 'number': 1},
                {'hash': '1211302313301010', 'number': 1},
                {'hash': '1211302313301031', 'number': 1},
                {'hash': '1211302313300022', 'number': 9},
                {'hash': '1211302313300020', 'number': 8},
                {'hash': '1211302313300023', 'number': 1},
                {'hash': '1211302313123322', 'number': 3},
                {'hash': '1211302313123332', 'number': 1},
                {'hash': '1211302313211133', 'number': 25},
                {'hash': '1211302313211132', 'number': 2},
                {'hash': '1211302313211130', 'number': 1},
                {'hash': '1211302313211131', 'number': 4},
            ],
            "ranges": check_geo_ranges(-74.006, 40.6905, -73.983, 40.7352),
            "number": 100,
        }
    ],
    "materialize": {
        "identifier": "datamart.test",
        "date": lambda d: isinstance(d, str),
        "direct_url": "http://test-discoverer:8080/geo_wkt.csv",
    },
    "sample": "id,coords,height\r\nplace05,POINT (-74.001501 40.722948),42.9" +
              "04820\r\nplace06,POINT (-73.996996 40.735108),48.345170\r\npl" +
              "ace14,POINT (-73.996875 40.733272),51.000673\r\nplace21,POINT" +
              " (-73.999205 40.733305),45.887002\r\nplace25,POINT (-73.99947" +
              "2 40.727810),35.740136\r\nplace39,POINT (-73.996864 40.732095" +
              "),47.361715\r\nplace41,POINT (-73.996098 40.727197),62.933509" +
              "\r\nplace44,POINT (-73.993764 40.730017),38.067007\r\nplace46" +
              ",POINT (-73.996633 40.730439),32.522354\r\nplace47,POINT (-73" +
              ".998520 40.736176),50.594276\r\nplace48,POINT (-74.001459 40." +
              "730226),5.034845\r\nplace51,POINT (-73.987300 40.692165),67.0" +
              "55957\r\nplace55,POINT (-73.984096 40.693658),27.633986\r\npl" +
              "ace60,POINT (-73.987374 40.691525),70.962950\r\nplace65,POINT" +
              " (-73.986475 40.692605),53.012337\r\nplace72,POINT (-73.98730" +
              "1 40.692980),46.909863\r\nplace74,POINT (-73.988686 40.693227" +
              "),59.675767\r\nplace85,POINT (-73.989237 40.692914),73.357646" +
              "\r\nplace87,POINT (-73.984213 40.693326),32.226852\r\nplace97" +
              ",POINT (-73.986984 40.692794),32.891257\r\n",
    "date": lambda d: isinstance(d, str),
    "version": version
}


lazo_metadata = {
    'id': 'datamart.test.lazo',
    "name": "lazo",
    "description": "Simple CSV with states and years to test the Lazo index service",
    'source': 'fernando',
    'types': ['temporal'],
    "size": 523,
    "nb_rows": 36,
    "nb_profiled_rows": 36,
    "nb_columns": 2,
    "nb_temporal_columns": 1,
    "average_row_size": lambda n: round(n, 2) == 14.53,
    "attribute_keywords": ["dessert", "year"],
    "columns": [
        {
            "name": "dessert",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [],
            "missing_values_ratio": lambda n: round(n, 4) == 0.0278,
            "num_distinct_values": 35,
        },
        {
            "name": "year",
            "structural_type": "http://schema.org/Text",
            "semantic_types": ["http://schema.org/DateTime"],
            "unclean_values_ratio": 0.0,
            'num_distinct_values': 2,
            "plot": check_plot('histogram_temporal'),
            "coverage": [
                {'range': {'gte': 631152000.0, 'lte': 631152000.0}},
                {'range': {'gte': 662688000.0, 'lte': 662688000.0}},
            ],
            "temporal_resolution": "year",
        }
    ],
    "temporal_coverage": [
        {
            'type': 'datetime',
            'column_names': ['year'],
            'column_indexes': [1],
            'column_types': ['http://schema.org/DateTime'],
            'ranges': [
                {'range': {'gte': 631152000.0, 'lte': 631152000.0}},
                {'range': {'gte': 662688000.0, 'lte': 662688000.0}},
            ],
            'temporal_resolution': 'year',
        },
    ],
    "materialize": {
        "identifier": "datamart.test",
        "date": lambda d: isinstance(d, str),
        "convert": [
            {'identifier': 'tsv', 'separator': '\t'},
            {'identifier': 'skip_rows', 'nb_rows': 2},
        ],
    },
    "sample": "dessert,year\r\ncandy,1990\r\ncookie,1990\r\npastry,1990\r\nj" +
              "ello,1990\r\napple,1990\r\nbanana,1990\r\nfruitcake,1990\r\no" +
              "range,1990\r\npetit four,1990\r\npop tart,1990\r\n,1990\r\nno" +
              "ugat,1990\r\nmarzipan,1990\r\nlemon,1990\r\nmacaron,1990\r\ng" +
              "ingerbread,1990\r\neclair,1990\r\nprofiterole,1990\r\ncaramel" +
              ",1991\r\nmilkshake,1991\r\n",
    "date": lambda d: isinstance(d, str),
    "version": version
}


daily_metadata = {
    'id': 'datamart.test.daily',
    'name': 'daily',
    'description': 'Temporal dataset with daily resolution',
    'source': 'remi',
    'types': ['categorical', 'temporal'],
    'size': 388,
    'nb_rows': 30,
    "nb_profiled_rows": 30,
    "nb_columns": 2,
    "nb_temporal_columns": 1,
    "nb_categorical_columns": 1,
    "average_row_size": lambda n: round(n, 2) == 12.93,
    "attribute_keywords": ["aug_date", "aug", "date", "rain"],
    'columns': [
        {
            'name': 'aug_date',
            'structural_type': 'http://schema.org/Text',
            'semantic_types': [
                'http://schema.org/DateTime',
            ],
            'unclean_values_ratio': 0.0,
            'num_distinct_values': 30,
            "plot": check_plot('histogram_temporal'),
            'coverage': [
                {'range': {'gte': 1555977600.0, 'lte': 1556755200.0}},
                {'range': {'gte': 1556841600.0, 'lte': 1557619200.0}},
                {'range': {'gte': 1557705600.0, 'lte': 1558483200.0}},
            ],
            'temporal_resolution': 'day',
        },
        {
            'name': 'rain',
            'structural_type': 'http://schema.org/Text',
            'semantic_types': [
                'http://schema.org/Boolean',
                'http://schema.org/Enumeration',
            ],
            'unclean_values_ratio': 0.0,
            'num_distinct_values': 2,
            "plot": check_plot('histogram_categorical'),
        },
    ],
    'temporal_coverage': [
        {
            'type': 'datetime',
            'column_names': ['aug_date'],
            'column_indexes': [0],
            'column_types': ['http://schema.org/DateTime'],
            'ranges': [
                {'range': {'gte': 1555977600.0, 'lte': 1556841600.0}},
                {'range': {'gte': 1556928000.0, 'lte': 1557705600.0}},
                {'range': {'gte': 1557792000.0, 'lte': 1558483200.0}},
            ],
            'temporal_resolution': 'day',
        },
    ],
    'materialize': {
        'identifier': 'datamart.test',
        'date': lambda d: isinstance(d, str),
    },
    'sample': "aug_date,rain\r\n20190423,no\r\n20190425,yes\r\n20190426,no\r" +
              "\n20190429,yes\r\n20190502,no\r\n20190503,yes\r\n20190505,yes" +
              "\r\n20190507,no\r\n20190508,yes\r\n20190509,yes\r\n20190510,n" +
              "o\r\n20190513,no\r\n20190514,no\r\n20190516,no\r\n20190517,ye" +
              "s\r\n20190518,no\r\n20190519,yes\r\n20190520,no\r\n20190521,n" +
              "o\r\n20190522,yes\r\n",
    'date': lambda d: isinstance(d, str),
    'version': version,
}


hourly_metadata = {
    'id': 'datamart.test.hourly',
    'name': 'hourly',
    'description': 'Temporal dataset with hourly resolution',
    'source': 'remi',
    'types': ['categorical', 'temporal'],
    'size': 1242,
    'nb_rows': 52,
    'nb_profiled_rows': 52,
    'nb_columns': 2,
    'nb_temporal_columns': 1,
    'nb_categorical_columns': 1,
    'average_row_size': lambda n: round(n, 2) == 23.88,
    'attribute_keywords': ['aug_date', 'aug', 'date', 'rain'],
    'columns': [
        {
            'name': 'aug_date',
            'structural_type': 'http://schema.org/Text',
            'semantic_types': [
                'http://schema.org/DateTime',
            ],
            'num_distinct_values': 52,
            "plot": check_plot('histogram_temporal'),
            'coverage': [
                {'range': {'gte': 1560297600.0, 'lte': 1560358784.0}},
                {'range': {'gte': 1560362368.0, 'lte': 1560419968.0}},
                {'range': {'gte': 1560423552.0, 'lte': 1560481152.0}},
            ],
            'temporal_resolution': 'hour',
        },
        {
            'name': 'rain',
            'structural_type': 'http://schema.org/Text',
            'semantic_types': [
                'http://schema.org/Boolean',
                'http://schema.org/Enumeration',
            ],
            'unclean_values_ratio': 0.0,
            'num_distinct_values': 2,
            "plot": check_plot('histogram_categorical'),
        },
    ],
    'temporal_coverage': [
        {
            'type': 'datetime',
            'column_names': ['aug_date'],
            'column_indexes': [0],
            'column_types': ['http://schema.org/DateTime'],
            'ranges': [
                {'range': {'gte': 1560297600.0, 'lte': 1560358784.0}},
                {'range': {'gte': 1560362368.0, 'lte': 1560419968.0}},
                {'range': {'gte': 1560423552.0, 'lte': 1560481152.0}},
            ],
            'temporal_resolution': 'hour',
        },
    ],
    'materialize': {
        'direct_url': 'http://test-discoverer:8080/hourly.csv',
        'identifier': 'datamart.test',
        'date': lambda d: isinstance(d, str),
    },
    'sample': "aug_date,rain\r\n2019-06-12T01:00:00,no\r\n2019-06-12T02:00:0" +
              "0,no\r\n2019-06-12T03:00:00,yes\r\n2019-06-12T09:00:00,no\r\n" +
              "2019-06-12T10:00:00,yes\r\n2019-06-12T11:00:00,yes\r\n2019-06" +
              "-12T12:00:00,yes\r\n2019-06-12T14:00:00,yes\r\n2019-06-12T15:" +
              "00:00,no\r\n2019-06-12T20:00:00,yes\r\n2019-06-12T21:00:00,ye" +
              "s\r\n2019-06-13T01:00:00,no\r\n2019-06-13T03:00:00,no\r\n2019" +
              "-06-13T05:00:00,no\r\n2019-06-13T07:00:00,yes\r\n2019-06-13T1" +
              "0:00:00,yes\r\n2019-06-13T14:00:00,yes\r\n2019-06-13T17:00:00" +
              ",yes\r\n2019-06-14T00:00:00,yes\r\n2019-06-14T01:00:00,yes\r\n",
    'date': lambda d: isinstance(d, str),
    'version': version,
}


dates_pivoted_metadata = {
    'id': 'datamart.test.dates_pivoted',
    'name': 'dates pivoted',
    'description': 'Temporal dataset but in columns',
    'source': 'remi',
    'types': ['categorical', 'temporal'],
    'size': 511,
    'nb_rows': 24,
    'nb_profiled_rows': 24,
    'nb_columns': 3,
    'nb_temporal_columns': 1,
    'nb_categorical_columns': 2,
    'average_row_size': lambda n: round(n, 2) == 21.29,
    'attribute_keywords': ['color', 'date', 'value'],
    'columns': [
        {
            'name': 'color',
            'structural_type': 'http://schema.org/Text',
            'semantic_types': [
                'http://schema.org/Enumeration',
            ],
            'num_distinct_values': 2,
            'plot': check_plot('histogram_categorical'),
        },
        {
            'name': 'date',
            'structural_type': 'http://schema.org/Text',
            'semantic_types': [
                'http://schema.org/DateTime',
            ],
            'num_distinct_values': 12,
            'plot': check_plot('histogram_temporal'),
        },
        {
            'name': 'value',
            'structural_type': 'http://schema.org/Text',
            'unclean_values_ratio': 0.0,
            'semantic_types': [
                'http://schema.org/Boolean',
                'http://schema.org/Enumeration',
            ],
            'num_distinct_values': 2,
            'plot': check_plot('histogram_categorical'),
        },
    ],
    'temporal_coverage': [
        {
            'type': 'datetime',
            'column_names': ['date'],
            'column_indexes': [1],
            'column_types': ['http://schema.org/DateTime'],
            'ranges': [
                {'range': {'gte': 1325376000.0, 'lte': 1333238400.0}},
                {'range': {'gte': 1335830400.0, 'lte': 1343779200.0}},
                {'range': {'gte': 1346457600.0, 'lte': 1354320000.0}},
            ],
            'temporal_resolution': 'month',
        },
    ],
    'materialize': {
        'direct_url': 'http://test-discoverer:8080/dates_pivoted.csv',
        'identifier': 'datamart.test',
        'date': lambda d: isinstance(d, str),
        'convert': [
            {'identifier': 'pivot', 'except_columns': [0], 'date_label': 'date'},
        ],
    },
    'sample': "color,date,value\r\ngreen,2012-01-01,yes\r\ngreen,2012-02-01," +
              "no\r\ngreen,2012-03-01,no\r\ngreen,2012-04-01,yes\r\ngreen,20" +
              "12-06-01,yes\r\ngreen,2012-07-01,yes\r\ngreen,2012-08-01,yes" +
              "\r\ngreen,2012-09-01,yes\r\ngreen,2012-10-01,no\r\ngreen,2012" +
              "-11-01,no\r\nred,2012-01-01,no\r\nred,2012-03-01,yes\r\nred,2" +
              "012-04-01,yes\r\nred,2012-05-01,no\r\nred,2012-06-01,no\r\nre" +
              "d,2012-07-01,no\r\nred,2012-09-01,no\r\nred,2012-10-01,yes\r" +
              "\nred,2012-11-01,yes\r\nred,2012-12-01,no\r\n",
    'date': lambda d: isinstance(d, str),
    'version': version
}


years_pivoted_metadata = {
    'id': 'datamart.test.years_pivoted',
    'name': 'years pivoted',
    'description': 'Temporal dataset but in columns',
    'source': 'remi',
    'types': ['categorical', 'temporal'],
    'size': 367,
    'nb_rows': 24,
    'nb_profiled_rows': 24,
    'nb_columns': 3,
    'nb_temporal_columns': 1,
    'nb_categorical_columns': 2,
    'average_row_size': lambda n: round(n, 2) == 15.29,
    'attribute_keywords': ['color', 'year', 'value'],
    'columns': [
        {
            'name': 'color',
            'structural_type': 'http://schema.org/Text',
            'semantic_types': [
                'http://schema.org/Enumeration',
            ],
            'num_distinct_values': 2,
            'plot': check_plot('histogram_categorical'),
        },
        {
            'name': 'year',
            'structural_type': 'http://schema.org/Text',
            'semantic_types': [
                'http://schema.org/DateTime',
            ],
            'unclean_values_ratio': 0.0,
            'num_distinct_values': 12,
            'plot': check_plot('histogram_temporal'),
        },
        {
            'name': 'value',
            'structural_type': 'http://schema.org/Text',
            'unclean_values_ratio': 0.0,
            'semantic_types': [
                'http://schema.org/Boolean',
                'http://schema.org/Enumeration',
            ],
            'num_distinct_values': 2,
            'plot': check_plot('histogram_categorical'),
        },
    ],
    'temporal_coverage': [
        {
            'type': 'datetime',
            'column_names': ['year'],
            'column_indexes': [1],
            'column_types': ['http://schema.org/DateTime'],
            'ranges': [
                {'range': {'gte': 1136073600.0, 'lte': 1230768000.0}},
                {'range': {'gte': 1262304000.0, 'lte': 1356998400.0}},
                {'range': {'gte': 1388534400.0, 'lte': 1483228800.0}},
            ],
            'temporal_resolution': 'year',
        },
    ],
    'materialize': {
        'direct_url': 'http://test-discoverer:8080/years_pivoted.csv',
        'identifier': 'datamart.test',
        'date': lambda d: isinstance(d, str),
        'convert': [
            {'identifier': 'pivot', 'except_columns': [0], 'date_label': 'year'},
        ],
    },
    'sample': "color,year,value\r\ngreen,2006,yes\r\ngreen,2007,no\r\ngreen," +
              "2008,no\r\ngreen,2009,yes\r\ngreen,2011,yes\r\ngreen,2012,yes" +
              "\r\ngreen,2013,yes\r\ngreen,2014,yes\r\ngreen,2015,no\r\ngree" +
              "n,2016,no\r\nred,2006,no\r\nred,2008,yes\r\nred,2009,yes\r\nr" +
              "ed,2010,no\r\nred,2011,no\r\nred,2012,no\r\nred,2014,no\r\nre" +
              "d,2015,yes\r\nred,2016,yes\r\nred,2017,no\r\n",
    'date': lambda d: isinstance(d, str),
    'version': version
}


other_formats_metadata = lambda fmt: {
    'id': lambda v: isinstance(v, str),
    'name': lambda v: isinstance(v, str),
    'description': lambda v: isinstance(v, str),
    'source': 'remi',
    'source_url': lambda s: isinstance(s, str),
    'types': ['numerical', 'temporal'],
    'size': 130,
    'nb_rows': 4,
    'nb_profiled_rows': 4,
    'nb_columns': 3,
    'nb_temporal_columns': 1,
    'nb_numerical_columns': 1,
    'average_row_size': lambda n: round(n, 2) == 32.5,
    'attribute_keywords': ['name', 'age', 'date'],
    'columns': [
        {
            'name': 'name',
            'structural_type': 'http://schema.org/Text',
            'semantic_types': [],
            'num_distinct_values': 4,
        },
        {
            'name': 'age',
            'structural_type': 'http://schema.org/Integer',
            'semantic_types': [],
            'unclean_values_ratio': 0.0,
            'num_distinct_values': 4,
            'mean': lambda n: round(n, 2) == 26.0,
            'stddev': lambda n: round(n, 2) == 10.61,
            'coverage': (
                lambda l: sorted(l, key=lambda e: e['range']['gte']) == [
                    {
                        'range': {
                            'gte': 9.0,
                            'lte': 9.0,
                        },
                    },
                    {
                        'range': {
                            'gte': 27.0,
                            'lte': 30.0,
                        },
                    },
                    {
                        'range': {
                            'gte': 38.0,
                            'lte': 38.0,
                        },
                    },
                ]
            ),
            'plot': check_plot('histogram_numerical'),
        },
        {
            'name': 'date',
            'structural_type': 'http://schema.org/Text',
            'semantic_types': ['http://schema.org/DateTime'],
            'num_distinct_values': 4,
            'plot': check_plot('histogram_temporal'),
        },
    ],
    'temporal_coverage': [
        {
            'type': 'datetime',
            'column_names': ['date'],
            'column_indexes': [2],
            'column_types': ['http://schema.org/DateTime'],
            'ranges': [
                {'range': {'gte': 473385600.0, 'lte': 473385600.0}},
                {'range': {'gte': 631152000.0, 'lte': 725846400.0}},
                {'range': {'gte': 1278460800.0, 'lte': 1278460800.0}},
            ],
            'temporal_resolution': 'year',
        },
    ],
    'materialize': {
        'direct_url': lambda v: isinstance(v, str),
        'identifier': 'datamart.test',
        'date': lambda d: isinstance(d, str),
        'convert': [{'identifier': fmt}],
    },
    'sample': 'name,age,date\r\nC++,38,1985-01-01T00:00:00\r\nPython,30,1990' +
              '-01-01T00:00:00\r\nRust,9,2010-07-07T00:00:00\r\nLua,27,1993-' +
              '01-01T00:00:00\r\n',
    'date': lambda d: isinstance(d, str),
    'version': version
}
