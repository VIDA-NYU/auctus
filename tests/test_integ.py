import elasticsearch
import io
import json
import jsonschema
import os
import re
import requests
import tempfile
import time
import zipfile

import datamart_materialize

from .utils import DataTestCase


schemas = os.path.join(os.path.dirname(__file__), '..', 'doc', 'schemas')
schemas = os.path.abspath(schemas)


# https://github.com/Julian/jsonschema/issues/343
def _fix_refs(obj, name):
    if isinstance(obj, dict):
        return {
            k: _fix_refs(v, name) if k != '$ref' else 'file://%s/%s%s' % (schemas, name, v)
            for k, v in obj.items()
        }
    elif isinstance(obj, list):
        return [_fix_refs(v, name) for v in obj]
    else:
        return obj


with open(os.path.join(schemas, 'query_result_schema.json')) as fp:
    result_schema = json.load(fp)
result_schema = _fix_refs(result_schema, 'query_result_schema.json')
result_list_schema = {
    'type': 'object',
    'properties': {
        'results': {'type': 'array', 'items': result_schema}
    },
    'definitions': result_schema.pop('definitions'),
}


class DatamartTest(DataTestCase):
    def datamart_get(self, url, **kwargs):
        return self._request('get', url, **kwargs)

    def datamart_post(self, url, **kwargs):
        return self._request('post', url, **kwargs)

    def _request(self, method, url, schema=None, check_status=True, **kwargs):
        response = requests.request(
            method,
            os.environ['QUERY_HOST'] + url,
            **kwargs
        )
        for _ in range(5):
            if response.status_code != 503:
                break
            time.sleep(0.5)
            response = requests.request(
                method,
                os.environ['QUERY_HOST'] + url,
                **kwargs
            )
        else:
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


class TestProfiler(DataTestCase):
    def test_basic(self):
        es = elasticsearch.Elasticsearch(
            os.environ['ELASTICSEARCH_HOSTS'].split(',')
        )
        hits = es.search(
            index='datamart',
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
                'datamart.test.basic': basic_metadata,
                'datamart.test.geo': geo_metadata,
                'datamart.test.agg': agg_metadata,
                'datamart.test.lazo': lazo_metadata,
                'datamart.test.daily': daily_metadata,
                'datamart.test.hourly': hourly_metadata,
            },
        )


class TestProfileQuery(DatamartTest):
    def test_basic(self):
        basic_path = os.path.join(
            os.path.dirname(__file__),
            'data', 'basic.csv',
        )
        with open(basic_path, 'rb') as basic_fp:
            response = self.datamart_post(
                '/profile',
                files={'data': basic_fp}
            )
        metadata = {k: v for k, v in basic_metadata.items()
                    if k not in {'name', 'description', 'date', 'materialize'}}
        metadata = dict(
            metadata,
            lazo=lambda lazo: (
                isinstance(lazo, list) and
                all(e.keys() == {'cardinality', 'hash_values',
                                 'n_permutations', 'name'}
                    for e in lazo) and
                {e['name'] for e in lazo} == {'name', 'country', 'what'}
            ),
        )
        self.assertJson(response.json(), metadata)


class TestSearch(DatamartTest):
    def test_basic_search_json(self):
        """Basic search, posting the query as JSON."""
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

    def test_basic_search_formdata(self):
        """Basic search, posting the query as formdata-urlencoded."""
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
        """Basic search, posting the query as a file in multipart/form-data."""
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


class TestDataSearch(DatamartTest):
    def test_basic_join(self):
        query = {'keywords': ['people']}

        response = self.datamart_post(
            '/search',
            files={
                'query': json.dumps(query).encode('utf-8'),
                'data': basic_aug_data.encode('utf-8'),
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
        response = self.datamart_post(
            '/search',
            files={
                'data': basic_aug_data.encode('utf-8'),
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
        response = self.datamart_post(
            '/search',
            data=basic_aug_data.encode('utf-8'),
            headers={'Content-type': 'text/csv'},
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
        response = self.datamart_post(
            '/profile',
            files={'data': basic_aug_data.encode('utf-8')},
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

    def test_both_data_profile(self):
        response = self.datamart_post(
            '/profile',
            files={'data': basic_aug_data.encode('utf-8')},
        )
        profile = response.json()

        response = self.datamart_post(
            '/search',
            files={
                'data': basic_aug_data.encode('utf-8'),
                'data_profile': json.dumps(profile).encode('utf-8'),
            },
            check_status=False,
        )
        self.assertEqual(response.status_code, 400)

    def test_lazo_join(self):
        response = self.datamart_post(
            '/search',
            files={
                'data': lazo_aug_data.encode('utf-8'),
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
                        'left_columns_names': [['home_address']],
                        'right_columns': [[0]],
                        'right_columns_names': [['state']],
                        'type': 'join'
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None
                }
            ]
        )

    def test_geo_union(self):
        query = {'keywords': ['places']}

        response = self.datamart_post(
            '/search',
            files={
                'query': json.dumps(query).encode('utf-8'),
                'data': geo_aug_data.encode('utf-8'),
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
        response = self.datamart_post(
            '/search',
            files={
                'data': geo_aug_data.encode('utf-8'),
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

    def test_temporal_daily_join(self):
        response = self.datamart_post(
            '/search',
            files={
                'data': daily_aug_data.encode('utf-8'),
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
                    'd3m_dataset_description':  lambda d: isinstance(d, dict),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0]],
                        'left_columns_names': [['orig_date']],
                        'right_columns': [[0]],
                        'right_columns_names':[['aug_date']],
                        'type':'join',
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None,
                },
            ],
        )

    def test_temporal_hourly_join(self):
        response = self.datamart_post(
            '/search',
            files={
                'data': hourly_aug_data.encode('utf-8'),
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
                    'd3m_dataset_description':  lambda d: isinstance(d, dict),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0]],
                        'left_columns_names': [['orig_date']],
                        'right_columns': [[0]],
                        'right_columns_names':[['aug_date']],
                        'type':'join',
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None,
                },
            ],
        )

    def test_temporal_hourly_daily_join(self):
        response = self.datamart_post(
            '/search',
            files={
                'data': hourly_aug_data_days.encode('utf-8'),
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
                    'd3m_dataset_description':  lambda d: isinstance(d, dict),
                    'score': lambda n: isinstance(n, float) and n > 0.0,
                    'augmentation': {
                        'left_columns': [[0]],
                        'left_columns_names': [['orig_date']],
                        'right_columns': [[0]],
                        'right_columns_names':[['aug_date']],
                        'type':'join',
                    },
                    'supplied_id': None,
                    'supplied_resource_id': None,
                },
            ],
        )


class TestDownload(DatamartTest):
    maxDiff = None

    def test_get_id(self):
        """Download datasets via GET /download/{dataset_id}"""
        # Basic dataset, materialized via direct_url
        response = self.datamart_get('/download/' + 'datamart.test.basic',
                                     # format defaults to csv
                                     allow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers['Location'],
                         'http://test_discoverer:7000/basic.csv')

        response = self.datamart_get('/download/' + 'datamart.test.basic',
                                     # explicit format
                                     params={'format': 'csv'},
                                     allow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers['Location'],
                         'http://test_discoverer:7000/basic.csv')

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
                                     allow_redirects=False)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'],
                         'application/octet-stream')
        self.assertTrue(response.content.startswith(b'id,lat,long,height\n'))

    def test_post(self):
        """Download datasets via POST /download"""
        # Basic dataset, materialized via direct_url
        basic_meta = self.datamart_get(
            '/metadata/' + 'datamart.test.basic'
        )
        basic_meta = basic_meta.json()['metadata']

        response = self.datamart_post(
            '/download', allow_redirects=False,
            params={'format': 'd3m', 'format_version': '3.2.0'},
            files={'task': json.dumps(
                {
                    'id': 'datamart.test.basic',
                    'score': 1.0,
                    'metadata': basic_meta
                }
            ).encode('utf-8')},
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
            params={'format': 'csv'},
            json={
                'id': 'datamart.test.basic',
                'score': 1.0,
                'metadata': basic_meta
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers['Location'],
                         'http://test_discoverer:7000/basic.csv')

        # Geo dataset, materialized via /datasets storage
        geo_meta = self.datamart_get(
            '/metadata/' + 'datamart.test.geo'
        )
        geo_meta = geo_meta.json()['metadata']

        response = self.datamart_post(
            '/download', allow_redirects=False,
            # format defaults to csv
            files={'task': json.dumps(
                {
                    'id': 'datamart.test.geo',
                    'score': 1.0,
                    'metadata': geo_meta
                }
            ).encode('utf-8')},
        )
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
        """Post invalid materialization information."""
        response = self.datamart_post(
            '/download', allow_redirects=False,
            files={'task': json.dumps(
                {
                    'id': 'datamart.nonexistent',
                    'score': 0.0,
                    'metadata': {
                        'name': "Non-existent dataset",
                        'materialize': {
                            'identifier': 'datamart.nonexistent',
                        }
                    }
                }
            ).encode('utf-8')},
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
        """Test datamart_materialize."""
        def assert_same_files(a, b):
            with open(a, 'r') as f_a:
                with open(b, 'r') as f_b:
                    self.assertEqual(f_a.read(), f_b.read())

        with tempfile.TemporaryDirectory() as tempdir:
            df = datamart_materialize.download(
                'datamart.test.agg',
                None,
                os.environ['QUERY_HOST'],
                'pandas',
            )
            self.assertEqual(df.shape, (8, 3))

            datamart_materialize.download(
                'datamart.test.geo',
                os.path.join(tempdir, 'geo.csv'),
                os.environ['QUERY_HOST'],
            )
            assert_same_files(
                os.path.join(tempdir, 'geo.csv'),
                os.path.join(os.path.dirname(__file__), 'data/geo.csv'),
            )

            datamart_materialize.download(
                'datamart.test.agg',
                os.path.join(tempdir, 'agg'),
                os.environ['QUERY_HOST'],
                'd3m',
            )
            assert_same_files(
                os.path.join(tempdir, 'agg/tables/learningData.csv'),
                os.path.join(os.path.dirname(__file__), 'data/agg.csv'),
            )


class TestAugment(DatamartTest):
    def test_basic_join(self):
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.basic'
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

        response = self.datamart_post(
            '/augment',
            files={
                'task': json.dumps(task).encode('utf-8'),
                'data': basic_aug_data.encode('utf-8'),
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
                'number,desk_faces,name,country,what',
                [
                    '4,west,remi,france,False',
                    '3,north,aecio,brazil,True',
                    '3,south,aecio,brazil,True',
                    '7,west,sonia,peru,True',
                    '8,east,roque,peru,True',
                    '10,,fernando,brazil,False',
                ],
            )
        with zip_.open('datasetDoc.json') as meta_fp:
            meta = json.load(meta_fp)
            self.assertJson(
                meta,
                {
                    'about': {
                        'approximateSize': '185 B',
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
                                    'colType': 'string',
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
                                    'colName': 'country',
                                    'colType': 'string',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 4,
                                    'colName': 'what',
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
                                'augmentation_type': 'join',
                                'nb_rows_after': 6,
                                'nb_rows_before': 6,
                                'new_columns': ['name', 'country', 'what'],
                                'removed_columns': [],
                            },
                            'qualValueType': 'dict',
                        },
                    ],
                },
            )

    def test_basic_join_auto(self):
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.basic'
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

        response = self.datamart_post(
            '/augment',
            files={
                'task': json.dumps(task).encode('utf-8'),
                'data': basic_aug_data.encode('utf-8'),
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
                'number,desk_faces,name,country,what',
                [
                    '4,west,remi,france,False',
                    '3,north,aecio,brazil,True',
                    '3,south,aecio,brazil,True',
                    '7,west,sonia,peru,True',
                    '8,east,roque,peru,True',
                    '10,,fernando,brazil,False',
                ],
            )
        with zip_.open('datasetDoc.json') as meta_fp:
            meta = json.load(meta_fp)
            self.assertJson(
                meta,
                {
                    'about': {
                        'approximateSize': '185 B',
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
                                    'colType': 'string',
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
                                    'colName': 'country',
                                    'colType': 'string',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 4,
                                    'colName': 'what',
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
                                'augmentation_type': 'join',
                                'nb_rows_after': 6,
                                'nb_rows_before': 6,
                                'new_columns': ['name', 'country', 'what'],
                                'removed_columns': [],
                            },
                            'qualValueType': 'dict',
                        },
                    ],
                },
            )

    def test_agg_join(self):
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.agg'
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

        response = self.datamart_post(
            '/augment',
            files={
                'task': json.dumps(task).encode('utf-8'),
                'data': agg_aug_data.encode('utf-8'),
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
                'id,location,work,mean salary,sum salary,max salary,min salary',
                [
                    '30,korea,True,150,300,200,100',
                    '40,brazil,False,100,100,100,100',
                    '70,usa,True,350,700,600,100',
                    '80,canada,True,200,200,200,200',
                    '100,france,False,250,500,300,200',
                ],
            )
        with zip_.open('datasetDoc.json') as meta_fp:
            meta = json.load(meta_fp)
            self.assertJson(
                meta,
                {
                    'about': {
                        'approximateSize': '216 B',
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
                                    'colType': 'string',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 2,
                                    'colName': 'work',
                                    'colType': 'string',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 3,
                                    'colName': 'mean salary',
                                    'colType': 'real',
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
                                    'colType': 'real',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 6,
                                    'colName': 'min salary',
                                    'colType': 'real',
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
                                    'work', 'mean salary', 'sum salary',
                                    'max salary', 'min salary',
                                ],
                                'removed_columns': [],
                            },
                            'qualValueType': 'dict',
                        },
                    ],
                },
            )

    def test_lazo_join(self):
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.lazo'
        )
        meta = meta.json()['metadata']

        task = {
            'id': 'datamart.test.lazo',
            'metadata': meta,
            'score': 1.0,
            'augmentation': {
                'left_columns': [[0]],
                'left_columns_names': [['home_address']],
                'right_columns': [[0]],
                'right_columns_names': [['state']],
                'type': 'join'
            },
            'supplied_id': None,
            'supplied_resource_id': None
        }

        response = self.datamart_post(
            '/augment',
            files={
                'task': json.dumps(task).encode('utf-8'),
                'data': lazo_aug_data.encode('utf-8'),
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
                'home_address,mean year,sum year,max year,min year',
                [
                    'AZ,1990.0,1990.0,1990.0,1990.0',
                    'PA,1990.0,1990.0,1990.0,1990.0',
                    'SD,,0.0,,',
                    'NJ,1990.0,1990.0,1990.0,1990.0',
                    'NH,,0.0,,',
                    'TX,1990.0,1990.0,1990.0,1990.0',
                    'MS,1990.0,1990.0,1990.0,1990.0',
                    'TN,1990.0,1990.0,1990.0,1990.0',
                    'WA,1990.0,1990.0,1990.0,1990.0',
                    'VA,1990.0,1990.0,1990.0,1990.0',
                    'NY,1990.0,1990.0,1990.0,1990.0',
                    'OH,1990.0,1990.0,1990.0,1990.0',
                    'OR,1990.0,1990.0,1990.0,1990.0',
                    'IL,1990.0,1990.0,1990.0,1990.0',
                    'MT,,0.0,,',
                    'GA,1990.0,1990.0,1990.0,1990.0',
                    'FL,,0.0,,',
                    'HI,,0.0,,',
                    'CA,1990.0,1990.0,1990.0,1990.0',
                    'NC,1990.0,1990.0,1990.0,1990.0',
                    'UT,1991.0,1991.0,1991.0,1991.0',
                    'SC,1991.0,1991.0,1991.0,1991.0',
                    'LA,1990.0,1990.0,1990.0,1990.0',
                    'RI,,0.0,,',
                    'PR,1990.0,1990.0,1990.0,1990.0',
                    'DE,,0.0,,',
                ],
            )
        with zip_.open('datasetDoc.json') as meta_fp:
            meta = json.load(meta_fp)
            print(meta['dataResources'])
            self.assertJson(
                meta,
                {
                    'about': {
                        'approximateSize': '709 B',
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
                                    'colName': 'home_address',
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
                                    'colType': 'real',
                                    'role': ['attribute'],
                                },
                                {
                                    'colIndex': 4,
                                    'colName': 'min year',
                                    'colType': 'real',
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
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.geo'
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

        response = self.datamart_post(
            '/augment',
            files={
                'task': json.dumps(task).encode('utf-8'),
                'data': geo_aug_data.encode('utf-8'),
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
                ','.join(e[:8] for e in l.split(','))
                for l in table_lines
            ]
            self.assertCsvEqualNoOrder(
                '\n'.join(table_lines[0:6]),
                'lat,long,id,letter',
                [
                    '40.73279,-73.9985,place100,a',
                    '40.72970,-73.9978,place101,b',
                    '40.73266,-73.9975,place102,c',
                    '40.73117,-74.0018,place103,d',
                    '40.69427,-73.9898,place104,e',
                ],
            )
        with zip_.open('datasetDoc.json') as meta_fp:
            meta = json.load(meta_fp)
            self.assertJson(
                meta,
                {
                    'about': {
                        'approximateSize': '3688 B',
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

    def test_temporal_daily_join(self):
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.daily'
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

        response = self.datamart_post(
            '/augment',
            files={
                'task': json.dumps(task).encode('utf-8'),
                'data': daily_aug_data.encode('utf-8'),
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
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.hourly'
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

        response = self.datamart_post(
            '/augment',
            files={
                'task': json.dumps(task).encode('utf-8'),
                'data': hourly_aug_data.encode('utf-8'),
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
                    '2019-06-13T01:00:00,blue,no',
                    '2019-06-13T02:00:00,blue,no',
                    '2019-06-13T03:00:00,green,no',
                    '2019-06-13T04:00:00,green,yes',
                    '2019-06-13T05:00:00,blue,no',
                ],
            )

    def test_temporal_hourly_days_join(self):
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.hourly'
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

        response = self.datamart_post(
            '/augment',
            files={
                'task': json.dumps(task).encode('utf-8'),
                'data': hourly_aug_data_days.encode('utf-8'),
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
                    '2019-06-12,blue,no',
                    '2019-06-13,green,no',
                ],
            )


def check_ranges(min_, max_):
    def check(ranges):
        assert len(ranges) == 3
        for rg in ranges:
            assert rg.keys() == {'range'}
            rg = rg['range']
            assert rg.keys() == {'gte', 'lte'}
            gte, lte = rg['gte'], rg['lte']
            assert min_ <= gte <= lte <= max_

        return True

    return check


def check_geo_ranges(min_long, min_lat, max_long, max_lat):
    def check(ranges):
        assert len(ranges) == 3
        for rg in ranges:
            assert rg.keys() == {'range'}
            rg = rg['range']
            assert rg.keys() == {'type', 'coordinates'}
            assert rg['type'] == 'envelope'
            [long1, lat1], [long2, lat2] = rg['coordinates']
            assert min_lat <= lat2 <= lat1 <= max_lat
            assert min_long <= long1 <= long2 <= max_long

        return True

    return check


version = os.environ['DATAMART_VERSION']
assert re.match(r'^v[0-9]+(\.[0-9]+)+(-[0-9]+-g[0-9a-f]{7})?$', version)


basic_metadata = {
    "name": "basic",
    "description": "This is a very simple CSV with people",
    "size": 126,
    "nb_rows": 5,
    "nb_profiled_rows": 5,
    "columns": [
        {
            "name": "name",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [
                "http://schema.org/Enumeration"
            ],
            "num_distinct_values": 5
        },
        {
            "name": "country",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [
                "http://schema.org/Enumeration"
            ],
            "num_distinct_values": 3
        },
        {
            "name": "number",
            "structural_type": "http://schema.org/Integer",
            "semantic_types": [],
            "mean": 6.4,
            "stddev": lambda n: round(n, 3) == 2.577,
            "coverage": (
                lambda l: sorted(l, key=lambda e: e['range']['gte']) == [
                    {
                        "range": {
                            "gte": 3.0,
                            "lte": 4.0
                        }
                    },
                    {
                        "range": {
                            "gte": 7.0,
                            "lte": 8.0
                        }
                    },
                    {
                        "range": {
                            "gte": 10.0,
                            "lte": 10.0
                        }
                    }
                ]
            )
        },
        {
            "name": "what",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [
                "http://schema.org/Boolean",
                "http://schema.org/Enumeration"
            ],
            "unclean_values_ratio": 0.0,
            "num_distinct_values": 2
        }
    ],
    "materialize": {
        "direct_url": "http://test_discoverer:7000/basic.csv",
        "identifier": "datamart.test",
        "date": lambda d: isinstance(d, str)
    },
    "sample": "name,country,number,what\nremi,france,4,false\n" +
              "aecio,brazil,3,true\nsonia,peru,7,true\nroque,peru,8,true\n" +
              "fernando,brazil,10,false\n",
    "date": lambda d: isinstance(d, str),
    "version": version
}


basic_metadata_d3m = lambda v: {
    'about': {
        'datasetID': 'datamart.test.basic',
        'datasetName': 'basic',
        'description': 'This is a very simple CSV with people',
        'license': 'unknown',
        'approximateSize': '126 B',
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
                    'colName': 'country',
                    'colType': 'string',
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
                    'colType': 'string',
                    'role': ['attribute'],
                },
            ],
        },
    ],
}


agg_metadata = {
    "name": "agg",
    "description": "Simple CSV with ids and salaries to test aggregation for numerical attributes",
    "size": 116,
    "nb_rows": 8,
    "nb_profiled_rows": 8,
    "columns": [
        {
            "name": "id",
            "structural_type": "http://schema.org/Integer",
            "semantic_types": [
                "http://schema.org/identifier"
            ],
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
            )
        },
        {
            "name": "work",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [
                "http://schema.org/Boolean",
                "http://schema.org/Enumeration"
            ],
            "unclean_values_ratio": 0.0,
            "num_distinct_values": 2
        },
        {
            "name": "salary",
            "structural_type": "http://schema.org/Integer",
            "semantic_types": [],
            "mean": 225.0,
            "stddev": lambda n: round(n, 3) == 156.125,
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
            )
        }
    ],
    "materialize": {
        "identifier": "datamart.test",
        "date": lambda d: isinstance(d, str)
    },
    "sample": "id,work,salary\n40,false,100\n30,true,200\n70,true,100\n80,tr" +
              "ue,200\n100,false,300\n100,true,200\n30,false,100\n70,false,6" +
              "00\n",
    "date": lambda d: isinstance(d, str),
    "version": version
}


geo_metadata = {
    "name": "geo",
    "description": "Another simple CSV with places",
    "size": 3910,
    "nb_rows": 100,
    "nb_profiled_rows": 100,
    "columns": [
        {
            "name": "id",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [],
            "missing_values_ratio": 0.01
        },
        {
            "name": "lat",
            "structural_type": "http://schema.org/Float",
            "semantic_types": lambda l: "http://schema.org/latitude" in l,
            "mean": lambda n: round(n, 3) == 40.711,
            "stddev": lambda n: round(n, 4) == 0.0186
        },
        {
            "name": "long",
            "structural_type": "http://schema.org/Float",
            "semantic_types": lambda l: "http://schema.org/longitude" in l,
            "mean": lambda n: round(n, 3) == -73.993,
            "stddev": lambda n: round(n, 5) == 0.00684
        },
        {
            "name": "height",
            "structural_type": "http://schema.org/Float",
            "semantic_types": lambda l: isinstance(l, list),
            "mean": lambda n: round(n, 3) == 47.827,
            "stddev": lambda n: round(n, 2) == 21.28,
            "coverage": check_ranges(1.0, 90.0)
        }
    ],
    "spatial_coverage": [
        {
            "lat": "lat",
            "lon": "long",
            "ranges": check_geo_ranges(-74.006, 40.6905, -73.983, 40.7352)
        }
    ],
    "materialize": {
        "identifier": "datamart.test",
        "date": lambda d: isinstance(d, str)
    },
    "sample": "id,lat,long,height\nplace05,40.722948,-74.001501,42.904820\np" +
              "lace06,40.735108,-73.996996,48.345170\nplace14,40.733272,-73." +
              "996875,51.000673\nplace21,40.733305,-73.999205,45.887002\npla" +
              "ce25,40.727810,-73.999472,35.740136\nplace39,40.732095,-73.99" +
              "6864,47.361715\nplace41,40.727197,-73.996098,62.933509\nplace" +
              "44,40.730017,-73.993764,38.067007\nplace46,40.730439,-73.9966" +
              "33,32.522354\nplace47,40.736176,-73.998520,50.594276\nplace48" +
              ",40.730226,-74.001459,5.034845\nplace51,40.692165,-73.987300," +
              "67.055957\nplace55,40.693658,-73.984096,27.633986\nplace60,40" +
              ".691525,-73.987374,70.962950\nplace65,40.692605,-73.986475,53" +
              ".012337\nplace72,40.692980,-73.987301,46.909863\nplace74,40.6" +
              "93227,-73.988686,59.675767\nplace85,40.692914,-73.989237,73.3" +
              "57646\nplace87,40.693326,-73.984213,32.226852\nplace97,40.692" +
              "794,-73.986984,32.891257\n",
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


lazo_metadata = {
    "name": "lazo",
    "description": "Simple CSV with states and years to test the Lazo index service",
    "size": 297,
    "nb_rows": 36,
    "nb_profiled_rows": 36,
    "columns": [
        {
            "name": "state",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [],
            "missing_values_ratio": lambda n: round(n, 4) == 0.0278
        },
        {
            "name": "year",
            "structural_type": "http://schema.org/Integer",
            "semantic_types": [],
            "mean": lambda n: round(n, 2) == 1990.11,
            "stddev": lambda n: round(n, 4) == 0.3143,
            "coverage": (
                lambda l: sorted(l, key=lambda e: e['range']['gte']) == [
                    {
                        "range": {
                            "gte": 1990.0,
                            "lte": 1990.0
                        }
                    },
                    {
                        "range": {
                            "gte": 1991.0,
                            "lte": 1991.0
                        }
                    }
                ]
            )
        }
    ],
    "materialize": {
        "direct_url": "http://test_discoverer:7000/lazo.csv",
        "identifier": "datamart.test",
        "date": lambda d: isinstance(d, str)
    },
    "sample": "state,year\nVA,1990\nKY,1990\nCA,1990\nWV,1990\nPR,1990\n" +
              "NC,1990\nAL,1990\nNJ,1990\nCT,1990\nCO,1990\n,1990\nMN,1990\n" +
              "OR,1990\nND,1990\nTN,1990\nGA,1990\nNM,1990\nAR,1990\n" +
              "UT,1991\nSC,1991\n",
    "date": lambda d: isinstance(d, str),
    "version": version
}


daily_metadata = {
    'name': 'daily',
    'description': 'Temporal dataset with daily resolution',
    'size': 448,
    'nb_rows': 30,
    "nb_profiled_rows": 30,
    'columns': [
        {
            'name': 'aug_date',
            'structural_type': 'http://schema.org/Text',
            'semantic_types': [
                'http://schema.org/DateTime',
                # FIXME: This is correct by our rules, but not a great outcome
                'https://metadata.datadrivendiscovery.org/types/PhoneNumber',
            ],
            'mean': lambda n: round(n) == 1557230400.0,
            'stddev': lambda n: round(n, 2) == 747830.14,
            'coverage': (
                lambda l: sorted(l, key=lambda e: e['range']['gte']) == [
                    {
                        'range': {
                            'gte': 1555977600.0,
                            'lte': 1556755200.0,
                        },
                    },
                    {
                        'range': {
                            'gte': 1556841600.0,
                            'lte': 1557619200.0,
                        },
                    },
                    {
                        'range': {
                            'gte': 1557705600.0,
                            'lte': 1558483200.0,
                        },
                    },
                ]
            ),
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
        },
    ],
    'materialize': {
        'identifier': 'datamart.test',
        'date': lambda d: isinstance(d, str),
    },
    'sample': "aug_date,rain\n2019-04-23,no\n2019-04-25,yes\n2019-04-26,no\n" +
              "2019-04-29,yes\n2019-05-02,no\n2019-05-03,yes\n2019-05-05,yes" +
              "\n2019-05-07,no\n2019-05-08,yes\n2019-05-09,yes\n2019-05-10,n" +
              "o\n2019-05-13,no\n2019-05-14,no\n2019-05-16,no\n2019-05-17,ye" +
              "s\n2019-05-18,no\n2019-05-19,yes\n2019-05-20,no\n2019-05-21,n" +
              "o\n2019-05-22,yes\n",
    'date': lambda d: isinstance(d, str),
    'version': version,
}


hourly_metadata = {
    'name': 'hourly',
    'description': 'Temporal dataset with hourly resolution',
    'size': 1242,
    'nb_rows': 52,
    "nb_profiled_rows": 52,
    'columns': [
        {
            'name': 'aug_date',
            'structural_type': 'http://schema.org/Text',
            'semantic_types': [
                'http://schema.org/DateTime',
            ],
            'mean': lambda n: round(n) == 1560389398.0,
            'stddev': lambda n: round(n, 2) == 54027.44,
            'coverage': (
                lambda l: sorted(l, key=lambda e: e['range']['gte']) == [
                    {
                        'range': {
                            'gte': 1560297600.0,
                            'lte': 1560358800.0,
                        },
                    },
                    {
                        'range': {
                            'gte': 1560362400.0,
                            'lte': 1560420000.0,
                        },
                    },
                    {
                        'range': {
                            'gte': 1560423600.0,
                            'lte': 1560481200.0,
                        },
                    },
                ]
            ),
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
        },
    ],
    'materialize': {
        'direct_url': 'http://test_discoverer:7000/hourly.csv',
        'identifier': 'datamart.test',
        'date': lambda d: isinstance(d, str),
    },
    'sample': "aug_date,rain\n2019-06-12T01:00:00,no\n2019-06-12T02:00:00,no" +
              "\n2019-06-12T03:00:00,yes\n2019-06-12T09:00:00,no\n2019-06-12" +
              "T10:00:00,yes\n2019-06-12T11:00:00,yes\n2019-06-12T12:00:00,y" +
              "es\n2019-06-12T14:00:00,yes\n2019-06-12T15:00:00,no\n2019-06-" +
              "12T20:00:00,yes\n2019-06-12T21:00:00,yes\n2019-06-13T01:00:00" +
              ",no\n2019-06-13T03:00:00,no\n2019-06-13T05:00:00,no\n2019-06-" +
              "13T07:00:00,yes\n2019-06-13T10:00:00,yes\n2019-06-13T14:00:00" +
              ",yes\n2019-06-13T17:00:00,yes\n2019-06-14T00:00:00,yes\n2019-" +
              "06-14T01:00:00,yes\n",
    'date': lambda d: isinstance(d, str),
    'version': version,
}


basic_aug_data = (
    'number,desk_faces\n'
    '4,west\n'
    '3,south\n'
    '7,west\n'
    '8,east\n'
    '3,north\n'
    '10,\n'
)


agg_aug_data = (
    'id,location\n'
    '40,brazil\n'
    '30,korea\n'
    '70,usa\n'
    '80,canada\n'
    '100,france\n'
)


geo_aug_data = (
    'lat,long,id,letter\n'
    '40.732792,-73.998516,place100,a\n'
    '40.729707,-73.997885,place101,b\n'
    '40.732666,-73.997576,place102,c\n'
    '40.731173,-74.001817,place103,d\n'
    '40.694272,-73.989852,place104,e\n'
    '40.694424,-73.987888,place105,f\n'
    '40.693446,-73.988829,place106,g\n'
    '40.692157,-73.989549,place107,h\n'
    '40.695933,-73.986665,place108,i\n'
    '40.692827,-73.988438,place109,j\n'
)


lazo_aug_data = (
    'home_address\n'
    'AZ\n'
    'PA\n'
    'SD\n'
    'NJ\n'
    'NH\n'
    'TX\n'
    'MS\n'
    'TN\n'
    'WA\n'
    'VA\n'
    'NY\n'
    'OH\n'
    'OR\n'
    'IL\n'
    'MT\n'
    'GA\n'
    'FL\n'
    'HI\n'
    'CA\n'
    'NC\n'
    'UT\n'
    'SC\n'
    'LA\n'
    'RI\n'
    'PR\n'
    'DE\n'
)

daily_aug_data = (
    'orig_date,n_people\n'
    '2019-04-28,3\n'
    '2019-04-29,5\n'
    '2019-04-30,0\n'
    '2019-05-01,1\n'
    '2019-05-02,3\n'
    '2019-05-03,2\n'
)

hourly_aug_data_days = (
    'orig_date,color\n'
    '2019-06-12,blue\n'
    '2019-06-13,green\n'
)

hourly_aug_data = (
    'orig_date,color\n'
    '2019-06-13T01:00:00,blue\n'
    '2019-06-13T02:00:00,blue\n'
    '2019-06-13T03:00:00,green\n'
    '2019-06-13T04:00:00,green\n'
    '2019-06-13T05:00:00,blue\n'
)
