import elasticsearch
import io
import json
import jsonschema
import os
import re
import requests
import time
import unittest
import zipfile

from .utils import assert_json


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


class DatamartTest(unittest.TestCase):
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


class TestProfiler(unittest.TestCase):
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

        assert_json(
            hits,
            {
                'datamart.test.basic': basic_metadata,
                'datamart.test.geo': geo_metadata,
                'datamart.test.agg': agg_metadata,
                'datamart.test.lazo': lazo_metadata
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
        assert_json(response.json(), metadata)


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
        assert_json(
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
                'd3m_dataset_description': basic_metadata_d3m,
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
        assert_json(
            results,
            [
                {
                    'id': 'datamart.test.basic',
                    'metadata': basic_metadata,
                    'd3m_dataset_description': basic_metadata_d3m,
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
        assert_json(
            results,
            [
                {
                    'id': 'datamart.test.basic',
                    'metadata': basic_metadata,
                    'd3m_dataset_description': basic_metadata_d3m,
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
        assert_json(
            results,
            [
                {
                    'id': 'datamart.test.basic',
                    'metadata': basic_metadata,
                    'd3m_dataset_description': basic_metadata_d3m,
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
        assert_json(
            results,
            [
                {
                    'id': 'datamart.test.basic',
                    'metadata': basic_metadata,
                    'd3m_dataset_description': basic_metadata_d3m,
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
                'data': lazo_data.encode('utf-8'),
            },
            schema=result_list_schema,
        )
        results = response.json()['results']
        assert_json(
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
        assert_json(
            results,
            [
                {
                    'id': 'datamart.test.geo',
                    'metadata': geo_metadata,
                    'd3m_dataset_description': geo_metadata_d3m,
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
        assert_json(
            results,
            [
                {
                    'id': 'datamart.test.geo',
                    'metadata': geo_metadata,
                    'd3m_dataset_description': geo_metadata_d3m,
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


class TestDownload(DatamartTest):
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

        # Geo dataset, materialized via /datasets storage
        response = self.datamart_get('/download/' + 'datamart.test.basic',
                                     params={'format': 'd3m'},
                                     allow_redirects=False)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'], 'application/zip')
        zip_ = zipfile.ZipFile(io.BytesIO(response.content))
        self.assertEqual(set(zip_.namelist()),
                         {'datasetDoc.json', 'tables/learningData.csv'})

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
            params={'format': 'd3m'},
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
        zip = zipfile.ZipFile(io.BytesIO(response.content))
        zip.testzip()
        self.assertEqual(
            set(zip.namelist()),
            {'datasetDoc.json', 'tables/learningData.csv'},
        )
        with zip.open('tables/learningData.csv') as table:
            self.assertEqual(
                table.read().decode('utf-8'),
                'number,desk_faces,name,country,what\n'
                '4,west,remi,france,False\n'
                '3,south,aecio,brazil,True\n'
                '7,west,sonia,peru,True\n'
                '8,east,roque,peru,True\n'
                '10,west,fernando,brazil,False\n',
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
        zip = zipfile.ZipFile(io.BytesIO(response.content))
        zip.testzip()
        self.assertEqual(
            set(zip.namelist()),
            {'datasetDoc.json', 'tables/learningData.csv'},
        )
        with zip.open('tables/learningData.csv') as table:
            self.assertEqual(
                table.read().decode('utf-8'),
                'number,desk_faces,name,country,what\n'
                '4,west,remi,france,False\n'
                '3,south,aecio,brazil,True\n'
                '7,west,sonia,peru,True\n'
                '8,east,roque,peru,True\n'
                '10,west,fernando,brazil,False\n',
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
        zip = zipfile.ZipFile(io.BytesIO(response.content))
        zip.testzip()
        self.assertEqual(
            set(zip.namelist()),
            {'datasetDoc.json', 'tables/learningData.csv'},
        )
        with zip.open('tables/learningData.csv') as table:
            self.assertEqual(
                table.read().decode('utf-8'),
                'id,location,mean salary,sum salary,amax salary,amin salary\n'
                '30,korea,150,300,200,100\n'
                '40,brazil,100,100,100,100\n'
                '70,usa,350,700,600,100\n'
                '80,canada,200,200,200,200\n'
                '100,france,250,500,300,200\n',
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
                'data': lazo_data.encode('utf-8'),
            },
        )
        self.assertEqual(response.headers['Content-Type'], 'application/zip')
        self.assertTrue(
            response.headers['Content-Disposition'].startswith('attachment')
        )
        zip = zipfile.ZipFile(io.BytesIO(response.content))
        zip.testzip()
        self.assertEqual(
            set(zip.namelist()),
            {'datasetDoc.json', 'tables/learningData.csv'},
        )
        with zip.open('tables/learningData.csv') as table:
            self.assertEqual(
                table.read().decode('utf-8'),
                'home_address,year\n'
                'AZ,1990.0\n'
                'PA,1990.0\n'
                'SD,\n'
                'NJ,1990.0\n'
                'NH,\n'
                'TX,1990.0\n'
                'MS,1990.0\n'
                'TN,1990.0\n'
                'WA,1990.0\n'
                'VA,1990.0\n'
                'NY,1990.0\n'
                'OH,1990.0\n'
                'OR,1990.0\n'
                'IL,1990.0\n'
                'MT,\n'
                'GA,1990.0\n'
                'FL,\n'
                'HI,\n'
                'CA,1990.0\n'
                'NC,1990.0\n'
                'UT,1991.0\n'
                'SC,1991.0\n'
                'LA,1990.0\n'
                'RI,\n'
                'PR,1990.0\n'
                'DE,\n'
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
        zip = zipfile.ZipFile(io.BytesIO(response.content))
        zip.testzip()
        self.assertEqual(
            set(zip.namelist()),
            {'datasetDoc.json', 'tables/learningData.csv'},
        )
        with zip.open('tables/learningData.csv') as table:
            table_lines = table.read().decode('utf-8').splitlines(False)
            # Truncate fields to work around rounding errors
            # FIXME: Deal with rounding errors
            table_lines = [
                ','.join(e[:8] for e in l.split(','))
                for l in table_lines
            ]
            self.assertEqual(
                '\n'.join(table_lines[0:6]),
                'lat,long,id,letter\n'
                '40.73279,-73.9985,place100,a\n'
                '40.72970,-73.9978,place101,b\n'
                '40.73266,-73.9975,place102,c\n'
                '40.73117,-74.0018,place103,d\n'
                '40.69427,-73.9898,place104,e'
            )


def check_ranges(min, max):
    def check(ranges):
        assert len(ranges) == 3
        for rg in ranges:
            assert rg.keys() == {'range'}
            rg = rg ['range']
            assert rg.keys() == {'gte', 'lte'}
            gte, lte = rg['gte'], rg['lte']
            assert min <= gte <= lte <= max

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
    "date": lambda d: isinstance(d, str),
    "version": version
}


basic_metadata_d3m = {
    'about': {
        'datasetID': 'datamart.test.basic',
        'datasetName': 'basic',
        'license': 'unknown',
        'approximateSize': '126 B',
        'datasetSchemaVersion': '3.2.0',
        'redacted': False,
        'datasetVersion': '0.0',
    },
    'dataResources': [
        {
            'resID': 'learningData',
            'resPath': 'tables/learningData.csv',
            'resType': 'table',
            'resFormat': ['text/csv'],
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
    "date": lambda d: isinstance(d, str),
    "version": version
}


geo_metadata = {
    "name": "geo",
    "description": "Another simple CSV with places",
    "size": 3910,
    "nb_rows": 100,
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
    "date": lambda d: isinstance(d, str),
    "version": version
}


geo_metadata_d3m = {
    'about': {
        'datasetID': 'datamart.test.geo',
        'datasetName': 'geo',
        'license': 'unknown',
        'approximateSize': '3910 B',
        'datasetSchemaVersion': '3.2.0',
        'redacted': False,
        'datasetVersion': '0.0',
    },
    'dataResources': [
        {
            'resID': 'learningData',
            'resPath': 'tables/learningData.csv',
            'resType': 'table',
            'resFormat': ['text/csv'],
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
    "date": lambda d: isinstance(d, str),
    "version": version
}


basic_aug_data = (
    'number,desk_faces\n'
    '4,west\n'
    '3,south\n'
    '7,west\n'
    '8,east\n'
    '10,west\n'
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


lazo_data = (
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
