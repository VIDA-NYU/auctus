import elasticsearch
import io
import json
import os
import re
import requests
import unittest
import zipfile

from .utils import assert_json


class DatamartTest(unittest.TestCase):
    def datamart_get(self, url, **kwargs):
        response = requests.get(
            os.environ['QUERY_HOST'] + url,
            **kwargs
        )
        self.assert_response(response)
        return response

    def datamart_post(self, url, **kwargs):
        response = requests.post(
            os.environ['QUERY_HOST'] + url,
            **kwargs
        )
        self.assert_response(response)
        return response

    def assert_response(self, response):
        if response.status_code == 400:  # pragma: no cover
            try:
                error = response.json()['error']
            except (KeyError, ValueError):
                error = "(not JSON)"
            self.fail("Error 400 from server: %s" % error)
        response.raise_for_status()


class TestProfile(unittest.TestCase):
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
            },
        )


class TestSearch(DatamartTest):
    def test_basic_search_json(self):
        """Basic search, posting the query as JSON."""
        @self.do_test_basic_search
        def query():
            response = self.datamart_post(
                '/search',
                json={'keywords': ['people']},
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
        )
        results = response.json()['results']
        assert_json(
            results,
            [
                {
                    'id': 'datamart.test.basic',
                    'metadata': basic_metadata,
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
        )
        results = response.json()['results']
        assert_json(
            results,
            [
                {
                    'id': 'datamart.test.basic',
                    'metadata': basic_metadata,
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

    def test_geo_union(self):
        query = {'keywords': ['places']}

        response = self.datamart_post(
            '/search',
            files={
                'query': json.dumps(query).encode('utf-8'),
                'data': geo_aug_data.encode('utf-8'),
            },
        )
        results = response.json()['results']
        results = [r for r in results if r['augmentation']['type'] == 'union']
        assert_json(
            results,
            [
                {
                    'id': 'datamart.test.geo',
                    'metadata': geo_metadata,
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
        )
        results = response.json()['results']
        results = [r for r in results if r['augmentation']['type'] == 'union']
        assert_json(
            results,
            [
                {
                    'id': 'datamart.test.geo',
                    'metadata': geo_metadata,
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
        self.assertTrue(response.content.startswith(b'id,lat,long\n'))

    def test_post(self):
        """Download datasets via POST /download"""
        # Basic dataset, materialized via direct_url
        basic_meta = self.datamart_get(
            '/metadata/' + 'datamart.test.basic'
        )
        basic_meta = basic_meta.json()

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
        geo_meta = geo_meta.json()

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
        self.assertTrue(response.content.startswith(b'id,lat,long\n'))

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


class TestAugment(DatamartTest):
    def test_basic_join(self):
        meta = self.datamart_get(
            '/metadata/' + 'datamart.test.basic'
        )
        meta = meta.json()

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


def check_ranges(min_long, min_lat, max_long, max_lat):
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
assert re.match(r'^v[0-9]+(\.[0-9]+)+(-[0-9]+)?-g[0-9a-f]{7}$', version)


basic_metadata = {
    "description": "This is a very simple CSV with people",
    "size": 126,
    "nb_rows": 5,
    "columns": [
        {
            "name": "name",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [
                "https://schema.org/Enumeration"
            ]
        },
        {
            "name": "country",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [
                "https://schema.org/Enumeration"
            ]
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
                "https://schema.org/Enumeration"
            ]
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


geo_metadata = {
    "description": "Another simple CSV with places",
    "size": 2912,
    "nb_rows": 100,
    "columns": [
        {
            "name": "id",
            "structural_type": "http://schema.org/Text",
            "semantic_types": []
        },
        {
            "name": "lat",
            "structural_type": "http://schema.org/Float",
            "semantic_types": lambda l: "http://schema.org/latitude" in l,
            "mean": lambda n: round(n, 3) == 40.712,
            "stddev": lambda n: round(n, 4) == 0.0187
        },
        {
            "name": "long",
            "structural_type": "http://schema.org/Float",
            "semantic_types": lambda l: "http://schema.org/longitude" in l,
            "mean": lambda n: round(n, 3) == -73.993,
            "stddev": lambda n: round(n, 5) == 0.00654
        }
    ],
    "spatial_coverage": [
        {
            "lat": "lat",
            "lon": "long",
            "ranges": check_ranges(-74.005, 40.6885, -73.9808, 40.7374)
        }
    ],
    "materialize": {
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


geo_aug_data = (
    'lat,long,id\n'
    '40.732792,-73.998516,place100\n'
    '40.729707,-73.997885,place101\n'
    '40.732666,-73.997576,place102\n'
    '40.731173,-74.001817,place103\n'
    '40.694272,-73.989852,place104\n'
    '40.694424,-73.987888,place105\n'
    '40.693446,-73.988829,place106\n'
    '40.692157,-73.989549,place107\n'
    '40.695933,-73.986665,place108\n'
    '40.692827,-73.988438,place109\n'
)
