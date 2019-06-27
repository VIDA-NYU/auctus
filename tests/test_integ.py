import elasticsearch
import json
import os
import requests
import unittest

from .utils import assert_json


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


class TestSearch(unittest.TestCase):
    def test_basic_search_json(self):
        """Basic search, posting the query as JSON."""
        @self.do_test_basic_search
        def query():
            response = requests.post(
                os.environ['QUERY_HOST'] + '/search',
                json={'keywords': ['people']},
            )
            self.assertEqual(response.request.headers['Content-Type'],
                             'application/json')
            return response

    def test_basic_search_formdata(self):
        """Basic search, posting the query as formdata-urlencoded."""
        @self.do_test_basic_search
        def query():
            response = requests.post(
                os.environ['QUERY_HOST'] + '/search',
                data={'query': json.dumps({'keywords': ['people']})},
            )
            self.assertEqual(response.request.headers['Content-Type'],
                             'application/x-www-form-urlencoded')
            return response

    def test_basic_search_file(self):
        """Basic search, posting the query as a file in multipart/form-data."""
        @self.do_test_basic_search
        def query():
            response = requests.post(
                os.environ['QUERY_HOST'] + '/search',
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
        if response.status_code == 400:
            try:
                error = response.json()['error']
            except (KeyError, ValueError):
                error = "(not JSON)"
            self.fail("Error 400 from server: %s" % error)
        response.raise_for_status()

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
                },
                'score': lambda n: isinstance(n, float),
                'metadata': basic_metadata
            },
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
    "date": lambda d: isinstance(d, str)
}


geo_metadata = {
    "description": "Another simple CSV with places",
    "size": 2913,
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
        "direct_url": "http://test_discoverer:7000/geo.csv",
        "identifier": "datamart.test",
        "date": lambda d: isinstance(d, str)
    },
    "date": lambda d: isinstance(d, str)
}


if __name__ == '__main__':
    unittest.main()
