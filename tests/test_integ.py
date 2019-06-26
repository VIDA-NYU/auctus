import json
import os
import requests
import time
import unittest

from .utils import assert_json


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
        start = time.perf_counter()
        while time.perf_counter() < start + 30:
            response = query_func()
            if response.status_code == 404:
                print('x', end='', flush=True)
                time.sleep(2)
                continue
            if response.status_code == 400:
                try:
                    error = response.json()['error']
                except (KeyError, ValueError):
                    error = "(not JSON)"
                self.fail("Error 400 from server: %s" % error)
            response.raise_for_status()
            break
        else:
            self.fail("No dataset ingested")

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
                'metadata': {
                    "description": "This is a very simple CSV with people",
                    "size": 125,
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
                            "mean": 6.2,
                            "stddev": lambda n: round(n, 3) == 2.315,
                            "coverage": [
                                {
                                    "range": {
                                        "gte": 3.0,
                                        "lte": 9.0
                                    }
                                }
                            ]
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
            },
        )


if __name__ == '__main__':
    unittest.main()
