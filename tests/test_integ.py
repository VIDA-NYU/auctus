import itertools
import json
import requests
import time
import unittest


def assert_json(actual, expected, pos='@'):
    if callable(expected):
        # The reason this function exists
        if not expected(actual):
            raise AssertionError(
                "Validation failed for %r at %s" % (actual, pos)
            )
        return

    if type(actual) != type(expected):
        raise AssertionError(
            "Type mismatch: expected %r, got %r at %s" % (
                type(expected), type(actual), pos,
            )
        )
    elif isinstance(actual, list):
        if len(actual) != len(expected):
            raise AssertionError(
                "List lengths don't match: expected %d, got %d at %s" % (
                    len(expected), len(actual), pos,
                )
            )
        for i, (a, e) in enumerate(zip(actual, expected)):
            assert_json(a, e, '%s[%d]' % (pos, i))
    elif isinstance(actual, dict):
        if actual.keys() != expected.keys():
            msg = "Dict lengths don't match; expected %d, got %d at %s" % (
                len(expected), len(actual), pos,
            )
            if len(actual) > len(expected):
                unexpected = set(actual) - set(expected)
                msg += "\nUnexpected keys: "
            else:
                unexpected = set(expected) - set(actual)
                msg += "\nMissing keys: "
            if len(unexpected) > 3:
                msg += ', '.join(repr(key)
                                 for key in itertools.islice(unexpected, 3))
                msg += ', ...'
            else:
                msg += ', '.join(repr(key)
                                 for key in unexpected)
            raise AssertionError(msg)
        for k, a in actual.items():
            e = expected[k]
            assert_json(a, e, '%s.%r' % (pos, k))
    else:
        if actual != expected:
            raise ValueError("%r != %r at %s" % (actual, expected, pos))


class TestSearch(unittest.TestCase):
    def test_post_json(self):
        @self.do_test_ingested
        def query():
            response = requests.post(
                'http://127.0.0.1:8002/search',
                json={'keywords': ['people']},
            )
            self.assertEqual(response.request.headers['Content-Type'],
                             'application/json')
            return response

    def test_post_data(self):
        @self.do_test_ingested
        def query():
            response = requests.post(
                'http://127.0.0.1:8002/search',
                data={'query': json.dumps({'keywords': ['people']})},
            )
            self.assertEqual(response.request.headers['Content-Type'],
                             'application/x-www-form-urlencoded')
            return response

    def test_post_file(self):
        @self.do_test_ingested
        def query():
            response = requests.post(
                'http://127.0.0.1:8002/search',
                files={'query': json.dumps({'keywords': ['people']})
                                .encode('utf-8')},
            )
            self.assertEqual(
                response.request.headers['Content-Type'].split(';', 1)[0],
                'multipart/form-data',
            )
            return response

    def do_test_ingested(self, query_func):
        start = time.perf_counter()
        while time.perf_counter() < start + 30:
            response = query_func()
            if response.status_code == 404:
                print('x', end='', flush=True)
                time.sleep(2)
                continue
            if response.status_code == 400:
                self.fail("Error 400 from server: %s" %
                          response.json()['error'])
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
                        "direct_url": "http://172.0.44.1:7000/basic.csv",
                        "identifier": "datamart.test",
                        "date": lambda d: isinstance(d, str)
                    },
                    "date": lambda d: isinstance(d, str)
                }
            },
        )


if __name__ == '__main__':
    unittest.main()
