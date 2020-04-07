import unittest

from query.search import parse_query


class TestSearch(unittest.TestCase):
    def test_simple(self):
        main, sup_funcs, sup_filters, vars = parse_query({
            'keywords': ['green', 'taxi'],
            'source': 'gov',
        })
        self.assertEqual(
            main,
            [
                {
                    'bool': {
                        'should': [
                            {
                                'match': {
                                    'description': {
                                        'query': 'green taxi',
                                        'operator': 'and',
                                    },
                                },
                            },
                            {
                                'match': {
                                    'name': {
                                        'query': 'green taxi',
                                        'operator': 'and',
                                    },
                                },
                            },
                            {
                                'nested': {
                                    'path': 'columns',
                                    'query': {
                                        'match': {
                                            'columns.name': {
                                                'query': 'green taxi',
                                                'operator': 'and',
                                            },
                                        },
                                    },
                                },
                            },
                        ],
                        'minimum_should_match': 1,
                    },
                },
                {
                    'bool': {
                        'filter': [
                            {
                                'terms': {
                                    'source': 'gov',
                                },
                            },
                        ],
                    },
                },
            ],
        )
        self.assertEqual(
            sup_funcs,
            [
                {
                    'filter': {
                        'match': {
                            'dataset_description': {
                                'query': 'green taxi',
                                'operator': 'and',
                            },
                        },
                    },
                    'weight': 10,
                },
                {
                    'filter': {
                        'match': {
                            'dataset_name': {
                                'query': 'green taxi',
                                'operator': 'and',
                            },
                        },
                    },
                    'weight': 10,
                },
                {
                    'filter': {
                        'match': {
                            'name': {
                                'query': 'green taxi',
                                'operator': 'and',
                            },
                        },
                    },
                    'weight': 10,
                },
            ],
        )
        self.assertEqual(
            sup_filters,
            [
                {
                    'terms': {
                        'dataset_source': 'gov',
                    },
                },
            ],
        )
        self.assertEqual(vars, [])
