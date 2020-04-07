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
                                    'id': {
                                        'query': 'green taxi',
                                        'operator': 'and',
                                    },
                                },
                            },
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
                            'dataset_id': {
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

    def test_ranges(self):
        main, sup_funcs, sup_filters, vars = parse_query({
            'keywords': ['green', 'taxi'],
            'source': 'gov',
            'variables': [
                {
                    'type': 'temporal_variable',
                    'start': '2019-01-01',
                    'end': '2019-12-31',
                },
                {
                    'type': 'geospatial_variable',
                    'latitude1': 45.4,
                    'latitude2': 50.6,
                    'longitude1': -73.2,
                    'longitude2': -75.8,
                },
            ],
        })
        self.assertEqual(
            main,
            [
                {
                    'bool': {
                        'should': [
                            {
                                'match': {
                                    'id': {
                                        'query': 'green taxi',
                                        'operator': 'and',
                                    },
                                },
                            },
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
                [
                    {
                        'nested': {
                            'path': 'columns',
                            'query': {
                                'bool': {
                                    'must': [
                                        {
                                            'term': {
                                                'columns.semantic_types': 'http://schema.org/DateTime',
                                            },
                                        },
                                        {
                                            'range': {
                                                'columns.coverage.range': {
                                                    'gte': 1546300800.0,
                                                    'lte': 1577750400.0,
                                                    'relation': 'intersects',
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                        },
                    },
                    {
                        'nested': {
                            'path': 'spatial_coverage.ranges',
                            'query': {
                                'bool': {
                                    'filter': {
                                        'geo_shape': {
                                            'spatial_coverage.ranges.range': {
                                                'shape': {
                                                    'type': 'envelope',
                                                    'coordinates': [
                                                        [-75.8, 50.6],
                                                        [-73.2, 45.4],
                                                    ],
                                                },
                                                'relation': 'intersects',
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                ],
            ],
        )
