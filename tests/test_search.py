import unittest
from unittest import mock

from apiserver.search import parse_query
from apiserver.search import join
from apiserver.search.union import name_similarity

from .utils import DataTestCase


class TestSearch(unittest.TestCase):
    def test_simple(self):
        """Test the query generation for a simple search"""
        main, sup_funcs, sup_filters, vars = parse_query({
            'keywords': ['green', 'taxi'],
            'source': 'gov',
        })
        self.assertEqual(
            main,
            [
                {
                    'multi_match': {
                        'query': 'green taxi',
                        'operator': 'and',
                        'type': 'cross_fields',
                        'fields': ['id^10', 'description', 'name^3', 'attribute_keywords'],
                    },
                },
                {
                    'bool': {
                        'filter': [
                            {
                                'terms': {
                                    'source': ['gov'],
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
                        'multi_match': {
                            'query': 'green taxi',
                            'operator': 'and',
                            'type': 'cross_fields',
                            'fields': [
                                'dataset_id^10',
                                'dataset_description',
                                'dataset_name^3',
                                'dataset_attribute_keywords',
                            ],
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
                        'dataset_source': ['gov'],
                    },
                },
            ],
        )
        self.assertEqual(vars, [])

    def test_types(self):
        """Test the query generation for a search with dataset types"""
        main, sup_funcs, sup_filters, vars = parse_query({
            'keywords': ['food'],
            'types': ['spatial', 'temporal'],
        })

        self.assertEqual(
            main,
            [
                {
                    'multi_match': {
                        'query': 'food',
                        'operator': 'and',
                        'type': 'cross_fields',
                        'fields': [
                            'id^10',
                            'description',
                            'name^3',
                            'attribute_keywords',
                        ],
                    },
                },
                {
                    'bool': {
                        'filter': [
                            {
                                'terms': {
                                    'types': ['spatial', 'temporal'],
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
                        'multi_match': {
                            'query': 'food',
                            'type': 'cross_fields',
                            'operator': 'and',
                            'fields': [
                                'dataset_id^10',
                                'dataset_description',
                                'dataset_name^3',
                                'dataset_attribute_keywords',
                            ],
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
                        'dataset_types': ['spatial', 'temporal'],
                    },
                },
            ],
        )
        self.assertEqual(vars, [])

    def test_ranges(self):
        """Test the query generation for spatial/temporal ranges"""
        main, sup_funcs, sup_filters, vars = parse_query({
            'keywords': ['green', 'taxi'],
            'source': ['gov'],
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
                    'multi_match': {
                        'query': 'green taxi',
                        'operator': 'and',
                        'type': 'cross_fields',
                        'fields': ['id^10', 'description', 'name^3', 'attribute_keywords'],
                    },
                },
                {
                    'bool': {
                        'filter': [
                            {
                                'terms': {
                                    'source': ['gov'],
                                },
                            },
                        ],
                    },
                },
                {
                    'nested': {
                        'path': 'temporal_coverage',
                        'query': {
                            'bool': {
                                'must': [
                                    {
                                        'nested': {
                                            'path': 'temporal_coverage.ranges',
                                            'query': {
                                                'range': {
                                                    'temporal_coverage.ranges.range': {
                                                        'gte': 1546300800.0,
                                                        'lte': 1577750400.0,
                                                        'relation': 'intersects',
                                                    },
                                                },
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
        )


class TestAugmentation(DataTestCase):
    def test_temporal(self):
        """Test searching for augmentation with temporal data"""
        main, sup_funcs, sup_filters, vars = parse_query({
            'keywords': 'green taxi',
        })
        es = mock.Mock()
        result = object()
        es.search.return_value = {
            'hits': {
                'hits': [
                    result,
                ],
            },
        }
        results = join.get_temporal_join_search_results(
            es,
            [[1.0, 2.0], [11.0, 12.0]],
            None,
            None,
            sup_funcs,
            sup_filters,
        )
        self.assertEqual(results, [result])
        self.assertEqual(len(es.search.call_args_list), 1)
        args, kwargs = es.search.call_args_list[0]
        self.assertEqual(args, ())
        temporal_query = lambda a, b: {
            'nested': {
                'path': 'ranges',
                'query': {
                    'function_score': {
                        'query': {
                            'range': {
                                'ranges.range': {
                                    'gte': a,
                                    'lte': b,
                                    'relation': 'intersects',
                                },
                            },
                        },
                        'script_score': {
                            'script': {
                                'lang': 'painless',
                                'params': {
                                    'gte': a,
                                    'lte': b,
                                    'coverage': 4.0,
                                },
                                'source': lambda s: (
                                    isinstance(s, str) and len(s) > 20
                                ),
                            },
                        },
                        'boost_mode': 'replace',
                    },
                },
                'inner_hits': {
                    '_source': False,
                    'size': 100,
                    'name': lambda s: s.startswith('range-'),
                },
                'score_mode': 'sum',
            },
        }
        kwargs.pop('request_timeout', None)
        self.assertJson(
            kwargs,
            dict(
                index='temporal_coverage',
                body={
                    '_source': lambda d: isinstance(d, dict),
                    'query': {
                        'function_score': {
                            'query': {
                                'bool': {
                                    'filter': [],
                                    'should': [
                                        temporal_query(1.0, 2.0),
                                        temporal_query(11.0, 12.0),
                                    ],
                                    'must_not': [],
                                    'minimum_should_match': 1
                                },
                            },
                            'functions': [
                                {
                                    'filter': {
                                        'multi_match': {
                                            'query': 'green taxi',
                                            'operator': 'and',
                                            'type': 'cross_fields',
                                            'fields': [
                                                'dataset_id^10',
                                                'dataset_description',
                                                'dataset_name^3',
                                                'dataset_attribute_keywords',
                                            ],
                                        },
                                    },
                                    'weight': 10,
                                },
                            ],
                            'score_mode': 'sum',
                            'boost_mode': 'multiply',
                        },
                    },
                },
                size=50,
            ),
        )

    def test_name_similarity(self):
        self.assertAlmostEqual(
            name_similarity("temperature", "temperature"),
            1.00,
            places=2,
        )
        self.assertAlmostEqual(
            name_similarity("fridge temperature", "temperature"),
            0.56,
            places=2,
        )
        self.assertAlmostEqual(
            name_similarity("avg temperature", "temperature avg"),
            0.625,
            places=2,
        )
        self.assertAlmostEqual(
            name_similarity("temperature", "temperament"),
            0.38,
            places=2,
        )
