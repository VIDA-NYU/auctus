#!/usr/bin/env python3
import elasticsearch
import os
import sys


COLUMN_MAPPING = {
    'properties': {
        'name': {
            'type': 'text',
            'fields': {
                'raw': {
                    'type': 'keyword'
                }
            }
        },
        'dataset_id': {
            'type': 'text'
        },
        'dataset_name': {
            'type': 'text'
        },
        'dataset_description': {
            'type': 'text'
        },
        'semantic_types': {
            'type': 'keyword',
            'index': True
        },
        'coverage': {
            'type': 'nested',
            'properties': {
                'range': {
                    'type': 'double_range'
                },
                # the following is needed so we can access this information
                #   inside the script, and this is not available for type
                #   'double_range'
                'gte': {
                    'type': 'double'
                },
                'lte': {
                    'type': 'double'
                }
            }
        }
    }
}


SPATIAL_COVERAGE_MAPPING = {
    'properties': {
        'lat': {
            'type': 'text'
        },
        'lon': {
            'type': 'text'
        },
        'dataset_id': {
            'type': 'text'
        },
        'dataset_name': {
            'type': 'text'
        },
        'dataset_description': {
            'type': 'text'
        },
        'ranges': {
            'type': 'nested',
            'properties': {
                'range': {
                    'type': 'geo_shape'
                },
                # the following is needed so we can access this information
                #   inside the script, and this is not available for type
                #   'geo_shape'
                'min_long': {
                    'type': 'double'
                },
                'max_lat': {
                    'type': 'double'
                },
                'max_long': {
                    'type': 'double'
                },
                'min_lat': {
                    'type': 'double'
                }
            }
        }
    }
}


def create_indices():
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )

    column_index_exists = False
    if es.indices.exists('datamart_columns'):
        print("Index for columns already exists -- skipping that.")
        column_index_exists = True
    else:
        es.indices.create(
            'datamart_columns',
            {
                'mappings': {
                    '_doc': COLUMN_MAPPING
                }
            }
        )

    spatial_coverage_index_exists = False
    if es.indices.exists('datamart_spatial_coverage'):
        print("Index for spatial coverage already exists -- skipping that.")
        spatial_coverage_index_exists = True
    else:
        es.indices.create(
            'datamart_spatial_coverage',
            {
                'mappings': {
                    '_doc': SPATIAL_COVERAGE_MAPPING
                }
            }
        )

    if column_index_exists and spatial_coverage_index_exists:
        sys.exit(0)

    body = {
        'query': {
            'match_all': {}
        }
    }

    from_ = 0
    result = es.search(
        index='datamart',
        body=body,
        from_=from_,
        size=100,
        request_timeout=30
    )

    size_ = len(result['hits']['hits'])

    while size_ > 0:
        for hit in result['hits']['hits']:
            common_dataset_info = dict()
            common_dataset_info["dataset_id"] = hit['_id']
            source = hit['_source']
            if 'name' in source:
                common_dataset_info['dataset_name'] = source['name']
            if 'description' in source:
                common_dataset_info['dataset_description'] = source['description']
            if not column_index_exists:
                columns = source['columns']
                for column in columns:
                    column_data = dict()
                    for key in common_dataset_info:
                        column_data[key] = common_dataset_info[key]
                    for key in column:
                        column_data[key] = column[key]
                    if 'coverage' in column_data:
                        for i in range(len(column_data['coverage'])):
                            column_data['coverage'][i]['gte'] = column_data['coverage'][i]['range']['gte']
                            column_data['coverage'][i]['lte'] = column_data['coverage'][i]['range']['lte']
                    es.index(
                        index='datamart_columns',
                        doc_type='_doc',
                        body=column_data
                    )
            if not spatial_coverage_index_exists and 'spatial_coverage' in source:
                spatial_coverages = source['spatial_coverage']
                for spatial_coverage in spatial_coverages:
                    spatial_coverage_data = dict()
                    for key in common_dataset_info:
                        spatial_coverage_data[key] = common_dataset_info[key]
                    for key in spatial_coverage:
                        spatial_coverage_data[key] = spatial_coverage[key]
                    for i in range(len(spatial_coverage_data['ranges'])):
                        spatial_coverage_data['ranges'][i]['min_long'] = \
                            spatial_coverage_data['ranges'][i]['range']['coordinates'][0][0]
                        spatial_coverage_data['ranges'][i]['max_lat'] = \
                            spatial_coverage_data['ranges'][i]['range']['coordinates'][0][1]
                        spatial_coverage_data['ranges'][i]['max_long'] = \
                            spatial_coverage_data['ranges'][i]['range']['coordinates'][1][0]
                        spatial_coverage_data['ranges'][i]['min_lat'] = \
                            spatial_coverage_data['ranges'][i]['range']['coordinates'][1][1]
                    es.index(
                        index='datamart_spatial_coverage',
                        doc_type='_doc',
                        body=spatial_coverage_data
                    )

        from_ += size_
        result = es.search(
            index='datamart',
            body=body,
            from_=from_,
            size=100,
            request_timeout=30
        )
        size_ = len(result['hits']['hits'])


if __name__ == '__main__':
    create_indices()
