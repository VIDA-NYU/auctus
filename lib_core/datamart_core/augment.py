import distance
import elasticsearch
import logging
import os
import pandas as pd
import shutil
import tempfile
import uuid

from .common import conv_datetime, conv_float, conv_int, Type

logger = logging.getLogger(__name__)
source_filter = {
    'excludes': [
        'date',
        'materialize',
        'name',
        'description',
        'license',
        'size',
        'columns.mean',
        'columns.stddev',
        'columns.structural_type',
        'columns.semantic_types'
    ]
}


def compute_levenshtein_sim(str1, str2):
    if len(str1) < 3:
        str1_set = [str1]
    else:
        str1_set = [str1[i:i + 3] for i in range(len(str1) - 2)]

    if len(str2) < 3:
        str2_set = [str2]
    else:
        str2_set = [str2[i:i + 3] for i in range(len(str2) - 2)]

    return 1 - distance.nlevenshtein(str1_set, str2_set, method=2)


def get_column_coverage(es, dataset_id, data_profile={}, filter_=[]):
    """
    Get coverage for each column of the input dataset.

    :param es:  Elasticsearch client.
    :param dataset_id: The identifier of the input dataset, if dataset is in DataMart index.
    :param data_profile: Profiled input dataset, if dataset is not in DataMart index.
    :param filter_: list of columns to return. If an empty list, return all the columns.
    :return: dict, where key is the column name, and value is a dict as follows:

        {
            'type': column meta-type ('structural_type', 'semantic_types', 'spatial'),
            'type_value': column type,
            'ranges': list of ranges
        }
    """

    column_coverage = dict()

    if dataset_id:
        index_query = {
            'query': {
                'match': {'_id': dataset_id}
            }
        }

        result = es.search(
            index='datamart',
            body=index_query
        )['hits']['hits'][0]['_source']
    else:
        result = data_profile

    for column in result['columns']:
        if 'coverage' not in column:
            continue
        column_name = column['name']
        if filter_ and column['name'].lower() not in filter_:
            continue
        # ignoring 'd3mIndex' for now -- seems useless
        if 'd3mIndex' in column_name:
            continue
        if Type.ID in column['semantic_types']:
            type_ = 'semantic_types'
            type_value = Type.ID
        elif column['structural_type'] in (Type.INTEGER, Type.FLOAT):
            type_ = 'structural_type'
            type_value = column['structural_type']
        elif Type.DATE_TIME in column['semantic_types']:
            type_ = 'semantic_types'
            type_value = Type.DATE_TIME
        else:
            continue
        column_coverage[column_name] = {
            'type':       type_,
            'type_value': type_value,
            'ranges':     []
        }
        for range_ in column['coverage']:
            column_coverage[column_name]['ranges'].\
                append([float(range_['range']['gte']),
                        float(range_['range']['lte'])])

    if 'spatial_coverage' in result:
        for spatial in result['spatial_coverage']:
            if filter_ and (spatial['lat'] not in filter_ or spatial['lon'] not in filter_):
                continue
            names = '(' + spatial['lat'] + ', ' + spatial['lon'] + ')'
            column_coverage[names] = {
                'type':      'spatial',
                'type_value': Type.LATITUDE + ', ' + Type.LONGITUDE,
                'ranges':     []
            }
            for range_ in spatial['ranges']:
                column_coverage[names]['ranges'].\
                    append(range_['range']['coordinates'])

    return column_coverage


def get_numerical_coverage_intersections(es, dataset_id, type_, type_value,
                                         ranges, query_args=None):
    """
    Retrieve numerical columns that intersect with the input numerical ranges.

    """

    intersections = dict()
    column_total_coverage = 0

    for range_ in ranges:
        column_total_coverage += (range_[1] - range_[0] + 1)

        bool_query = {
            'must': [
                {
                    'match': {'columns.%s' % type_: type_value}
                },
                {
                    'nested': {
                        'path': 'columns.coverage',
                        'query': {
                            'range': {
                                'columns.coverage.range': {
                                    'gte': range_[0],
                                    'lte': range_[1],
                                    'relation': 'intersects'
                                }
                            }
                        },
                        'inner_hits': {'_source': False}
                    }
                }
            ]
        }

        if dataset_id:
            bool_query['must_not'] = {
                'match': {'_id': dataset_id}
            }

        intersection = {
            'nested': {
                'path': 'columns',
                'query': {
                    'bool': bool_query
                },
                'inner_hits': {'_source': False}
            }
        }

        if not query_args:
            query_obj = {
                '_source': source_filter,
                'query': intersection,
            }
        else:
            args = [intersection] + query_args
            query_obj = {
                '_source': source_filter,
                'query': {
                    'bool': {
                        'must': args,
                    },
                },
            }

        # logger.info("Query (numerical): %r", query_obj)

        from_ = 0
        result = es.search(
            index='datamart',
            body=query_obj,
            from_=from_,
            size=100,
            request_timeout=30
        )

        size_ = len(result['hits']['hits'])

        while size_ > 0:
            for hit in result['hits']['hits']:

                dataset_name = hit['_id']
                columns = hit['_source']['columns']
                inner_hits = hit['inner_hits']

                for column_hit in inner_hits['columns']['hits']['hits']:
                    column_offset = int(column_hit['_nested']['offset'])
                    column_name = columns[column_offset]['name']
                    # ignoring 'd3mIndex' for now -- seems useless
                    if 'd3mIndex' in column_name:
                        continue
                    name = '%s$$%s' % (dataset_name, column_name)
                    if name not in intersections:
                        intersections[name] = 0

                    # ranges from column
                    for range_hit in column_hit['inner_hits']['columns.coverage']['hits']['hits']:
                        # compute intersection
                        range_offset = int(range_hit['_nested']['_nested']['offset'])
                        start_result = columns[column_offset]['coverage'][range_offset]['range']['gte']
                        end_result = columns[column_offset]['coverage'][range_offset]['range']['lte']

                        start = max(start_result, range_[0])
                        end = min(end_result, range_[1])

                        intersections[name] += (end - start + 1)

            # pagination
            from_ += size_
            result = es.search(
                index='datamart',
                body=query_obj,
                from_=from_,
                size=100,
                request_timeout=30
            )
            size_ = len(result['hits']['hits'])

    return intersections, column_total_coverage


def get_spatial_coverage_intersections(es, dataset_id, ranges,
                                       query_args=None):
    """
    Retrieve spatial columns that intersect with the input spatial ranges.

    """

    intersections = dict()
    column_total_coverage = 0

    for range_ in ranges:
        column_total_coverage += \
            (range_[1][0] - range_[0][0]) * (range_[0][1] - range_[1][1])

        bool_query = {
            'filter': {
                'geo_shape': {
                    'spatial_coverage.ranges.range': {
                        'shape': {
                            'type': 'envelope',
                            'coordinates': [
                                [range_[0][0], range_[0][1]],
                                [range_[1][0], range_[1][1]]
                            ]
                        },
                        'relation': 'intersects'
                    }
                }
            }
        }

        if dataset_id:
            bool_query['must_not'] = {
                'match': {'_id': dataset_id}
            }

        intersection = {
            'nested': {
                'path': 'spatial_coverage.ranges',
                'query': {
                    'bool': bool_query
                },
                'inner_hits': {'_source': False}
            }
        }

        if not query_args:
            query_obj = {
                '_source': source_filter,
                'query': intersection,
            }
        else:
            args = [intersection] + query_args
            query_obj = {
                '_source': source_filter,
                'query': {
                    'bool': {
                        'must': args,
                    },
                },
            }

        # logger.info("Query (spatial): %r", query_obj)

        from_ = 0
        result = es.search(
            index='datamart',
            body=query_obj,
            from_=from_,
            size=100,
            request_timeout=30
        )

        size_ = len(result['hits']['hits'])

        while size_ > 0:
            for hit in result['hits']['hits']:

                dataset_name = hit['_id']
                spatial_coverages = hit['_source']['spatial_coverage']
                inner_hits = hit['inner_hits']

                for coverage_hit in inner_hits['spatial_coverage.ranges']['hits']['hits']:
                    spatial_coverage_offset = int(coverage_hit['_nested']['offset'])
                    spatial_coverage_name = \
                        '(' + spatial_coverages[spatial_coverage_offset]['lat'] + ', ' \
                        + spatial_coverages[spatial_coverage_offset]['lon'] + ')'
                    name = '%s$$%s' % (dataset_name, spatial_coverage_name)
                    if name not in intersections:
                        intersections[name] = 0

                    # compute intersection
                    range_offset = int(coverage_hit['_nested']['_nested']['offset'])
                    min_lon = \
                        spatial_coverages[spatial_coverage_offset]['ranges'][range_offset]['range']['coordinates'][0][0]
                    max_lat = \
                        spatial_coverages[spatial_coverage_offset]['ranges'][range_offset]['range']['coordinates'][0][1]
                    max_lon = \
                        spatial_coverages[spatial_coverage_offset]['ranges'][range_offset]['range']['coordinates'][1][0]
                    min_lat = \
                        spatial_coverages[spatial_coverage_offset]['ranges'][range_offset]['range']['coordinates'][1][1]

                    n_min_lon = max(min_lon, range_[0][0])
                    n_max_lat = min(max_lat, range_[0][1])
                    n_max_lon = max(max_lon, range_[1][0])
                    n_min_lat = min(min_lat, range_[1][1])

                    intersections[name] += (n_max_lon - n_min_lon) * (n_max_lat - n_min_lat)

            # pagination
            from_ += size_
            result = es.search(
                index='datamart',
                body=query_obj,
                from_=from_,
                size=100,
                request_timeout=30
            )
            size_ = len(result['hits']['hits'])

    return intersections, column_total_coverage


def get_dataset_metadata(es, dataset_id):
    """
    Retrieve metadata about input dataset.

    """

    hit = es.search(
        index='datamart',
        body={
            'query': {
                'match': {
                    '_id': dataset_id,
                }
            }
        }
    )['hits']['hits'][0]

    return hit


def get_joinable_datasets(es, dataset_id=None, data_profile={},
                          query_args=None, search_columns={}):
    """
    Retrieve datasets that can be joined with an input dataset.

    :param es: Elasticsearch client.
    :param dataset_id: The identifier of the input dataset, if dataset is in DataMart index.
    :param data_profile: Profiled input dataset, if dataset is not in DataMart index.
    :param query_args: list of query arguments (optional).
    :param search_columns: specifies which columns to focus on for the search.
    """

    if not dataset_id and not data_profile:
        raise RuntimeError('Either a dataset id or a data profile '
                           'must be provided for the join.')

    # search columns

    required_columns = [] if 'required' not in search_columns else search_columns['required']
    desired_columns = [] if 'desired' not in search_columns else search_columns['desired']

    # get the coverage for each column of the input dataset

    intersections = dict()
    column_coverage = get_column_coverage(
        es,
        dataset_id,
        data_profile,
        required_columns
    )

    # get coverage intersections

    for column in column_coverage:
        type_ = column_coverage[column]['type']
        type_value = column_coverage[column]['type_value']
        if type_ == 'spatial':
            intersections_column, column_total_coverage = \
                get_spatial_coverage_intersections(
                    es,
                    dataset_id,
                    column_coverage[column]['ranges'],
                    query_args
                )
        else:
            intersections_column, column_total_coverage = \
                get_numerical_coverage_intersections(
                    es,
                    dataset_id,
                    type_,
                    type_value,
                    column_coverage[column]['ranges'],
                    query_args
                )

        if not intersections_column:
            continue

        for name, size in intersections_column.items():
            sim = compute_levenshtein_sim(
                column.lower(),
                name.split("$$")[1].lower()
            )
            score = size / column_total_coverage
            if type_value not in (Type.DATE_TIME,
                                  Type.LATITUDE + ', ' + Type.LONGITUDE):
                score *= sim
            if score > 0:
                external_dataset, external_column = name.split('$$')
                if external_dataset not in intersections:
                    intersections[external_dataset] = []
                intersections[external_dataset].append(
                    (column, external_column, score)
                )

    # get pairs of columns with higher score

    for dt in intersections:
        intersections[dt] = sorted(
            intersections[dt],
            key=lambda item: item[2],
            reverse=True
        )

        seen_1 = set()
        seen_2 = set()
        pairs = []
        for column, external_column, score in intersections[dt]:
            if column in seen_1 or external_column in seen_2:
                continue
            seen_1.add(column)
            seen_2.add(external_column)
            pairs.append((column, external_column, score))
        intersections[dt] = pairs

    # filtering based on search columns

    if required_columns or desired_columns:
        for dt in list(intersections.keys()):
            required = 0
            desired = 0
            for att_1, att_2, sim in intersections[dt]:
                if required_columns and att_1.lower() in required_columns:
                    required += 1
                if desired_columns and att_1.lower() in desired_columns:
                    desired += 1
            if required_columns and required < len(required_columns):
                del intersections[dt]
            elif desired_columns and desired == 0:
                del intersections[dt]

    # sorting datasets based on the column with highest score

    sorted_datasets = []
    for dt in intersections:
        items = intersections[dt]
        sorted_datasets.append((dt, items[0][2]))
    sorted_datasets = sorted(
        sorted_datasets,
        key=lambda item: item[1],
        reverse=True
    )

    results = []
    for dt, score in sorted_datasets:
        info = get_dataset_metadata(es, dt)
        meta = info.pop('_source')
        materialize = meta.get('materialize', {})
        if 'description' in meta and len(meta['description']) > 100:
            meta['description'] = meta['description'][:100] + "..."
        results.append(dict(
            id=dt,
            score=score,
            discoverer=materialize['identifier'],
            metadata=meta,
            columns=[(att_1, att_2) for att_1, att_2, sim in intersections[dt]],
        ))

    return {'results': results}


def get_column_information(es=None, dataset_id=None, data_profile={},
                           query_args=None, filter_=[]):
    """
    Retrieve information about the columns (name and type) of either
    all of the datasets, or the input dataset.

    """

    def store_column_information(metadata, filter_=[]):
        output = dict()
        for column in metadata['columns']:
            name = column['name']
            if filter_ and column['name'] not in filter_:
                continue
            # ignoring 'd3mIndex' for now -- seems useless
            if 'd3mIndex' in name:
                continue
            for semantic_type in column['semantic_types']:
                if semantic_type not in output:
                    output[semantic_type] = []
                output[semantic_type].append(name)
            if not column['semantic_types']:
                if column['structural_type'] not in output:
                    output[column['structural_type']] = []
                output[column['structural_type']].append(name)
        return output

    if data_profile:
        return store_column_information(data_profile, filter_)

    dataset_columns = dict()

    if dataset_id:
        query = {
            'match': {
                '_id': dataset_id,
            }
        }
    else:
        query = {
            'match_all': {}
        }

    if not query_args:
        query_obj = {
            'query': query,
        }
    else:
        args = [query] + query_args
        query_obj = {
            'query': {
                'bool': {
                    'must': args,
                },
            },
        }

    # logger.info("Query: %r", query_obj)

    from_ = 0
    result = es.search(
        index='datamart',
        body=query_obj,
        from_=from_,
        size=100,
        request_timeout=30
    )

    size_ = len(result['hits']['hits'])

    while size_ > 0:
        for hit in result['hits']['hits']:
            dataset = hit['_id']
            dataset_columns[dataset] = store_column_information(hit['_source'], filter_)

        # pagination
        from_ += size_
        result = es.search(
            index='datamart',
            body=query_obj,
            from_=from_,
            size=100,
            request_timeout=30
        )
        size_ = len(result['hits']['hits'])

    return dataset_columns


def get_unionable_datasets_brute_force(es, dataset_id=None, data_profile={},
                                       query_args=None, search_columns={}):
    """
    Retrieve datasets that can be unioned to an input dataset using a brute force approach.

    :param es: Elasticsearch client.
    :param dataset_id: The identifier of the input dataset.
    :param data_profile: Profiled input dataset, if dataset is not in DataMart index.
    :param query_args: list of query arguments (optional).
    :param search_columns: specifies which columns to focus on for the search.
    """

    # search columns

    required_columns = [] if 'required' not in search_columns else search_columns['required']
    desired_columns = [] if 'desired' not in search_columns else search_columns['desired']

    dataset_columns = get_column_information(es=es, query_args=query_args)
    if dataset_id:
        if dataset_id in dataset_columns:
            del dataset_columns[dataset_id]
        main_dataset_columns = get_column_information(
            es=es,
            dataset_id=dataset_id,
            filter_=required_columns
        )[dataset_id]
    else:
        main_dataset_columns = get_column_information(
            data_profile=data_profile,
            filter_=required_columns
        )

    n_columns = 0
    for type_ in main_dataset_columns:
        n_columns += len(main_dataset_columns[type_])

    column_pairs = dict()
    scores = dict()
    for dataset in list(dataset_columns.keys()):

        # check all pairs of attributes
        pairs = []
        for type_ in main_dataset_columns:
            if type_ not in dataset_columns[dataset]:
                continue
            for att_1 in main_dataset_columns[type_]:
                for att_2 in dataset_columns[dataset][type_]:
                    sim = compute_levenshtein_sim(att_1.lower(), att_2.lower())
                    pairs.append((att_1, att_2, sim))

        # choose pairs with higher Jaccard distance
        seen_1 = set()
        seen_2 = set()
        column_pairs[dataset] = []
        for att_1, att_2, sim in sorted(pairs,
                                        key=lambda item: item[2],
                                        reverse=True):
            if att_1 in seen_1 or att_2 in seen_2:
                continue
            seen_1.add(att_1)
            seen_2.add(att_2)
            column_pairs[dataset].append((att_1, att_2, sim))

        if len(column_pairs[dataset]) <= 1:
            del column_pairs[dataset]
            continue

        scores[dataset] = 0

        for i in range(len(column_pairs[dataset])):
            sim = column_pairs[dataset][i][2]
            scores[dataset] += sim

    scores[dataset] = scores[dataset] / n_columns

    # filtering based on search columns

    if required_columns or desired_columns:
        for dt in list(column_pairs.keys()):
            required = 0
            desired = 0
            for att_1, att_2, sim in column_pairs[dt]:
                if required_columns and att_1.lower() in required_columns:
                    required += 1
                if desired_columns and att_1.lower() in desired_columns:
                    desired += 1
            if required_columns and required < len(required_columns):
                del column_pairs[dt]
                del scores[dt]
            elif desired_columns and desired == 0:
                del column_pairs[dt]
                del scores[dt]

    sorted_datasets = sorted(
        scores.items(),
        key=lambda item: item[1],
        reverse=True
    )

    results = []
    for dt, score in sorted_datasets:
        info = get_dataset_metadata(es, dt)
        meta = info.pop('_source')
        materialize = meta.get('materialize', {})
        if 'description' in meta and len(meta['description']) > 100:
            meta['description'] = meta['description'][:100] + "..."
        results.append(dict(
            id=dt,
            score=score,
            discoverer=materialize['identifier'],
            metadata=meta,
            columns=[(att_1, att_2) for att_1, att_2, sim in column_pairs[dt]],
        ))

    return {'results': results}


def get_unionable_datasets_fuzzy(es, dataset_id=None, data_profile={},
                                 query_args=None, search_columns={}):
    """
    Retrieve datasets that can be unioned to an input dataset using fuzzy search
    (max edit distance = 2).

    :param es: Elasticsearch client.
    :param dataset_id: The identifier of the input dataset.
    :param data_profile: Profiled input dataset, if dataset is not in DataMart index.
    :param query_args: list of query arguments (optional).
    :param search_columns: specifies which columns to focus on for the search.
    """

    # search columns

    required_columns = [] if 'required' not in search_columns else search_columns['required']
    desired_columns = [] if 'desired' not in search_columns else search_columns['desired']

    if dataset_id:
        main_dataset_columns = get_column_information(
            es=es,
            dataset_id=dataset_id,
            filter_=required_columns
        )[dataset_id]
    else:
        main_dataset_columns = get_column_information(
            data_profile=data_profile,
            filter_=required_columns
        )

    n_columns = 0
    for type_ in main_dataset_columns:
        n_columns += len(main_dataset_columns[type_])

    column_pairs = dict()
    for type_ in main_dataset_columns:
        for att in main_dataset_columns[type_]:
            partial_query = {
                'should': [
                    {
                        'match': {'columns.structural_type': type_}
                    },
                    {
                        'match': {'columns.semantic_types': type_}
                    },
                ],
                'must': {
                    "fuzzy": {"columns.name.raw": att}
                },
                "minimum_should_match": 1
            }

            if dataset_id:
                partial_query['must_not'] = {
                    'match': {'_id': dataset_id}
                }

            query = {
                'nested': {
                    'path': 'columns',
                    'query': {
                        'bool': partial_query
                    },
                    'inner_hits': {'_source': False}
                }
            }

            if not query_args:
                query_obj = {
                    '_source': source_filter,
                    'query': query,
                }
            else:
                args = [query] + query_args
                query_obj = {
                    '_source': source_filter,
                    'query': {
                        'bool': {
                            'must': args,
                        },
                    },
                }

            # logger.info("Query (union-fuzzy): %r", query_obj)

            from_ = 0
            result = es.search(
                index='datamart',
                body=query_obj,
                from_=from_,
                size=100,
                request_timeout=30
            )

            size_ = len(result['hits']['hits'])

            while size_ > 0:
                for hit in result['hits']['hits']:

                    dataset_name = hit['_id']
                    columns = hit['_source']['columns']
                    inner_hits = hit['inner_hits']

                    if dataset_name not in column_pairs:
                        column_pairs[dataset_name] = []

                    for column_hit in inner_hits['columns']['hits']['hits']:
                        column_offset = int(column_hit['_nested']['offset'])
                        column_name = columns[column_offset]['name']
                        sim = compute_levenshtein_sim(att.lower(), column_name.lower())
                        column_pairs[dataset_name].append((att, column_name, sim))

                # pagination
                from_ += size_
                result = es.search(
                    index='datamart',
                    body=query_obj,
                    from_=from_,
                    size=100,
                    request_timeout=30
                )
                size_ = len(result['hits']['hits'])

    scores = dict()
    for dataset in list(column_pairs.keys()):

        # choose pairs with higher similarity
        seen_1 = set()
        seen_2 = set()
        pairs = []
        for att_1, att_2, sim in sorted(column_pairs[dataset],
                                        key=lambda item: item[2],
                                        reverse=True):
            if att_1 in seen_1 or att_2 in seen_2:
                continue
            seen_1.add(att_1)
            seen_2.add(att_2)
            pairs.append((att_1, att_2, sim))

        if len(pairs) <= 1:
            del column_pairs[dataset]
            continue

        column_pairs[dataset] = pairs
        scores[dataset] = 0

        for i in range(len(column_pairs[dataset])):
            sim = column_pairs[dataset][i][2]
            scores[dataset] += sim

        scores[dataset] = scores[dataset] / n_columns

    # filtering based on search columns

    if required_columns or desired_columns:
        for dt in list(column_pairs.keys()):
            required = 0
            desired = 0
            for att_1, att_2, sim in column_pairs[dt]:
                if required_columns and att_1.lower() in required_columns:
                    required += 1
                if desired_columns and att_1.lower() in desired_columns:
                    desired += 1
            if required_columns and required < len(required_columns):
                del column_pairs[dt]
                del scores[dt]
            elif desired_columns and desired == 0:
                del column_pairs[dt]
                del scores[dt]

    sorted_datasets = sorted(
        scores.items(),
        key=lambda item: item[1],
        reverse=True
    )

    results = []
    for dt, score in sorted_datasets:
        info = get_dataset_metadata(es, dt)
        meta = info.pop('_source')
        materialize = meta.get('materialize', {})
        if 'description' in meta and len(meta['description']) > 100:
            meta['description'] = meta['description'][:100] + "..."
        results.append(dict(
            id=dt,
            score=score,
            discoverer=materialize['identifier'],
            metadata=meta,
            columns=[(att_1, att_2) for att_1, att_2, sim in column_pairs[dt]],
        ))

    return {'results': results}


def get_unionable_datasets(es, dataset_id=None, data_profile={},
                           query_args=None, fuzzy=False, search_columns={}):
    """
    Retrieve datasets that can be unioned to an input dataset.

    :param es: Elasticsearch client.
    :param dataset_id: The identifier of the input dataset.
    :param data_profile: Profiled input dataset, if dataset is not in DataMart index.
    :param query_args: list of query arguments (optional).
    :param fuzzy: if True, applies fuzzy search instead of looking for all of the datasets.
    :param search_columns: specifies which columns to focus on for the search.
    """

    if not dataset_id and not data_profile:
        raise RuntimeError('Either a dataset id or a data profile '
                           'must be provided for the union.')

    if fuzzy:
        return get_unionable_datasets_fuzzy(
            es,
            dataset_id,
            data_profile,
            query_args,
            search_columns
        )
    return get_unionable_datasets_brute_force(
        es,
        dataset_id,
        data_profile,
        query_args,
        search_columns
    )


def convert_to_pd(file_path, columns_metadata):
    """
    Convert a dataset to pandas.DataFrame based on the provided metadata.
    """

    converters = dict()

    for column in columns_metadata:
        name = column['name']
        if Type.DATE_TIME in column['semantic_types']:
            converters[name] = conv_datetime
        elif Type.INTEGER in column['structural_type']:
            converters[name] = conv_int
        elif Type.FLOAT in column['structural_type']:
            converters[name] = conv_float

    return pd.read_csv(
        file_path,
        converters=converters,
        error_bad_lines=False
    )


def materialize_dataset(es, dataset_id):
    """
    Materializes a dataset as a pandas.DataFrame
    """

    from .materialize import get_dataset

    # get metadata data from Elasticsearch
    try:
        metadata = es.get('datamart', '_doc', id=dataset_id)['_source']
    except elasticsearch.NotFoundError:
        raise RuntimeError('Dataset id not found in Elasticsearch.')

    getter = get_dataset(metadata, dataset_id, format='csv')
    try:
        dataset_path = getter.__enter__()
        df = convert_to_pd(dataset_path, metadata['columns'])
    except Exception:
        raise RuntimeError('Materializer reports failure.')
    finally:
        getter.__exit__(None, None, None)

    return df


def join(original_data, augment_data, columns, how='inner'):
    """
    Performs an inner join between original_data (pandas.DataFrame)
    and augment_data (pandas.DataFrame) using columns.

    Returns the new pandas.DataFrame object.
    """

    rename = dict()
    for c in columns:
        rename[c[1]] = c[0]
    augment_data = augment_data.rename(columns=rename)
    join_ = pd.merge(
        original_data,
        augment_data,
        how=how,
        on=[c[0] for c in columns],
        suffixes=('_l', '_r')
    )

    # remove all columns with 'd3mIndex'
    join_ = join_.drop([c for c in join_.columns if 'd3mIndex' in c], axis=1)

    # create a single d3mIndex
    join_['d3mIndex'] = pd.Series(
        data=[i for i in range(join_.shape[0])],
        index=join_.index
    )

    return join_


def union(original_data, augment_data, columns):
    """
    Performs a union between original_data (pandas.DataFrame)
    and augment_data (pandas.DataFrame) using columns.

    Returns the new pandas.DataFrame object.
    """

    # dropping columns not in union
    original_columns = [c[0] for c in columns]
    augment_data_columns = [c[1] for c in columns]
    original_data = original_data.drop(
        [c for c in original_data.columns if c not in original_columns],
        axis=1
    )
    augment_data = augment_data.drop(
        [c for c in augment_data.columns if c not in augment_data_columns],
        axis=1
    )

    rename = dict()
    for c in columns:
        rename[c[1]] = c[0]
    augment_data = augment_data.rename(columns=rename)
    union_ = pd.concat([original_data, augment_data])

    # create a single d3mIndex
    union_['d3mIndex'] = pd.Series(
        data=[i for i in range(union_.shape[0])],
        index=union_.index
    )

    return union_


def generate_d3m_dataset(data, destination=None):
    """
    Generates a D3M dataset from data (pandas.DataFrame).

    Returns the path to the D3M-style directory.
    """

    from datamart_materialize.d3m import D3mWriter

    def add_column(column_, type_, metadata_):
        metadata_['columns'].append({'name': column_})
        if 'datetime' in type_:
            metadata_['columns'][-1]['semantic_types'] = [Type.DATE_TIME]
            metadata_['columns'][-1]['structural_type'] = Type.TEXT
        elif 'int' in type_:
            metadata_['columns'][-1]['semantic_types'] = []
            metadata_['columns'][-1]['structural_type'] = Type.INTEGER
        elif 'float' in type_:
            metadata_['columns'][-1]['semantic_types'] = []
            metadata_['columns'][-1]['structural_type'] = Type.FLOAT
        elif 'bool' in type_:
            metadata_['columns'][-1]['semantic_types'] = []
            metadata_['columns'][-1]['structural_type'] = Type.BOOLEAN
        else:
            metadata_['columns'][-1]['semantic_types'] = []
            metadata_['columns'][-1]['structural_type'] = Type.TEXT

    dir_name = uuid.uuid4().hex
    if destination:
        data_path = os.path.join(destination, dir_name)
        if os.path.exists(data_path):
            shutil.rmtree(data_path)
    else:
        temp_dir = tempfile.mkdtemp()
        data_path = os.path.join(temp_dir, dir_name)

    metadata = dict(columns=[])
    for column in data.columns:
        add_column(
            column,
            str(data[column].dtype),
            metadata
        )
    metadata['size'] = data.memory_usage(index=True, deep=True).sum()

    writer = D3mWriter(dir_name, data_path, metadata)
    with writer.open_file('w') as fp:
        data.to_csv(fp, index=False)

    return data_path


def augment(es, data, metadata, task, destination=None):
    """
    Augments original data based on the task.

    :param es: Elasticsearch client.
    :param data: the data to be augmented.
    :param metadata: the metadata of the data to be augmented.
    :param task: the augmentation task.
    :param destination: location to save the files.
    """

    if 'id' not in task:
        raise RuntimeError('Dataset id for the augmentation task not provided.')

    if 'join_columns' in task and len(task['join_columns']) > 0:
        join_ = join(
            convert_to_pd(data, metadata['columns']),
            materialize_dataset(es, task['id']),
            task['join_columns']
        )
        return generate_d3m_dataset(join_, destination)
    elif 'union_columns' in task and len(task['union_columns']) > 0:
        union_ = union(
            convert_to_pd(data, metadata['columns']),
            materialize_dataset(es, task['id']),
            task['union_columns']
        )
        return generate_d3m_dataset(union_, destination)
    else:
        raise RuntimeError('Augmentation task not provided.')
