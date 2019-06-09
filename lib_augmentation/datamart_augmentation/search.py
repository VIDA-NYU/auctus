import logging

from datamart_core.common import Type
from .utils import compute_levenshtein_sim

logger = logging.getLogger(__name__)

PAGINATION_SIZE = 10
JOIN_SIMILARITY_THRESHOLD = 0.3
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


def get_column_coverage(data_profile, filter_=[]):
    """
    Get coverage for each column of the input dataset.

    :param data_profile: Profiled input dataset, if dataset is not in DataMart index.
    :param filter_: list of column indices to return. If an empty list, return all the columns.
    :return: dict, where key is the column index, and value is a dict as follows:

        {
            'type': column meta-type ('structural_type', 'semantic_types', 'spatial'),
            'type_value': column type,
            'ranges': list of ranges
        }
    """

    column_coverage = dict()

    column_index = -1
    column_index_mapping = dict()
    for column in data_profile['columns']:
        column_index += 1
        column_name = column['name']
        column_index_mapping[column_name] = column_index
        if 'coverage' not in column:
            continue
        if filter_ and column_index not in filter_:
            continue
        # ignoring 'd3mIndex' for now -- seems useless
        if 'd3mIndex' in column_name:
            continue
        if Type.ID in column['semantic_types']:
            type_ = 'semantic_types'
            type_value = Type.ID
        # elif column['structural_type'] == Type.FLOAT:
        #     type_ = 'structural_type'
        #     type_value = column['structural_type']
        # elif column['structural_type'] == Type.INTEGER:
        #    type_ = 'structural_type'
        #    type_value = column['structural_type']
        elif Type.DATE_TIME in column['semantic_types']:
            type_ = 'semantic_types'
            type_value = Type.DATE_TIME
        else:
            continue
        column_coverage[str(column_index)] = {
            'type':       type_,
            'type_value': type_value,
            'ranges':     []
        }
        for range_ in column['coverage']:
            column_coverage[str(column_index)]['ranges'].\
                append([float(range_['range']['gte']),
                        float(range_['range']['lte'])])

    if 'spatial_coverage' in data_profile:
        for spatial in data_profile['spatial_coverage']:
            if filter_ and (
                    column_index_mapping[spatial['lat']] not in filter_ or
                    column_index_mapping[spatial['lon']] not in filter_):
                continue
            names = str(column_index_mapping[spatial['lat']]) + ',' +\
                    str(column_index_mapping[spatial['lat']])
            column_coverage[names] = {
                'type':      'spatial',
                'type_value': Type.LATITUDE + ', ' + Type.LONGITUDE,
                'ranges':     []
            }
            for range_ in spatial['ranges']:
                column_coverage[names]['ranges'].\
                    append(range_['range']['coordinates'])

    return column_coverage


def get_numerical_coverage_intersections(es, type_, type_value, pivot_column, ranges,
                                         dataset_id=None, query_args=None):
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
                        'inner_hits': {'_source': False, 'size': 100}
                    }
                }
            ]
        }

        if dataset_id:
            bool_query['must'].append(
                {'match': {'_id': dataset_id}}
            )

        intersection = {
            'nested': {
                'path': 'columns',
                'query': {
                    'bool': bool_query
                },
                'inner_hits': {'_source': False, 'size': 100}
            }
        }

        if not query_args:
            query_obj = {
                '_source': source_filter,
                'query': {
                    'bool': {
                        'filter': intersection,
                    }
                }
            }
        else:
            args = [intersection] + query_args
            query_obj = {
                '_source': source_filter,
                'query': {
                    'bool': {
                        'filter': {
                            'bool': {
                                'must': args,
                            },
                        },
                    },
                },
            }

        # logger.info("Query (numerical): %r", query_obj)

        from_ = 0
        result = es.search(
            index='datamart',
            body=query_obj,
            from_=from_,
            size=PAGINATION_SIZE,
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

                    sim = 1.0
                    if type_value != Type.DATE_TIME:
                        sim = compute_levenshtein_sim(
                            pivot_column.lower(),
                            column_name.lower()
                        )
                        if sim <= JOIN_SIMILARITY_THRESHOLD:
                            continue

                    name = '%s$$%d' % (dataset_name, column_offset)
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

                        intersections[name] += (end - start + 1) * sim

            # pagination
            from_ += size_
            result = es.search(
                index='datamart',
                body=query_obj,
                from_=from_,
                size=PAGINATION_SIZE,
                request_timeout=30
            )
            size_ = len(result['hits']['hits'])

    return intersections, column_total_coverage


def get_spatial_coverage_intersections(es, ranges, dataset_id=None,
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
            bool_query['must'] = {
                'match': {'_id': dataset_id}
            }

        intersection = {
            'nested': {
                'path': 'spatial_coverage.ranges',
                'query': {
                    'bool': bool_query
                },
                'inner_hits': {'_source': False, 'size': 100}
            }
        }

        if not query_args:
            query_obj = {
                '_source': source_filter,
                'query': {
                    'bool': {
                        'filter': intersection,
                    }
                }
            }
        else:
            args = [intersection] + query_args
            query_obj = {
                '_source': source_filter,
                'query': {
                    'bool': {
                        'filter': {
                            'bool': {
                                'must': args,
                            },
                        },
                    },
                },
            }

        # logger.info("Query (spatial): %r", query_obj)

        from_ = 0
        result = es.search(
            index='datamart',
            body=query_obj,
            from_=from_,
            size=PAGINATION_SIZE,
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
                    lat_index, lon_index = get_column_identifiers(
                        es,
                        [spatial_coverages[spatial_coverage_offset]['lat'],
                         spatial_coverages[spatial_coverage_offset]['lon']],
                        dataset_id=dataset_name
                    )
                    spatial_coverage_name = str(lat_index) + ',' + str(lon_index)
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
                size=PAGINATION_SIZE,
                request_timeout=30
            )
            size_ = len(result['hits']['hits'])

    return intersections, column_total_coverage


# TODO: ideally, this should be stored in the index
def get_column_identifiers(es, column_names, dataset_id=None, data_profile=None):
    column_indices = [-1 for _ in column_names]
    if not data_profile:
        columns = es.search(
            index='datamart',
            body={
                'query': {
                    'match': {
                        '_id': dataset_id,
                    }
                }
            }
        )['hits']['hits'][0]['_source']['columns']
    else:
        columns = data_profile['columns']
    for i in range(len(columns)):
        for j in range(len(column_names)):
            if columns[i]['name'] == column_names[j]:
                column_indices[j] = i
    return column_indices


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


def get_joinable_datasets(es, data_profile, dataset_id=None,
                          query_args=None, tabular_variables=[]):
    """
    Retrieve datasets that can be joined with an input dataset.

    :param es: Elasticsearch client.
    :param data_profile: Profiled input dataset.
    :param dataset_id: The identifier of the desired DataMart dataset for augmentation.
    :param query_args: list of query arguments (optional).
    :param tabular_variables: specifies which columns to focus on for the search.
    """

    if not dataset_id and not data_profile:
        raise RuntimeError('Either a dataset id or a data profile '
                           'must be provided for the join.')

    # get the coverage for each column of the input dataset

    intersections = dict()
    column_coverage = get_column_coverage(
        data_profile,
        tabular_variables
    )

    # get coverage intersections

    for column in column_coverage:
        type_ = column_coverage[column]['type']
        type_value = column_coverage[column]['type_value']
        if type_ == 'spatial':
            intersections_column, column_total_coverage = \
                get_spatial_coverage_intersections(
                    es,
                    column_coverage[column]['ranges'],
                    dataset_id,
                    query_args
                )
        else:
            try:
                column_name = data_profile['columns'][int(column)]['name']
            except:
                index_1, index_2 = column.split(',')
                column_name = data_profile['columns'][int(index_1)]['name'] +\
                              ',' + data_profile['columns'][int(index_2)]['name']
            intersections_column, column_total_coverage = \
                get_numerical_coverage_intersections(
                    es,
                    type_,
                    type_value,
                    column_name,
                    column_coverage[column]['ranges'],
                    dataset_id,
                    query_args
                )

        if not intersections_column:
            continue

        for name, size in intersections_column.items():
            score = size / column_total_coverage
            if score > 0:
                external_dataset, external_column = name.split('$$')
                if external_dataset not in intersections:
                    intersections[external_dataset] = []
                intersections[external_dataset].append(
                    (column, external_column, score)
                )

    # get pairs of columns with higher score

    all_pairs = []
    for dt in intersections:
        intersections[dt] = sorted(
            intersections[dt],
            key=lambda item: item[2],
            reverse=True
        )

        seen_1 = set()
        seen_2 = set()
        for column, external_column, score in intersections[dt]:
            if column in seen_1 or external_column in seen_2:
                continue
            seen_1.add(column)
            seen_2.add(external_column)
            all_pairs.append((column, dt, external_column, score))

    all_pairs = sorted(
        all_pairs,
        key=lambda item: item[3],
        reverse=True
    )

    results = []
    for column, dt, external_column, score in all_pairs:
        info = get_dataset_metadata(es, dt)
        meta = info.pop('_source')
        # materialize = meta.get('materialize', {})
        if 'description' in meta and len(meta['description']) > 100:
            meta['description'] = meta['description'][:100] + "..."
        left_columns = []
        right_columns = []
        left_columns_names = []
        try:
            left_columns.append([int(column)])
            left_columns_names.append(data_profile['columns'][int(column)]['name'])
        except ValueError:
            index_1, index_2 = column.split(",")
            left_columns.append([int(index_1), int(index_2)])
            left_columns_names.append(data_profile['columns'][int(index_1)]['name'] +
                                      ', ' + data_profile['columns'][int(index_2)]['name'])
        try:
            right_columns.append([int(external_column)])
        except ValueError:
            index_1, index_2 = external_column.split(",")
            right_columns.append([int(index_1), int(index_2)])
        results.append(dict(
            id=dt,
            score=score,
            # discoverer=materialize['identifier'],
            metadata=meta,
            augmentation={
                'type': 'join',
                'left_columns': left_columns,
                'right_columns': right_columns,
                'left_columns_names': left_columns_names
            }
        ))

    return results


def get_column_information(data_profile, filter_=[]):
    """
    Retrieve information about the columns (name and type) of a dataset.

    """

    output = dict()
    column_index = -1
    for column in data_profile['columns']:
        column_index += 1
        name = column['name']
        if filter_ and column_index not in filter_:
            continue
        # ignoring 'd3mIndex' for now -- seems useless
        if 'd3mIndex' in name:
            continue
        # ignoring phone numbers for now
        semantic_types = [
            sem for sem in column['semantic_types']
            if Type.PHONE_NUMBER not in sem
        ]
        for semantic_type in semantic_types:
            if semantic_type not in output:
                output[semantic_type] = []
            output[semantic_type].append(name)
        if not semantic_types:
            if column['structural_type'] not in output:
                output[column['structural_type']] = []
            output[column['structural_type']].append(name)
    return output


def get_unionable_datasets(es, data_profile, dataset_id=None,
                           query_args=None, tabular_variables=[]):
    """
    Retrieve datasets that can be unioned to an input dataset using fuzzy search
    (max edit distance = 2).

    :param es: Elasticsearch client.
    :param data_profile: Profiled input dataset.
    :param dataset_id: The identifier of the desired DataMart dataset for augmentation.
    :param query_args: list of query arguments (optional).
    :param tabular_variables: specifies which columns to focus on for the search.
    """

    if not dataset_id and not data_profile:
        raise RuntimeError('Either a dataset id or a data profile '
                           'must be provided for the union.')

    main_dataset_columns = get_column_information(
        data_profile=data_profile,
        filter_=tabular_variables
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
                'must': [
                    {
                        'fuzzy': {'columns.name.raw': att}
                    }
                ],
                'minimum_should_match': 1
            }

            if dataset_id:
                partial_query['must'].append(
                    {'match': {'_id': dataset_id}}
                )

            query = {
                'nested': {
                    'path': 'columns',
                    'query': {
                        'bool': partial_query
                    },
                    'inner_hits': {'_source': False, 'size': 100}
                }
            }

            if not query_args:
                query_obj = {
                    '_source': source_filter,
                    'query': {
                        'bool': {
                            'filter': query,
                        }
                    }
                }
            else:
                args = [query] + query_args
                query_obj = {
                    '_source': source_filter,
                    'query': {
                        'bool': {
                            'filter': {
                                'bool': {
                                    'must': args,
                                },
                            },
                        },
                    },
                }

            # logger.info("Query (union-fuzzy): %r", query_obj)

            from_ = 0
            result = es.search(
                index='datamart',
                body=query_obj,
                from_=from_,
                size=PAGINATION_SIZE,
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
                    size=PAGINATION_SIZE,
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

    sorted_datasets = sorted(
        scores.items(),
        key=lambda item: item[1],
        reverse=True
    )

    results = []
    for dt, score in sorted_datasets:
        info = get_dataset_metadata(es, dt)
        meta = info.pop('_source')
        # materialize = meta.get('materialize', {})
        if 'description' in meta and len(meta['description']) > 100:
            meta['description'] = meta['description'][:100] + "..."
        # TODO: augmentation information is incorrect
        left_columns = []
        right_columns = []
        left_columns_names = []
        for att_1, att_2, sim in column_pairs[dt]:
            if dataset_id:
                left_columns.append(
                    get_column_identifiers(es, [att_1], dataset_id=dataset_id)
                )
            else:
                left_columns.append(
                    get_column_identifiers(es, [att_1], data_profile=data_profile)
                )
            left_columns_names.append(att_1)
            right_columns.append(
                get_column_identifiers(es, [att_2], dataset_id=dt)
            )
        results.append(dict(
            id=dt,
            score=score,
            # discoverer=materialize['identifier'],
            metadata=meta,
            augmentation={
                'type': 'union',
                'left_columns': left_columns,
                'right_columns': right_columns,
                'left_columns_names': left_columns_names
            }
        ))

    return results
