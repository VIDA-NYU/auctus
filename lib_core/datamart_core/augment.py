import distance
import logging

from .common import Type

logger = logging.getLogger(__name__)


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


def get_column_coverage(es, dataset_id, data_profile={}):
    """
    Get coverage for each column of the input dataset.

    :param es:  Elasticsearch client.
    :param dataset_id: The identifier of the input dataset, if dataset is in DataMart index.
    :param data_profile: Profiled input dataset, if dataset is not in DataMart index.
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
                    'match': {'columns.%s'%type_: type_value}
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
                'query': intersection,
            }
        else:
            args = [intersection] + query_args
            query_obj = {
                'query': {
                    'bool': {
                        'must': args,
                    },
                },
            }

        logger.info("Query (numerical): %r", query_obj)

        result = es.search(
            index='datamart',
            body=query_obj,
            scroll='2m',
            size=10000
        )

        sid = result['_scroll_id']
        scroll_size = result['hits']['total']

        while scroll_size > 0:
            for hit in result['hits']['hits']:

                dataset_name = hit['_id']
                columns = hit['_source']['columns']
                inner_hits = hit['inner_hits']

                for column_hit in inner_hits['columns']['hits']['hits']:
                    column_offset = int(column_hit['_nested']['offset'])
                    column_name = columns[column_offset]['name']
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

            # scrolling
            result = es.scroll(
                scroll_id=sid,
                scroll='2m'
            )
            sid = result['_scroll_id']
            scroll_size = len(result['hits']['hits'])

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
            (range_[1][0] - range_[0][0])*(range_[0][1] - range_[1][1])

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
                'query': intersection,
            }
        else:
            args = [intersection] + query_args
            query_obj = {
                'query': {
                    'bool': {
                        'must': args,
                    },
                },
            }

        logger.info("Query (spatial): %r", query_obj)

        result = es.search(
            index='datamart',
            body=query_obj,
            scroll='2m',
            size=10000
        )

        sid = result['_scroll_id']
        scroll_size = result['hits']['total']

        while scroll_size > 0:
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

                    intersections[name] += (n_max_lon - n_min_lon)*(n_max_lat - n_min_lat)

            # scrolling
            result = es.scroll(
                scroll_id=sid,
                scroll='2m'
            )
            sid = result['_scroll_id']
            scroll_size = len(result['hits']['hits'])

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


def get_joinable_datasets(es, dataset_id, data_profile={}, query_args=None):
    """
    Retrieve datasets that can be joined with an input dataset.

    :param es: Elasticsearch client.
    :param dataset_id: The identifier of the input dataset, if dataset is in DataMart index.
    :param data_profile: Profiled input dataset, if dataset is not in DataMart index.
    :param query_args: list of query arguments (optional).
    """

    # get the coverage for each column of the input dataset

    intersections = dict()
    column_coverage = get_column_coverage(es, dataset_id, data_profile)

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
            columns=intersections[dt],
        ))

    return {'results': results}


def get_column_information(es=None, dataset_id=None, data_profile={}, query_args=None):
    """
    Retrieve information about the columns (name and type) of either
    all of the datasets, or the input dataset.

    """

    def store_column_information(metadata):
        output = dict()
        for column in metadata['columns']:
            name = column['name']
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
        return store_column_information(data_profile)

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

    logger.info("Query: %r", query_obj)

    result = es.search(
        index='datamart',
        body=query_obj,
        scroll='2m',
        size=10000
    )

    sid = result['_scroll_id']
    scroll_size = result['hits']['total']

    while scroll_size > 0:
        for hit in result['hits']['hits']:
            dataset = hit['_id']
            dataset_columns[dataset] = store_column_information(hit['_source'])

        # scrolling
        result = es.scroll(
            scroll_id=sid,
            scroll='2m'
        )
        sid = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])

    return dataset_columns


def get_unionable_datasets(es, dataset_id, data_profile={}, query_args=None):
    """
    Retrieve datasets that can be unioned to an input dataset.

    :param es: Elasticsearch client.
    :param dataset_id: The identifier of the input dataset.
    :param data_profile: Profiled input dataset, if dataset is not in DataMart index.
    :param query_args: list of query arguments (optional).
   """

    dataset_columns = get_column_information(es=es, query_args=query_args)
    if dataset_id:
        if dataset_id in dataset_columns:
            del dataset_columns[dataset_id]
        main_dataset_columns = get_column_information(
            es=es,
            dataset_id=dataset_id
        )[dataset_id]
    else:
        main_dataset_columns = get_column_information(
            data_profile=data_profile
        )

    n_columns = 0
    for type_ in main_dataset_columns:
        n_columns += len(main_dataset_columns[type_])

    column_pairs = dict()
    scores = dict()
    for dataset in dataset_columns:

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
            column_pairs[dataset] = []
            continue

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
        materialize = meta.get('materialize', {})
        if 'description' in meta and len(meta['description']) > 100:
            meta['description'] = meta['description'][:100] + "..."
        results.append(dict(
            id=dt,
            score=score,
            discoverer=materialize['identifier'],
            metadata=meta,
            columns=column_pairs[dt],
        ))

    return {'results': results}
