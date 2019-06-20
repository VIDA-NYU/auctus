from datetime import datetime
from dateutil.parser import parse
import hashlib
import logging
import os
import pickle
import tempfile
import time

from datamart_augmentation.search import \
    get_joinable_datasets, get_unionable_datasets
from datamart_core.common import Type
from datamart_profiler import process_dataset


logger = logging.getLogger(__name__)


BUF_SIZE = 128000


class ClientError(ValueError):
    """Error in query sent by client.
    """


def parse_query(query_json):
    """Parses a Datamart query, turning it into an Elasticsearch query.
    """
    query_args = list()

    # keywords
    keywords_query_all = list()
    if 'keywords' in query_json and query_json['keywords']:
        if not isinstance(query_json['keywords'], list):
            raise ClientError("'keywords' must be an array")
        keywords_query = list()
        # description
        keywords_query.append({
            'match': {
                'description': {
                    'query': ' '.join(query_json['keywords']),
                    'operator': 'or'
                }
            }
        })
        # name
        keywords_query.append({
            'match': {
                'name': {
                    'query': ' '.join(query_json['keywords']),
                    'operator': 'or'
                }
            }
        })
        # keywords
        for name in query_json['keywords']:
            keywords_query.append({
                'nested': {
                    'path': 'columns',
                    'query': {
                        'match': {'columns.name': name},
                    },
                },
            })
            keywords_query.append({
                'wildcard': {
                    'materialize.identifier': '*%s*' % name.lower()
                }
            })
        keywords_query_all.append({
            'bool': {
                'should': keywords_query,
                'minimum_should_match': 1
            }
        })

    if keywords_query_all:
        query_args.append(keywords_query_all)

    # tabular_variables
    tabular_variables = []

    # variables
    variables_query = None
    if 'variables' in query_json:
        variables_query = parse_query_variables(
            query_json['variables'],
            tabular_variables=tabular_variables
        )

    if variables_query:
        query_args.append(variables_query)

    return query_args, list(set(tabular_variables))


def parse_query_variables(data, tabular_variables=None):
    output = list()

    if not data:
        return output

    for variable in data:
        if 'type' not in variable:
            raise ClientError("variable is missing property 'type'")
        variable_query = list()

        # temporal variable
        # TODO: handle 'granularity'
        if 'temporal_variable' in variable['type']:
            variable_query.append({
                'nested': {
                    'path': 'columns',
                    'query': {
                        'match': {'columns.semantic_types': Type.DATE_TIME},
                    },
                },
            })
            start = end = None
            if 'start' in variable and 'end' in variable:
                try:
                    start = parse(variable['start']).timestamp()
                    end = parse(variable['end']).timestamp()
                except (KeyError, ValueError, OverflowError):
                    pass
            elif 'start' in variable:
                try:
                    start = parse(variable['start']).timestamp()
                    end = datetime.now().timestamp()
                except (KeyError, ValueError, OverflowError):
                    pass
            elif 'end' in variable:
                try:
                    start = 0
                    end = parse(variable['end']).timestamp()
                except (KeyError, ValueError, OverflowError):
                    pass
            else:
                pass
            if start and end:
                variable_query.append({
                    'nested': {
                        'path': 'columns.coverage',
                        'query': {
                            'range': {
                                'columns.coverage.range': {
                                    'gte': start,
                                    'lte': end,
                                    'relation': 'intersects'
                                }
                            }
                        }
                    }
                })

        # geospatial variable
        # TODO: handle 'granularity'
        elif 'geospatial_variable' in variable['type']:
            if ('latitude1' not in variable or
                    'latitude2' not in variable or
                    'longitude1' not in variable or
                    'longitude2' not in variable):
                continue
            longitude1 = min(
                float(variable['longitude1']),
                float(variable['longitude2'])
            )
            longitude2 = max(
                float(variable['longitude1']),
                float(variable['longitude2'])
            )
            latitude1 = max(
                float(variable['latitude1']),
                float(variable['latitude2'])
            )
            latitude2 = min(
                float(variable['latitude1']),
                float(variable['latitude2'])
            )
            variable_query.append({
                'nested': {
                    'path': 'spatial_coverage.ranges',
                    'query': {
                        'bool': {
                            'filter': {
                                'geo_shape': {
                                    'spatial_coverage.ranges.range': {
                                        'shape': {
                                            'type': 'envelope',
                                            'coordinates':
                                                [[longitude1, latitude1],
                                                 [longitude2, latitude2]]
                                        },
                                        'relation': 'intersects'
                                    }
                                }
                            }
                        }
                    }
                }
            })

        # tabular variable
        # TODO: handle 'relationship'
        #  for now, it assumes the relationship is 'contains'
        elif 'tabular_variable' in variable['type']:
            if 'columns' in variable:
                for column_index in variable['columns']:
                    tabular_variables.append(column_index)

        if variable_query:
            output.append({
                'bool': {
                    'must': variable_query,
                }
            })

    if output:
        return {
            'bool': {
#                    'should': output,
#                    'minimum_should_match': 1,
                'must': output
            }
        }
    return {}


def get_augmentation_search_results(es, data_profile, query_args,
                                    tabular_variables, score_threshold,
                                    dataset_id=None, join=True, union=True):
    join_results = []
    union_results = []

    if join:
        logger.info("Looking for joins...")
        start = time.perf_counter()
        join_results = get_joinable_datasets(
            es=es,
            data_profile=data_profile,
            dataset_id=dataset_id,
            query_args=query_args,
            tabular_variables=tabular_variables
        )
        logger.info("Found %d join results in %.2fs",
                    len(join_results), time.perf_counter() - start)
    if union:
        logger.info("Looking for unions...")
        start = time.perf_counter()
        union_results = get_unionable_datasets(
            es=es,
            data_profile=data_profile,
            dataset_id=dataset_id,
            query_args=query_args,
            tabular_variables=tabular_variables
        )
        logger.info("Found %d union results in %.2fs",
                    len(union_results), time.perf_counter() - start)

    results = []
    for r in join_results:
        if r['score'] < score_threshold:
            continue
        results.append(dict(
            id=r['id'],
            score=r['score'],
            metadata=r['metadata'],
            augmentation=r['augmentation'],
        ))
    for r in union_results:
        if r['score'] < score_threshold:
            continue
        results.append(dict(
            id=r['id'],
            score=r['score'],
            metadata=r['metadata'],
            augmentation=r['augmentation'],
        ))

    return sorted(
        results,
        key=lambda item: item['score'],
        reverse=True
    )


def get_profile_data(filepath, metadata=None):
    # hashing data
    sha1 = hashlib.sha1()
    with open(filepath, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha1.update(data)
    hash_ = sha1.hexdigest()

    # checking for cached data
    cached_data = os.path.join('/cache', hash_)
    if os.path.exists(cached_data):
        logger.info("Found cached profile_data")
        return pickle.load(open(cached_data, 'rb'))

    # profile data and save
    logger.info("Profiling...")
    start = time.perf_counter()
    data_profile = process_dataset(filepath, metadata)
    logger.info("Profiled in %.2fs", time.perf_counter() - start)
    pickle.dump(data_profile, open(cached_data, 'wb'))
    return data_profile


def handle_data_parameter(data):
    """
    Handles the 'data' parameter.

    :param data: the input parameter
    :return: (data_path, data_profile, tmp)
      data_path: path to the input data
      data_profile: the profiling (metadata) of the data
      tmp: True if data_path points to a temporary file
    """

    if not isinstance(data, (str, bytes)):
        raise ClientError("The parameter 'data' is in the wrong format")

    tmp = False
    if not os.path.exists(data):
        # data represents the entire file
        logger.info("Data is not a path")

        tmp = True
        temp_file = tempfile.NamedTemporaryFile(mode='wb', delete=False)
        temp_file.write(data)
        temp_file.close()

        data_path = temp_file.name
        data_profile = get_profile_data(data_path)

    else:
        # data represents a file path
        logger.info("Data is a path")
        if os.path.isdir(data):
            # path to a D3M dataset
            data_file = os.path.join(data, 'tables', 'learningData.csv')
            if not os.path.exists(data_file):
                raise ClientError("%s does not exist" % data_file)
            else:
                data_path = data_file
                data_profile = get_profile_data(data_file)
        else:
            # path to a CSV file
            data_path = data
            data_profile = get_profile_data(data)

    return data_path, data_profile, tmp
