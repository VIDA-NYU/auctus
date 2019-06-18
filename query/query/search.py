import hashlib
import logging
import os
import pickle
import tempfile
import time

from datamart_augmentation.search import \
    get_joinable_datasets, get_unionable_datasets
from datamart_profiler import process_dataset


logger = logging.getLogger(__name__)


BUF_SIZE = 128000


class ClientError(ValueError):
    """Error in query sent by client.
    """


def get_augmentation_search_results(es, lazo_client, data_profile, query_args,
                                    tabular_variables, score_threshold,
                                    dataset_id=None, join=True, union=True):
    join_results = []
    union_results = []

    if join:
        logger.info("Looking for joins...")
        start = time.perf_counter()
        join_results = get_joinable_datasets(
            es=es,
            lazo_client=lazo_client,
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


def get_profile_data(filepath, metadata=None, lazo_client=None):
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
    data_profile = process_dataset(
        data=filepath,
        metadata=metadata,
        lazo_client=lazo_client,
        search=True
    )
    logger.info("Profiled in %.2fs", time.perf_counter() - start)
    pickle.dump(data_profile, open(cached_data, 'wb'))
    return data_profile


def handle_data_parameter(data, lazo_client=None):
    """
    Handles the 'data' parameter.

    :param data: the input parameter
    :param lazo_client: client for the Lazo Index Server
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
        temp_file = tempfile.NamedTemporaryFile(
            mode='wb',
            delete=False,
            dir='/lazo-data'
        )
        temp_file.write(data)
        temp_file.close()

        data_path = temp_file.name
        data_profile = get_profile_data(
            filepath=data_path,
            lazo_client=lazo_client
        )

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
                data_profile = get_profile_data(
                    filepath=data_file,
                    lazo_client=lazo_client
                )
        else:
            # path to a CSV file
            data_path = data
            data_profile = get_profile_data(
                filepath=data,
                lazo_client=lazo_client
            )

    return data_path, data_profile, tmp
