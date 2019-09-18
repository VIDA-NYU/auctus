import aio_pika
import asyncio
import elasticsearch
import hashlib
import json
import logging
import re
import sys
import threading


logger = logging.getLogger(__name__)


class Type:
    MISSING_DATA = 'https://metadata.datadrivendiscovery.org/types/' + \
                   'MissingData'
    INTEGER = 'http://schema.org/Integer'
    FLOAT = 'http://schema.org/Float'
    TEXT = 'http://schema.org/Text'
    BOOLEAN = 'http://schema.org/Boolean'
    LATITUDE = 'http://schema.org/latitude'
    LONGITUDE = 'http://schema.org/longitude'
    DATE_TIME = 'http://schema.org/DateTime'
    PHONE_NUMBER = 'https://metadata.datadrivendiscovery.org/types/' + \
                   'PhoneNumber'
    ID = 'http://schema.org/identifier'
    CATEGORICAL = 'http://schema.org/Enumeration'


def block_wait_future(future):
    """Block the current thread until the future is done, return result.

    This is like ``await`` but for threads. Do not call this on the event-loop
    thread.
    """
    event = threading.Event()
    future.add_done_callback(lambda *a, **kw: event.set())
    event.wait()
    return future.result()


def block_run(loop, coro):
    """Block the current thread until the coroutine is done, return result.

    The coroutine should not have been submitted to asyncio yet. Do not call
    this on the event-loop thread.
    """
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return block_wait_future(future)


def json2msg(obj, **kwargs):
    return aio_pika.Message(json.dumps(obj).encode('utf-8'), **kwargs)


def msg2json(msg):
    return json.loads(msg.body.decode('utf-8'))


def log_future(future, logger, message="Exception in background task",
               should_never_exit=False):
    def log(future):
        try:
            future.result()
        except Exception:
            logger.exception(message)
        if should_never_exit:
            logger.critical("Critical task died, exiting")
            asyncio.get_event_loop().stop()
            sys.exit(1)
    future.add_done_callback(log)


re_non_path_safe = re.compile(r'[^A-Za-z0-9_.-]')


def encode_dataset_id(dataset_id):
    """Encode a dataset ID to a format suitable for file names.
    """
    dataset_id = dataset_id.replace('_', '__')
    dataset_id = re_non_path_safe.sub(lambda m: '_%X' % ord(m.group(0)),
                                      dataset_id)
    return dataset_id


def decode_dataset_id(dataset_id):
    """Decode a dataset ID encoded using `encode_dataset_id()`.
    """
    dataset_id = list(dataset_id)
    i = 0
    while i < len(dataset_id):
        if dataset_id[i] == '_':
            if dataset_id[i + 1] == '_':
                del dataset_id[i + 1]
            else:
                char_hex = dataset_id[i + 1:i + 3]
                dataset_id[i + 1:i + 3] = []
                char_hex = ''.join(char_hex)
                dataset_id[i] = chr(int(char_hex, 16))
        i += 1
    return ''.join(dataset_id)


def hash_json(*args, **kwargs):
    if not args:
        dct = dict()
    elif len(args) == 1:
        dct = args[0]
        assert isinstance(dct, dict)
    else:
        raise TypeError("Expected 1 positional argument, got %d" % len(args))

    dct.update(**kwargs)

    bytes_ = json.dumps(dct, sort_keys=True).encode('utf-8')
    return hashlib.sha1(bytes_).hexdigest()


def add_dataset_to_sup_index(es, dataset_id, metadata):
    """
    Adds dataset to the supplementary Datamart indices:
    'datamart_columns' and 'datamart_spatial_coverage'.
    """

    common_dataset_metadata = dict(dataset_id=dataset_id)
    if 'name' in metadata:
        common_dataset_metadata['dataset_name'] = \
            metadata['name']
    if metadata.get('description'):
        common_dataset_metadata['dataset_description'] = \
            metadata['description']

    column_name_to_index = dict()

    # 'datamart_columns' index
    for column_index, column in enumerate(metadata['columns']):
        column_metadata = dict()
        column_metadata.update(common_dataset_metadata)
        column_metadata.update(column)
        column_metadata['index'] = column_index
        column_name_to_index[column_metadata['name']] = column_index
        if 'coverage' in column_metadata:
            for num_range in column_metadata['coverage']:
                num_range['gte'] = num_range['range']['gte']
                num_range['lte'] = num_range['range']['lte']
        es.index(
            'datamart_columns',
            column_metadata
        )

    # 'datamart_spatial_coverage' index
    if 'spatial_coverage' in metadata:
        for spatial_coverage in metadata['spatial_coverage']:
            spatial_coverage_metadata = dict()
            spatial_coverage_metadata.update(common_dataset_metadata)
            spatial_coverage_metadata.update(spatial_coverage)
            spatial_coverage_metadata['name'] = ' , '.join([
                spatial_coverage_metadata['lat'],
                spatial_coverage_metadata['lon']
            ])
            spatial_coverage_metadata['lat_index'] = \
                column_name_to_index[spatial_coverage_metadata['lat']]
            spatial_coverage_metadata['lon_index'] = \
                column_name_to_index[spatial_coverage_metadata['lon']]
            for spatial_range in spatial_coverage_metadata['ranges']:
                coordinates = spatial_range['range']['coordinates']
                spatial_range['min_lon'] = coordinates[0][0]
                spatial_range['max_lat'] = coordinates[0][1]
                spatial_range['max_lon'] = coordinates[1][0]
                spatial_range['min_lat'] = coordinates[1][1]
            es.index(
                'datamart_spatial_coverage',
                spatial_coverage_metadata,
            )


def add_dataset_to_index(es, dataset_id, metadata):
    """
    Safely adds a dataset to all the Datamart indices.
    """

    # 'datamart' index
    es.index(
        'datamart',
        metadata,
        id=dataset_id,
    )

    add_dataset_to_sup_index(
        es,
        dataset_id,
        metadata
    )


def add_dataset_to_lazo_storage(es, id, metadata):
    """Adds a dataset to Lazo.
    """

    es.index(
        'lazo',
        metadata,
        id=id,
    )


def delete_dataset_from_index(es, dataset_id, lazo_client=None):
    """
    Safely deletes a dataset from the 'datamart' index,
    including its corresponding information in
    'datamart_columns' and 'datamart_spatial_coverage' indices.
    This function also connects to the Lazo index service
    and deletes any corresponding sketch.
    """

    if lazo_client:
        # checking if there are any textual columns in the dataset
        # remove them from the Lazo index service
        body = {
            'query': {
                'bool': {
                    'must': [
                        {'term': {'dataset_id': dataset_id}},
                        {'term': {'structural_type': Type.TEXT}}
                    ],
                    'must_not': {
                        'term': {'semantic_types': Type.DATE_TIME}
                    }
                }
            }
        }
        textual_columns = list()
        while True:
            hits = es.search(
                index='datamart_columns',
                body=body,
                size=10000,
            )['hits']['hits']
            for h in hits:
                textual_columns.append(h['_source']['name'])
            if len(hits) != 10000:
                break
        if textual_columns:
            ack = lazo_client.remove_sketches(dataset_id, textual_columns)
            if ack:
                logger.info(
                    "Deleted %d documents from Lazo",
                    len(textual_columns)
                )
            else:
                logger.info("Error while deleting documents from Lazo")

    # deleting from 'datamart'
    try:
        es.delete('datamart', dataset_id)
    except elasticsearch.NotFoundError:
        return

    # deleting from 'datamart_columns' and 'datamart_spatial_coverage'
    body = {
        'query': {
            'term': {'dataset_id': dataset_id}
        }
    }
    for index in ('datamart_columns', 'datamart_spatial_coverage'):
        nb = es.delete_by_query(
            index=index,
            body=body,
        )['deleted']
        logger.info("Deleted %d documents from %s", nb, index)
