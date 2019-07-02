import aio_pika
import asyncio
import json
import re
import sys
import threading


class Type:
    MISSING_DATA = 'https://metadata.datadrivendiscovery.org/types/' +\
                   'MissingData'
    INTEGER = 'http://schema.org/Integer'
    FLOAT = 'http://schema.org/Float'
    TEXT = 'http://schema.org/Text'
    BOOLEAN = 'http://schema.org/Boolean'
    LATITUDE = 'http://schema.org/latitude'
    LONGITUDE = 'http://schema.org/longitude'
    DATE_TIME = 'http://schema.org/DateTime'
    PHONE_NUMBER = 'https://metadata.datadrivendiscovery.org/types/' +\
                   'PhoneNumber'
    ID = 'http://schema.org/identifier'
    CATEGORICAL = 'https://schema.org/Enumeration'


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
