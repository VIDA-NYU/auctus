import aio_pika
import asyncio
from dateutil.parser import parse
import json
import numpy as np
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


def conv_float(x):
    try:
        return float(x)
    except Exception:
        return np.nan


def conv_int(x):
    try:
        return int(x)
    except Exception:
        return np.nan


def conv_datetime(x):
    try:
        return parse(x)
    except Exception:
        return np.nan


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
