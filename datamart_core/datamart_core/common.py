import aio_pika
import asyncio
import json
import sys
import threading


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


def json2msg(obj):
    return aio_pika.Message(json.dumps(obj).encode('utf-8'))


def msg2json(msg):
    return json.loads(msg.body.decode('utf-8'))


class Storage(object):
    def __init__(self, obj):
        self.path = obj['path']

    def __repr__(self):
        return '<Storage %r>' % self.path

    def to_json(self):
        return {'path': self.path}


class WriteStorage(Storage):
    def __init__(self, obj):
        super(WriteStorage, self).__init__(obj)
        self.max_size_bytes = obj.get('max_size_bytes')

    def __repr__(self):
        return '<WriteStorage %r%s>' % (
            self.path,
            ' max_size_bytes=%r' % self.max_size_bytes
            if self.max_size_bytes else ''
        )


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
