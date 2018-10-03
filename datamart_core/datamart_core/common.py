import asyncio
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


class Storage(object):
    def __init__(self, obj):
        self.path = obj['path']

    def __repr__(self):
        return '<Storage %r>' % self.path


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
