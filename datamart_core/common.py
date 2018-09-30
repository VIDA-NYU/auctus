import aiohttp
import asyncio
import elasticsearch
import logging
import os
import threading
import urllib.parse


logger = logging.getLogger(__name__)


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


class Async(object):
    pass


class BaseHandler(object):
    BASE_PLUGIN_CLASSES = ()
    POLL_PATH = None

    def __init__(self, obj, identifier, concurrent):
        if not isinstance(obj, self.BASE_PLUGIN_CLASSES):
            raise TypeError("Plugin object doesn't derive a base class")
        elif isinstance(obj, Async):
            self._async = True
        else:
            self._async = False
        self._obj = obj
        self._identifier = identifier
        self.elasticsearch = elasticsearch.Elasticsearch(
            os.environ['ELASTICSEARCH_HOSTS'].split(',')
        )
        self.coordinator = os.environ['COORDINATOR_URL'].rstrip('/')
        self._work = asyncio.Semaphore(concurrent)
        self.http_session = aiohttp.ClientSession()
        self.loop = asyncio.get_event_loop()
        self.loop.create_task(self._request_work())

    def get(self, path):
        url = (self.coordinator + path +
               '?id=' + urllib.parse.quote(self._identifier))
        return self.http_session.get(url)

    def post(self, path, json):
        url = (self.coordinator + path +
               '?id=' + urllib.parse.quote(self._identifier))
        return self.http_session.post(url, json=json)

    def _call(self, method, *args):
        if self._async:
            return asyncio.get_event_loop().create_task(
                method(*args),
            )
        else:
            return asyncio.get_event_loop().run_in_executor(
                None,
                method,
                *args,
            )

    async def _request_work(self):
        while True:
            await self._work.acquire()
            try:
                async with self.get(self.POLL_PATH) as resp:
                    obj = await resp.json()
            except:
                logger.exception("Got error polling coordinator")
            else:
                future = self.work_received(obj)
                if future is None:
                    logger.error("Got unknown request from coordinator")
                else:
                    future.add_done_callback(self._work_done)
                    continue

            self._work.release()
            await asyncio.sleep(5)

    def work_received(self, obj):
        raise NotImplementedError

    def _work_done(self, future):
        self._work.release()


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
