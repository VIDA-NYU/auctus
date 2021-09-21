import contextlib
import logging
import os
import prometheus_client
import shutil

from . import FSLockExclusive, FSLockShared


logger = logging.getLogger(__name__)


PROM_CACHE_HITS = prometheus_client.Counter(
    'cache_hits',
    "Number of cache lookups that hit, per cache directory",
    ['cache_dir'],
)
PROM_CACHE_MISSES = prometheus_client.Counter(
    'cache_misses',
    "Number of cache lookups that miss, per cache directory",
    ['cache_dir'],
)

PROM_CACHE_HITS.labels('/cache/datasets').inc(0)
PROM_CACHE_MISSES.labels('/cache/datasets').inc(0)
PROM_CACHE_HITS.labels('/cache/aug').inc(0)
PROM_CACHE_MISSES.labels('/cache/aug').inc(0)
PROM_CACHE_HITS.labels('/cache/user_data').inc(0)
PROM_CACHE_MISSES.labels('/cache/user_data').inc(0)


@contextlib.contextmanager
def cache_get_or_set(cache_dir, key, create_function, cache_invalid=False):
    """This function is a file cache safe for multiple processes (locking).

    It is used like so::

        # This function is called to create the entry if it doesn't exist
        def create_it(tmp_path):
            # In this function, the path is locked with an exclusive lock
            # `tmp_path` will be renamed to the cache path on success
            with open(tmp_path, 'w') as fp:
                fp.write('%d\n' % long_computation())

        with cache_get_or_set('/tmp/cache', 'key123', create_it) as entry_path:
            # In this with-block, the path is locked with a shared lock, so it
            # won't be changed or removed
            with open(entry_path) as fp:
                print(fp.read())
    """
    entry_path = os.path.join(cache_dir, key + '.cache')
    lock_path = os.path.join(cache_dir, key + '.lock')
    temp_path = os.path.join(cache_dir, key + '.temp')
    metric_set = False
    while True:
        if not cache_invalid:
            with contextlib.ExitStack() as lock:
                try:
                    lock.enter_context(FSLockShared(lock_path))
                except FileNotFoundError:
                    pass
                else:
                    if os.path.exists(entry_path):
                        if not metric_set:
                            metric_set = True
                            PROM_CACHE_HITS.labels(cache_dir).inc(1)

                        # Update time on the file
                        with open(lock_path, 'a'):
                            pass

                        # Entry exists and we have it locked, return it
                        yield entry_path
                        return
                    # Entry was removed while we waited -- we'll try creating

        with FSLockExclusive(lock_path):
            if cache_invalid:
                # Remove the cache that's invalid
                if os.path.isdir(entry_path):
                    shutil.rmtree(entry_path)
                elif os.path.isfile(entry_path):
                    os.remove(entry_path)

                cache_invalid = False
            elif os.path.exists(entry_path):
                # Cache was created while we waited
                # We can't downgrade to a shared lock, so restart
                continue

            # Remove temporary file
            if os.path.isdir(temp_path):
                shutil.rmtree(temp_path)
            elif os.path.isfile(temp_path):
                os.remove(temp_path)

            try:
                if not metric_set:
                    metric_set = True
                    PROM_CACHE_MISSES.labels(cache_dir).inc(1)
                # Cache doesn't exist and we have it locked -- create
                create_function(temp_path)
            except BaseException:
                # Creation failed, clean up before unlocking!
                if os.path.isdir(temp_path):
                    shutil.rmtree(temp_path)
                elif os.path.isfile(temp_path):
                    os.remove(temp_path)
                os.remove(lock_path)
                raise
            else:
                # Rename it to destination
                os.rename(temp_path, entry_path)

            # We can't downgrade to a shared lock, so restart


@contextlib.contextmanager
def cache_get(cache_dir, key):
    """This is like `cache_get_or_set()` except it won't create the entry.

    It is used like so::

        with cache_get('/tmp/cache', 'key123') as entry_path:
            if entry_path is None:
                raise KeyError("We don't have this cached")
            else:
                # We have the path locked with a shared lock, so it won't be
                # changed or removed while we read and process it
                with open(entry_path) as fp:
                    print(fp.read())
    """
    entry_path = os.path.join(cache_dir, key + '.cache')
    lock_path = os.path.join(cache_dir, key + '.lock')

    with contextlib.ExitStack() as lock:
        try:
            lock.enter_context(FSLockShared(lock_path))
        except FileNotFoundError:
            yield None
            return
        else:
            if os.path.exists(entry_path):
                PROM_CACHE_HITS.labels(cache_dir).inc(1)

                # Update time on the file
                with open(lock_path, 'a'):
                    pass

                # Entry exists and we have it locked, return it
                yield entry_path
                return

            # Entry was removed while we waited
            PROM_CACHE_MISSES.labels(cache_dir).inc(1)
            yield None
            return


def clear_cache(cache_dir, should_delete=None, only_if_possible=True):
    """Function used to safely clear a cache.

    Directory currently locked by other processes will be retried with a 60s
    timeout if `only_if_possible=False` (default ``True``).
    """
    if should_delete is None:
        should_delete = lambda *, key: True

    files = set()
    for filename in os.listdir(cache_dir):
        if filename.endswith(('.lock', '.cache', '.temp')):
            filename = filename.rsplit('.', 1)[0]
            files.add(filename)
        else:
            logger.warning("Unexpected file in cache directory: %s", filename)
    files = sorted(files)
    logger.info("Cleaning cache, %d entries in %r", len(files), cache_dir)

    # Loop while there are entries to delete
    timeout = 0
    while files:
        not_deleted = []
        for key in files:
            if not should_delete(key=key):
                logger.info("Skipping entry: %r", key)
                continue
            try:
                delete_cache_entry(cache_dir, key, timeout=timeout)
            except TimeoutError:
                logger.warning("Entry is locked: %r", key)
                not_deleted.append(key)
                continue
        files = not_deleted
        logger.info("%d entries left", len(files))
        if only_if_possible:
            break  # Give up on deleting the ones that are left
        else:
            timeout = 60  # Retry with a non-zero timeout


def delete_cache_entry(cache_dir, key, timeout=None):
    entry_path = os.path.join(cache_dir, key + '.cache')
    lock_path = os.path.join(cache_dir, key + '.lock')
    temp_path = os.path.join(cache_dir, key + '.temp')
    logger.info("Locking entry: %r", key)
    with contextlib.ExitStack() as lock:
        lock.enter_context(  # Might raise TimeoutError
            FSLockExclusive(lock_path, timeout=timeout),
        )
        if os.path.exists(entry_path):
            logger.info("Deleting entry: %r", key)
            if os.path.isfile(entry_path):
                os.remove(entry_path)
            else:
                shutil.rmtree(entry_path)
            os.remove(lock_path)
        else:
            os.remove(lock_path)
        if os.path.exists(temp_path):
            logger.info("Deleting temporary file: %r", key + '.temp')
            if os.path.isfile(temp_path):
                os.remove(temp_path)
            else:
                shutil.rmtree(temp_path)
