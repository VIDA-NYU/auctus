import contextlib
import fcntl
import logging
import os
import shutil
import signal


logger = logging.getLogger(__name__)


@contextlib.contextmanager
def timeout_syscall(seconds):
    original_handler = signal.signal(signal.SIGALRM, lambda *a: None)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original_handler)


class FilesystemLocks(object):
    """File locking system.

    Warning: this is NOT thread-safe, do not use it when multiple threads might
    lock the same files!
    """
    def __init__(self):
        self._locks = {}

    def lock_exclusive(self, filepath, timeout=None):
        filepath = os.path.realpath(filepath)
        if filepath in self._locks:
            raise RuntimeError("Getting lock on already-locked file %r" %
                               filepath)
        fd = os.open(filepath, os.O_RDONLY | os.O_CREAT)
        if self._lock_fd(fd, fcntl.LOCK_EX, timeout):
            self._locks[filepath] = 'ex'
            logger.debug("Acquired exclusive lock: %r", filepath)
            return fd, filepath
        else:
            logger.debug("Timeout getting exclusive lock: %r", filepath)
            return None

    def unlock_exclusive(self, lock):
        fd, filepath = lock
        assert self._locks.pop(filepath) == 'ex'
        self._unlock_fd(fd)
        logger.debug("Released exclusive lock: %r", filepath)

    def lock_shared(self, filepath, timeout=None):
        filepath = os.path.realpath(filepath)
        if filepath in self._locks:
            raise RuntimeError("Getting lock on already-locked file %r" %
                               filepath)
        fd = os.open(filepath, os.O_RDONLY)
        if self._lock_fd(fd, fcntl.LOCK_SH, timeout):
            self._locks[filepath] = 'sh'
            logger.debug("Acquired shared lock: %r", filepath)
            return fd, filepath
        else:
            logger.debug("Timeout getting shared lock: %r", filepath)
            return None

    def unlock_shared(self, lock):
        fd, filepath = lock
        assert self._locks.pop(filepath) == 'sh'
        self._unlock_fd(fd)
        logger.debug("Released shared lock: %r", filepath)

    def _lock_fd(self, fd, lock, timeout):
        if timeout is None:
            fcntl.flock(fd, lock)
        elif timeout == 0:
            fcntl.flock(fd, lock | fcntl.LOCK_NB)
        else:
            with timeout_syscall(timeout):
                try:
                    fcntl.flock(fd, lock)
                except InterruptedError:
                    return False
        return True

    def _unlock_fd(self, fd):
        fcntl.flock(fd, fcntl.LOCK_UN)


FilesystemLocks = FilesystemLocks()


@contextlib.contextmanager
def cache_get_or_set(path, create_function):
    """This function is a file cache safe for multiple processes (locking).

    It is used like so:

        # The path to be access or created
        cache_filename = '/tmp/cache/cachekey123'

        # This function is called to create the entry if it doesn't exist
        def create_entry():
            with open(cache_filename, 'w') as fp:
                fp.write('%d\n' % long_computation())

        with cache_get_or_set(cache_filename, create_entry):
            # In this with-block, the file or directory is locked with a shared
            # lock, so it won't be changed or removed
            with open(cache_filename) as fp:
                print(fp.read())
    """
    lock_path = path + '.lock'
    while True:
        try:
            lock = FilesystemLocks.lock_shared(lock_path)
        except FileNotFoundError:
            pass
        else:
            try:
                if os.path.exists(path):
                    # Entry exists and we have it locked, return it
                    yield
                    return
                # Entry was removed while we waited -- we'll try creating
            finally:
                FilesystemLocks.unlock_shared(lock)

        lock = FilesystemLocks.lock_exclusive(lock_path)
        try:
            if os.path.exists(path):
                # Cache was created while we waited
                # We can't downgrade to a shared lock, so restart
                continue
            else:
                # Cache doesn't exist and we have it locked -- create
                create_function()

                # We can't downgrade to a shared lock, so restart
                continue
        finally:
            FilesystemLocks.unlock_exclusive(lock)


def clear_cache(cache_root, should_delete=None):
    """Function used to safely clear a cache.

    Directory currently locked by other processes will be
    """
    if should_delete is None:
        should_delete = lambda *, fname: True

    files = set(os.listdir(cache_root))
    files = sorted(
        f for f in files
        if not (f.endswith('.lock') and f[:-5] in files)
    )
    logger.info("Cleaning cache, %d entries in %r", len(files), cache_root)

    for fname in files:
        path = os.path.join(cache_root, fname)
        if not should_delete(fname=path):
            logger.info("Skipping entry: %r", fname)
            continue
        lock_path = path + '.lock'
        logger.info("Locking entry: %r", fname)
        lock = FilesystemLocks.lock_exclusive(lock_path, timeout=300)
        if lock is None:
            logger.warning("Entry is locked: %r", fname)
            continue
        try:
            if os.path.exists(path):
                logger.info("Deleting entry: %r", fname)
                shutil.rmtree(path)
                os.remove(lock_path)
            else:
                logger.error("Concurrent deletion?! Entry is gone: %r", fname)
        finally:
            FilesystemLocks.unlock_exclusive(lock)
