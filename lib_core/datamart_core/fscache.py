import contextlib
import fcntl
import logging
import os


logger = logging.getLogger(__name__)


class FilesystemLocks(object):
    """File locking system.

    Warning: this is NOT thread-safe, do not use it when multiple threads might
    lock the same files!
    """
    def __init__(self):
        self._locks = {}

    def lock_exclusive(self, filepath):
        filepath = os.path.realpath(filepath)
        if filepath in self._locks:
            raise RuntimeError("Getting lock on already-locked file %r" %
                               filepath)
        fd = os.open(filepath, os.O_RDONLY | os.O_CREAT)
        self._locks[filepath] = 'ex'
        fcntl.flock(fd, fcntl.LOCK_EX)
        logger.debug("Acquired exclusive lock: %r", filepath)
        return fd, filepath

    def unlock_exclusive(self, lock):
        fd, filepath = lock
        assert self._locks.pop(filepath) == 'ex'
        fcntl.flock(fd, fcntl.LOCK_UN)
        logger.debug("Released exclusive lock: %r", filepath)

    def lock_shared(self, filepath):
        filepath = os.path.realpath(filepath)
        if filepath in self._locks:
            raise RuntimeError("Getting lock on already-locked file %r" %
                               filepath)
        fd = os.open(filepath, os.O_RDONLY)
        self._locks[filepath] = 'sh'
        fcntl.flock(fd, fcntl.LOCK_SH)
        logger.debug("Acquired shared lock: %r", filepath)
        return fd, filepath

    def unlock_shared(self, lock):
        fd, filepath = lock
        assert self._locks.pop(filepath) == 'sh'
        fcntl.flock(fd, fcntl.LOCK_UN)
        logger.debug("Released shared lock: %r", filepath)


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
