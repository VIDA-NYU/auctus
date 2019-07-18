import contextlib
import fcntl
import logging
import multiprocessing
import os
import prometheus_client
import shutil
import signal


logger = logging.getLogger(__name__)


PROM_LOCK_ACQUIRE = prometheus_client.Histogram(
    'cache_lock_acquire_seconds',
    "Time to acquire lock on cache",
    ['type'],
    buckets=[1.0, 10.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0,
             float('inf')],
)
PROM_LOCKS_HELD = prometheus_client.Gauge(
    'cache_locks_held',
    "Number of locks on cache currently held",
    ['type'],
)


@contextlib.contextmanager
def timeout_syscall(seconds):
    """Interrupt a system-call after a time (main thread only).

    Warning: this only works from the main thread! Trying to use this on
    another thread will cause the call to not timeout, and the main thread will
    receive an InterruptedError instead!

    Example::

        with timeout_syscall(5):
            try:
                socket.connect(...)
            except InterruptedError:
                raise ValueError("This host does not respond in time")
    """
    def timeout_handler(signum, frame):
        raise InterruptedError

    original_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original_handler)


def _lock_process(pipe, filepath, exclusive, timeout=None):
    """Locking function, runs in a subprocess.

    We run the locking in a subprocess so that we are the main thread
    (required to use SIGALRM) and to avoid spurious unlocking on Linux (which
    can happen if a different file descriptor for the same file gets closed,
    even by another thread).
    """
    try:
        # Reset signal handlers
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGHUP, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

        # Open the file
        mode = os.O_RDONLY | os.O_CREAT if exclusive else os.O_RDONLY
        try:
            fd = os.open(filepath, mode)
        except FileNotFoundError:
            pipe.send('NOTFOUND')
            return

        # Lock it
        op = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        if timeout is None:
            fcntl.flock(fd, op)
        elif timeout == 0:
            fcntl.flock(fd, op | fcntl.LOCK_NB)
        else:
            with timeout_syscall(timeout):
                try:
                    fcntl.flock(fd, op)
                except InterruptedError:
                    pipe.send('TIMEOUT')
                    return
        pipe.send('LOCKED')
    except Exception:
        pipe.send('ERROR')
        raise

    # Wait for unlock message then exit
    assert pipe.recv() == 'UNLOCK'

    # Exiting releases the lock


@contextlib.contextmanager
def _lock(filepath, exclusive, timeout=None):
    type_ = "exclusive" if exclusive else "shared"

    locked = False
    pipe, pipe2 = multiprocessing.Pipe()
    proc = multiprocessing.Process(
        target=_lock_process,
        args=(pipe2, filepath, exclusive, timeout),
    )
    try:
        with PROM_LOCK_ACQUIRE.labels(type_).time():
            proc.start()

            out = pipe.recv()
            if out == 'LOCKED':
                logger.info("Acquired %s lock: %r", type_, filepath)
                locked = True
                PROM_LOCKS_HELD.labels(type_).inc()
            elif out == 'TIMEOUT':
                logger.debug("Timeout getting %s lock: %r", type_, filepath)
                raise TimeoutError
            elif out == 'NOTFOUND':
                raise FileNotFoundError
            else:
                logger.error("Error getting %s lock: %r", type_, filepath)
                raise OSError("Error getting %s lock: %r", type_, filepath)

        yield
    finally:
        logger.debug("Releasing %s lock: %r", type_, filepath)
        pipe.send('UNLOCK')
        proc.join(10)
        if proc.exitcode != 0:
            logger.critical("Failed (%r) to release %s lock: %r",
                            proc.exitcode, type_, filepath)
            raise SystemExit("Failed (%r) to release %s lock: %r" % (
                proc.exitcode, type_, filepath,
            ))
        logger.info("Released %s lock: %r", type_, filepath)
        if locked:
            PROM_LOCKS_HELD.labels(type_).dec()


def FSLockExclusive(filepath, timeout=None):
    """Get an exclusive lock.

    The file is created if it doesn't exist.
    """
    return _lock(filepath, True, timeout=timeout)


def FSLockShared(filepath, timeout=None):
    """Get a shared lock.

    :raises FileNotFoundError: if the file doesn't exist.
    """
    return _lock(filepath, False, timeout=timeout)


@contextlib.contextmanager
def cache_get_or_set(path, create_function):
    """This function is a file cache safe for multiple processes (locking).

    It is used like so::

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
        with contextlib.ExitStack() as lock:
            try:
                lock.enter_context(FSLockShared(lock_path))
            except FileNotFoundError:
                pass
            else:
                if os.path.exists(path):
                    # Entry exists and we have it locked, return it
                    yield
                    return
                # Entry was removed while we waited -- we'll try creating

        with FSLockExclusive(lock_path):
            if os.path.exists(path):
                # Cache was created while we waited
                # We can't downgrade to a shared lock, so restart
                continue
            else:
                try:
                    # Cache doesn't exist and we have it locked -- create
                    create_function()
                except:
                    # Creation failed, clean up before unlocking!
                    if os.path.isdir(path):
                        shutil.rmtree(path)
                    elif os.path.isfile(path):
                        os.remove(path)
                    os.remove(lock_path)
                    raise

                # We can't downgrade to a shared lock, so restart
                continue


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
        with contextlib.ExitStack() as lock:
            try:
                lock.enter_context(FSLockExclusive(lock_path, timeout=300))
            except TimeoutError:
                logger.warning("Entry is locked: %r", fname)
                continue
            if os.path.exists(path):
                logger.info("Deleting entry: %r", fname)
                if os.path.isfile(path):
                    os.remove(path)
                else:
                    shutil.rmtree(path)
                os.remove(lock_path)
            else:
                logger.error("Concurrent deletion?! Entry is gone: %r", fname)