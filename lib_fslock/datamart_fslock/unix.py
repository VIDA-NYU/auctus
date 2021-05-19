"""File locking implementation using flock(2).
"""

# This whole module exists as stand-alone, without the Prometheus metrics
# https://pypi.org/project/fslock/

import contextlib
import fcntl
import logging
import multiprocessing
import os
import prometheus_client
import signal
import time


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
                raise ValueError("This host did not respond in time")
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
            try:
                fcntl.flock(fd, op | fcntl.LOCK_NB)
            except BlockingIOError:
                pipe.send('TIMEOUT')
                return
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


# Using the 'fork' method causes deadlocks because other threads acquire locks
_mp_context = multiprocessing.get_context('spawn')


def join_process(proc, timeout):
    # multiprocessing.Process.join() waits "at most", this waits "at least"
    start = now = time.perf_counter()
    while now - start < timeout:
        proc.join(timeout - now + start)
        if proc.exitcode is not None:
            break
        now = time.perf_counter()


@contextlib.contextmanager
def _lock(filepath, exclusive, timeout=None):
    type_ = "exclusive" if exclusive else "shared"

    started = False
    locked = False
    pipe, pipe2 = _mp_context.Pipe()
    proc = _mp_context.Process(
        target=_lock_process,
        args=(pipe2, filepath, exclusive, timeout),
    )
    try:
        with PROM_LOCK_ACQUIRE.labels(type_).time():
            proc.start()
            started = True

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
        if not started:
            return
        logger.debug("Releasing %s lock: %r", type_, filepath)
        start = time.perf_counter()
        pipe.send('UNLOCK')
        join_process(proc, 60)
        if proc.exitcode != 0:
            logger.critical("Failed (%r) to release %s lock: %r",
                            proc.exitcode, type_, filepath)
            raise SystemExit("Failed (%r) to release %s lock: %r" % (
                proc.exitcode, type_, filepath,
            ))
        now = time.perf_counter()
        if now - start > 10:
            logger.critical("Releasing %s lock took %.2fs: %r",
                            type_, now - start, filepath)
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
