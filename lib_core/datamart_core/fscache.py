import contextlib
import logging
import os
import shutil
import signal
import subprocess


logger = logging.getLogger(__name__)


safe_shell_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                       "abcdefghijklmnopqrstuvwxyz"
                       "0123456789"
                       "-+=/:.,%_")


def shell_escape(s):
    r"""Given bl"a, returns "bl\\"a".
    """
    if isinstance(s, bytes):
        s = s.decode('utf-8')
    if not s or any(c not in safe_shell_chars for c in s):
        return '"%s"' % (s.replace('\\', '\\\\')
                          .replace('"', '\\"')
                          .replace('`', '\\`')
                          .replace('$', '\\$'))
    else:
        return s


WAIT_FOREVER_COMMAND = 'while true; do sleep 3600; done'


def lock_exclusive(filepath, timeout=None):
    """Get an exclusive lock.

    The file is created if it doesn't exist.
    """
    args = ['-x']
    if timeout is not None:
        args.extend(['--wait', timeout, '-E', 2])
    args.extend([
        filepath, '-c',
        'echo LOCKED; %s' % WAIT_FOREVER_COMMAND,
    ])
    proc = subprocess.Popen(
        ['flock'] + args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE,
        # Causes subprocesses (e.g. sleep) to get killed when the shell
        # process group is killed
        start_new_session=True,
    )
    proc.stdin.close()

    # Read out of stdout
    out = proc.stdout.read(6)
    if out == b'LOCKED':
        logger.info("Acquired exclusive lock: %r", filepath)
        return proc, filepath
    else:
        out = proc.stdout.read()
        if proc.wait() == 2:
            logger.debug("Timeout getting exclusive lock: %r", filepath)
            return None
        else:
            raise OSError("Error getting exclusive lock %r: %s" % (
                filepath,
                out.decode('utf-8', 'replace'),
            ))


def unlock_exclusive(lock):
    proc, filepath = lock
    logger.debug("Releasing exclusive lock: %r", filepath)
    os.killpg(proc.pid, signal.SIGINT)
    try:
        proc.wait(10)
    except subprocess.TimeoutExpired:
        logger.critical("Failed to release exclusive lock: %r", filepath)
        raise SystemExit("Failed to release exclusive lock: %r" % filepath)
    logger.info("Released exclusive lock: %r", filepath)


def lock_shared(filepath, timeout=None):
    """Get a shared lock.

    :raises FileNotFoundError: if the file doesn't exist.
    """
    cmd = '(flock -s %s 3 && echo LOCKED && %s) 3<%s' % (
        '--wait %d -E 2' % timeout if timeout is not None else '',
        WAIT_FOREVER_COMMAND,
        shell_escape(filepath),
    )
    proc = subprocess.Popen(
        ['bash', '-c', cmd],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE,
        # Causes subprocesses (e.g. sleep) to get killed when the shell
        # process group is killed
        start_new_session=True,
    )
    proc.stdin.close()

    # Read out of stdout
    out = proc.stdout.read(6)
    if out == b'LOCKED':
        logger.info("Acquired shared lock: %r", filepath)
        return proc, filepath
    else:
        out = proc.stdout.read()
        if proc.wait() == 2:
            logger.debug("Timeout getting shared lock: %r", filepath)
            return None
        elif proc.wait() == 1:
            raise FileNotFoundError
        else:
            raise OSError("Error getting shared lock %r: %s" % (
                filepath,
                out.decode('utf-8', 'replace'),
            ))


def unlock_shared(lock):
    proc, filepath = lock
    logger.debug("Releasing shared lock: %r", filepath)
    os.killpg(proc.pid, signal.SIGINT)
    try:
        proc.wait(10)
    except subprocess.TimeoutExpired:
        logger.critical("Failed to release shared lock: %r", filepath)
        raise SystemExit("Failed to release shared lock: %r" % filepath)
    logger.info("Released shared lock: %r", filepath)


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
            lock = lock_shared(lock_path)
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
                unlock_shared(lock)

        lock = lock_exclusive(lock_path)
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
            unlock_exclusive(lock)


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
        lock = lock_exclusive(lock_path, timeout=300)
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
            unlock_exclusive(lock)
