import asyncio
import logging
import os
import prometheus_client

from datamart_core.common import log_future
from datamart_core.fscache import clear_cache


logger = logging.getLogger(__name__)


PROM_CACHE_DATASETS = prometheus_client.Gauge(
    'cache_datasets_count',
    "Number of datasets in cache",
)
PROM_CACHE_DATASETS_BYTES = prometheus_client.Gauge(
    'cache_datasets_bytes',
    "Total size of datasets in cache",
)
PROM_CACHE_AUGMENTATIONS = prometheus_client.Gauge(
    'cache_augmentations_count',
    "Number of augmentation results in cache",
)
PROM_CACHE_AUGMENTATIONS_BYTES = prometheus_client.Gauge(
    'cache_augmentations_bytes',
    "Total size of augmentation results in cache",
)


CACHE_HIGH = os.environ.get('MAX_CACHE_BYTES')
CACHE_HIGH = int(CACHE_HIGH, 10) if CACHE_HIGH else 100000000000  # 100 GB
CACHE_LOW = CACHE_HIGH * 0.33

CACHES = ('/cache/datasets', '/cache/aug')


def get_tree_size(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    size = 0
    for dirpath, _, filenames in os.walk(path):
        for filename in filenames:
            try:
                size += os.path.getsize(os.path.join(dirpath, filename))
            except OSError:
                pass
    return size


def clear_caches():
    logger.warning("Cache size over limit, clearing")

    # Build list of all entries
    entries = []
    for cache in CACHES:
        for name in os.listdir(cache):
            if not name.endswith('.cache'):
                continue
            key = name[:-6]
            path = os.path.join(cache, name)
            stat = os.stat(path)
            entries.append((cache, key, get_tree_size(path), stat.st_mtime))

    # Sort it by date
    entries = sorted(entries, key=lambda e: -e[3])

    # Select entries to keep while staying under threshold
    keep = {cache: set() for cache in CACHES}
    total_size = 0
    for cache, key, size, mtime in entries:
        if total_size + size <= CACHE_LOW:
            keep[cache].add(key)
            total_size += size

    for cache in CACHES:
        clear_cache(
            cache,
            should_delete=lambda key, keep_set=keep[cache]: key not in keep_set,
        )


def measure_cache_dir(dirname):
    entries = 0
    size_bytes = 0
    for name in os.listdir(dirname):
        path = os.path.join(dirname, name)
        if not name.endswith('.cache'):
            continue
        entries += 1
        size_bytes += get_tree_size(path)
    return entries, size_bytes


def check_cache():
    try:
        # Count datasets in cache
        datasets, datasets_bytes = measure_cache_dir('/cache/datasets')
        PROM_CACHE_DATASETS.set(datasets)
        PROM_CACHE_DATASETS_BYTES.set(datasets_bytes)
        logger.info("%d datasets in cache, %d bytes",
                    datasets, datasets_bytes)

        # Count augmentations in cache
        augmentations, augmentations_bytes = measure_cache_dir('/cache/aug')
        PROM_CACHE_AUGMENTATIONS.set(augmentations)
        PROM_CACHE_AUGMENTATIONS_BYTES.set(augmentations_bytes)
        logger.info("%d augmentations in cache, %d bytes",
                    augmentations, augmentations_bytes)

        # Remove from caches if max is reached
        if datasets_bytes + augmentations_bytes > CACHE_HIGH:
            fut = asyncio.get_event_loop().run_in_executor(
                None,
                clear_caches,
            )
            log_future(fut, logger)
    finally:
        asyncio.get_event_loop().call_later(
            5 * 60,
            check_cache,
        )
