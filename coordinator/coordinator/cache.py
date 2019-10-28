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
PROM_CACHE_PROFILES = prometheus_client.Gauge(
    'cache_profiles_count',
    "Number of data profiles in cache",
)


CACHE_MAX = os.environ.get('MAX_CACHE_BYTES', 100_000_000_000)  # 100 GB


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


def clear_caches(aug):
    logger.warning("Cache size over limit, clearing")
    clear_cache('/cache/datasets')
    if aug:
        clear_cache('/cache/aug')


def check_cache():
    try:
        # Count datasets in cache
        datasets = 0
        datasets_bytes = 0
        for name in os.listdir('/cache/datasets'):
            path = os.path.join('/cache/datasets', name)
            if not name.endswith('.cache'):
                continue
            datasets += 1
            datasets_bytes += get_tree_size(path)
        PROM_CACHE_DATASETS.set(datasets)
        PROM_CACHE_DATASETS_BYTES.set(datasets_bytes)
        logger.info("%d datasets in cache, %d bytes",
                    datasets, datasets_bytes)

        # Count augmentations in cache
        augmentations = 0
        augmentations_bytes = 0
        for name in os.listdir('/cache/aug'):
            path = os.path.join('/cache/aug', name)
            if not name.endswith('.cache'):
                continue
            augmentations += 1
            augmentations_bytes += get_tree_size(path)
        PROM_CACHE_AUGMENTATIONS.set(augmentations)
        PROM_CACHE_AUGMENTATIONS_BYTES.set(augmentations_bytes)
        logger.info("%d augmentations in cache, %d bytes",
                    augmentations, augmentations_bytes)

        # Count profiles in cache
        PROM_CACHE_PROFILES.set(len(os.listdir('/cache/queries')))

        # Remove from caches if max is reached
        if datasets_bytes + augmentations_bytes > CACHE_MAX:
            fut = asyncio.get_event_loop().run_in_executor(
                None,
                clear_caches,
                augmentations_bytes > CACHE_MAX,
            )
            log_future(fut, logger)
    finally:
        asyncio.get_event_loop().call_later(
            5 * 60,
            check_cache,
        )
