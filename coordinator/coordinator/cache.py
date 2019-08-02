import asyncio
import logging
import os
import prometheus_client


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


def check_cache():
    try:
        # Count datasets in cache
        datasets = 0
        datasets_bytes = 0
        augmentations = 0
        augmentations_bytes = 0
        for name in os.listdir('/dataset_cache'):
            path = os.path.join('/dataset_cache', name)
            if name.startswith('aug_'):
                augmentations += 1
                augmentations_bytes += get_tree_size(path)
            else:
                datasets += 1
                datasets_bytes += get_tree_size(path)
        PROM_CACHE_DATASETS.set(datasets)
        PROM_CACHE_DATASETS_BYTES.set(datasets_bytes)
        logger.info("%d datasets in cache, %d bytes",
                    datasets, datasets_bytes)
        PROM_CACHE_AUGMENTATIONS.set(augmentations)
        PROM_CACHE_AUGMENTATIONS_BYTES.set(augmentations_bytes)
        logger.info("%d augmentations in cache, %d bytes",
                    augmentations, augmentations_bytes)

        # TODO: Remove some datasets from the cache

        # Count profiles in cache
        PROM_CACHE_PROFILES.set(len(os.listdir('/cache')))
    finally:
        asyncio.get_event_loop().call_later(
            5 * 60,
            check_cache,
        )
