import asyncio
import logging
import prometheus_client

from datamart_core.objectstore import get_object_store


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


def check_cache():
    try:
        object_store = get_object_store()

        # Count datasets in cache
        datasets = 0
        datasets_bytes = 0
        for obj in object_store.list_bucket_details('cached-datasets'):
            datasets += 1
            datasets_bytes += obj['size']
        PROM_CACHE_DATASETS.set(datasets)
        PROM_CACHE_DATASETS_BYTES.set(datasets_bytes)
        logger.info("%d datasets in cache, %d bytes",
                    datasets, datasets_bytes)

        # Count augmentations in cache
        augmentations = 0
        augmentations_bytes = 0
        for obj in object_store.list_bucket_details('cached-augmentations'):
            augmentations += 1
            augmentations_bytes += obj['size']
        PROM_CACHE_AUGMENTATIONS.set(augmentations)
        PROM_CACHE_AUGMENTATIONS_BYTES.set(augmentations_bytes)
        logger.info("%d augmentations in cache, %d bytes",
                    augmentations, augmentations_bytes)

    finally:
        asyncio.get_event_loop().call_later(
            5 * 60,
            check_cache,
        )
