#!/usr/bin/env python3

"""This script clears the cache folders safely.

This should not result in any data being lost or affect any running process.
"""

import logging

from datamart_core.objectstore import get_object_store


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    store = get_object_store()
    store.clear_bucket('cached-datasets')
    store.clear_bucket('cached-augmentations')
