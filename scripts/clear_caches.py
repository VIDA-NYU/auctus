#!/usr/bin/env python3

"""This script clears the cache folders safely.

This should not result in any data being lost or affect any running process.
"""

import logging
import os
import sys

from datamart_core.fscache import clear_cache


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    if (
            not os.path.isdir('/cache/datasets') or
            not os.path.isdir('/cache/queries')):
        print(
            "Cache directories don't exist; are you not running this script "
            "inside Docker?",
            file=sys.stderr,
        )
        sys.exit(1)
    clear_cache('/cache/datasets')
    clear_cache('/cache/queries')
