#!/usr/bin/env python3

"""This script clears the cache folders safely.

This should not result in any data being lost or affect any running process.
"""

import logging
import os
import sys

from datamart_fslock.cache import clear_cache


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    if sys.argv[1:] == []:
        only_if_possible = False
    elif sys.argv[1:] == ['--if-possible']:
        only_if_possible = True
    else:
        print("Usage: clear_caches.py [--if-possible]", file=sys.stderr)
        sys.exit(2)

    if (
        not os.path.isdir('/cache/datasets') or
        not os.path.isdir('/cache/aug') or
        not os.path.isdir('/cache/user_data')
    ):
        print(
            "Cache directories don't exist; are you not running this script "
            "inside Docker?",
            file=sys.stderr,
        )
        sys.exit(1)
    clear_cache('/cache/datasets', only_if_possible=only_if_possible)
    clear_cache('/cache/aug', only_if_possible=only_if_possible)
    clear_cache('/cache/user_data', only_if_possible=only_if_possible)
