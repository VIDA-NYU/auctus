#!/usr/bin/env python3

import logging
import os
import sys

from datamart_core.fscache import clear_cache


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    if not os.path.isdir('/dataset_cache') or not os.path.isdir('/cache'):
        print(
            "Cache directories don't exist; are you not running this script "
            "inside Docker?",
            file=sys.stderr,
        )
        sys.exit(1)
    clear_cache('/dataset_cache')
    clear_cache('/cache')
