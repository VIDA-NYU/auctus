#!/usr/bin/env python3

"""Install dependencies from poetry.lock.json

This scripts is used as part of the Docker build to install all dependencies as
an initial step before building the images. This makes caching efficient,
allowing for faster builds that work offline.

It means all images have all dependencies installed, but thanks to
de-duplication, this generally uses less space if all images exist on the same
machine.

Poetry actually uses TOML, which Python can't read directly, so we have an
extra step to convert from TOML to JSON before this is run.
"""

import json
import subprocess
import sys


def main():
    with open(sys.argv[1]) as fp:
        lockfile = json.load(fp)

    packages = []

    for package in lockfile['package']:
        if 'source' in package and 'url' in package['source']:
            continue
        packages.append('%s==%s' % (package['name'], package['version']))

    subprocess.check_call(['pip3', 'install'] + packages)


if __name__ == '__main__':
    main()
