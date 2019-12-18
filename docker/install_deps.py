#!/usr/bin/env python3

"""Install dependencies from Pipfile.lock

This scripts is used as part of the Docker build to install all dependencies as
an initial step before building the images. This makes caching efficient,
allowing for faster builds that work offline.

It means all images have all dependencies installed, but thanks to
de-duplication, this generally uses less space if all images exist on the same
machine.
"""

import json
import subprocess
import sys


def main():
    with open(sys.argv[1]) as fp:
        lockfile = json.load(fp)

    packages = []

    for name, dep in lockfile['default'].items():
        if 'path' in dep:
            continue
        elif 'git' in dep:
            packages.extend([
                '-e',
                'git+%s@%s#egg=%s' % (dep['git'], dep['ref'], name),
            ])
        else:
            packages.append('%s=%s' % (name, dep['version']))

    subprocess.check_call(['pip3', 'install'] + packages)


if __name__ == '__main__':
    main()
