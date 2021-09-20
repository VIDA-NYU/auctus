#!/usr/bin/env python3

"""Install dependencies from poetry.lock

This scripts is used as part of the Docker build to install all dependencies as
an initial step before building the images. This makes caching efficient,
allowing for faster builds that work offline.

It means all images have all dependencies installed, but thanks to
de-duplication, this generally uses less space if all images exist on the same
machine.
"""

import subprocess
import sys
import toml


def main(args):
    devel = False
    if args[0] == '--dev':
        devel = True
        args = args[1:]

    with open(args[0]) as fp:
        lockfile = toml.load(fp)

    packages = []

    for package in lockfile['package']:
        if package['category'] == 'dev':
            if not devel:
                continue
        elif package['category'] != 'main':
            raise ValueError(
                "Unknown package category %s" % package['category']
            )

        if 'source' in package:
            if package['source']['type'] == 'git':
                packages.append('git+%s@%s' % (
                    package['source']['url'],
                    package['source']['reference'],
                ))
            elif package['source']['type'] == 'url':
                packages.append(package['source']['url'])
            elif package['source']['type'] != 'directory':
                raise ValueError(
                    "Unknown package source %s" % package['source']['type']
                )
            # Ignore 'directory' dependencies
        else:
            packages.append('%s==%s' % (package['name'], package['version']))

    subprocess.check_call(
        [
            'pip3',
            '--disable-pip-version-check',
            '--no-cache-dir',
            'install',
        ] + packages,
    )


if __name__ == '__main__':
    main(sys.argv[1:])
