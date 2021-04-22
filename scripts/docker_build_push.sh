#!/bin/sh
set -eux

cd "$(dirname "$0")/.."

CONTAINERS="frontend apiserver coordinator cache-cleaner profiler socrata zenodo ckan worldbank uaz-indicators"
VERSION=$(git describe)

# Build
docker-compose build --build-arg version=$VERSION $CONTAINERS

# Push
for i in $CONTAINERS; do
    docker tag auctus_$i registry.gitlab.com/vida-nyu/auctus/auctus/$i:$VERSION
    docker push registry.gitlab.com/vida-nyu/auctus/auctus/$i:$VERSION
done
