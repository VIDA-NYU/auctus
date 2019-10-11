#!/bin/sh

export PATH="$HOME/bin:$PATH"

cd "$(dirname "$(dirname "$0")")"

set -eux

# Re-build and re-start services
#docker-compose build --build-arg version=v0.0 coordinator profiler test_discoverer
docker-compose build --build-arg version=v0.0 query
#docker-compose up -d coordinator profiler querylb
docker-compose up -d --force-recreate query

# Clear cache
docker exec -ti $(basename "$(pwd)")_coordinator_1 sh -c 'rm -rf /cache/*/*'

# Clear index
#scripts/docker_purge_source.sh datamart.test

sleep 2

# Re-profile
#docker-compose up -d --force-recreate test_discoverer
#sleep 10

# Run tests
DATAMART_VERSION=v0.0 pipenv run python tests tests.test_integ.TestAugment.test_agg_join
