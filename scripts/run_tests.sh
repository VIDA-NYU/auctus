#!/bin/bash

export PATH="$HOME/bin:$PATH"

cd "$(dirname "$(dirname "$0")")"

set -eux

# Run frontend tests
docker build -t datamart_frontend_npm -f frontend/Dockerfile --target build .
docker run -ti --name datamart_npm_test --rm datamart_frontend_npm sh -c "CI=true npm run test"

# Re-build and re-start services
docker-compose build --build-arg version=v0.0 coordinator profiler query frontend test_discoverer
docker-compose up -d coordinator
docker-compose up -d --force-recreate profiler query querylb frontend

# XXX: To run with debugger: remove 'query' up here, use 'read' to block, and
# run query container like so:
# docker run -ti --rm --name query --network datamart_default -e ELASTICSEARCH_HOSTS=elasticsearch:9200 -e AMQP_HOST=rabbitmq -e AMQP_USER=${AMQP_USER} -e AMQP_PASSWORD=${AMQP_PASSWORD} -e LAZO_SERVER_HOST=lazo -e LAZO_SERVER_PORT=50051 -v $(pwd)/volumes/datasets:/datasets -v $(pwd)/volumes/cache:/cache datamart_query
#echo "START QUERY MANUALLY" && read i

# Clear cache
docker exec -ti $(basename "$(pwd)")_coordinator_1 sh -c 'rm -rf /cache/*/*'

# Clear index
scripts/docker_purge_source.sh datamart.test

sleep 2

# Re-profile
docker-compose up -d --force-recreate test_discoverer
sleep 10

# Load .env
set +x
cat .env | while read l; do [ -z "$l" ] || [ "${l:0:1}" = \# ] || echo "export $l"; done >.env.sh && . .env.sh && rm .env.sh
set -x

# Run tests
DATAMART_VERSION=v0.0 poetry run python tests
