#!/bin/bash

cd "$(dirname "$(dirname "$0")")"

set -eux

export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1

# Check lib_geo data
test -e lib_geo/data/admins.sqlite3

# Re-build and re-start services
docker-compose build --build-arg BUILDKIT_INLINE_CACHE=1 --build-arg version=v0.0 cache-cleaner coordinator profiler apiserver test-discoverer
docker-compose up -d cache-cleaner coordinator
sleep 2
docker-compose up -d --force-recreate profiler apiserver apilb

# XXX: To run with debugger: remove 'apiserver' or 'profiler' up here, and
# run apiserver/profiler in another terminal like so:
# docker run -ti --rm --name apiserver --network auctus_default -v $(pwd)/lib_geo/data:/usr/src/app/lib_geo/data -e AUCTUS_DEBUG=yes -e ELASTICSEARCH_HOSTS=elasticsearch:9200 -e ELASTICSEARCH_PREFIX=${ELASTICSEARCH_PREFIX} -e AMQP_HOST=rabbitmq -e AMQP_PORT=5672 -e AMQP_USER=${AMQP_USER} -e AMQP_PASSWORD=${AMQP_PASSWORD} -e REDIS_HOST=redis -e S3_KEY=${S3_KEY} -e S3_SECRET=${S3_SECRET} -e S3_URL=${S3_URL} -e S3_CLIENT_URL=${S3_CLIENT_URL} -e S3_BUCKET_PREFIX=${S3_BUCKET_PREFIX} -e LAZO_SERVER_HOST=lazo -e LAZO_SERVER_PORT=50051 -e NOMINATIM_URL=${NOMINATIM_URL} -e FRONTEND_URL=${FRONTEND_URL} -e API_URL=${API_URL} -e CUSTOM_FIELDS="${CUSTOM_FIELDS}" -v $(pwd)/volumes/datasets:/datasets -v $(pwd)/volumes/cache:/cache auctus_apiserver
# docker run -ti --rm --name profiler --network auctus_default -v $(pwd)/lib_geo/data:/usr/src/app/lib_geo/data -e AUCTUS_DEBUG=yes -e ELASTICSEARCH_HOSTS=elasticsearch:9200 -e ELASTICSEARCH_PREFIX=${ELASTICSEARCH_PREFIX} -e AMQP_HOST=rabbitmq -e AMQP_PORT=5672 -e AMQP_USER=${AMQP_USER} -e AMQP_PASSWORD=${AMQP_PASSWORD} -e S3_KEY=${S3_KEY} -e S3_SECRET=${S3_SECRET} -e S3_URL=${S3_URL} -e S3_CLIENT_URL=${S3_CLIENT_URL} -e S3_BUCKET_PREFIX=${S3_BUCKET_PREFIX} -e LAZO_SERVER_HOST=lazo -e LAZO_SERVER_PORT=50051 -e NOMINATIM_URL=${NOMINATIM_URL} -e FRONTEND_URL=${FRONTEND_URL} -e API_URL=${API_URL} -e CUSTOM_FIELDS="${CUSTOM_FIELDS}" -v $(pwd)/volumes/datasets:/datasets -v $(pwd)/volumes/cache:/cache auctus_profiler
#echo "START AUCTUS-APISERVER MANUALLY" && read i

# Clear cache
docker exec -ti $(basename "$(pwd)")_coordinator_1 sh -c 'rm -rf /cache/*/*'
docker-compose exec redis redis-cli flushall

# Clear index
scripts/docker_purge_source.sh datamart.test
scripts/docker_purge_source.sh upload

sleep 2

# Re-profile
docker-compose up -d --force-recreate test-discoverer

# Wait for profiling to end
(set +x
slept=10
sleep 10
while [ "$(curl -s http://localhost:8012/metrics | sed -n '/^rabbitmq_queue_messages{.*queue="profile".* \([0-9]*\)$/s//\1/p')" != 0 ]; do
  if [ $slept -gt 180 ]; then
    echo "Profiling didn't end after ${slept}s"
    exit 1
  fi
  sleep 1; slept=$((slept + 1))
done
)

# Load .env
set +x
. scripts/load_env.sh
set -x
export DATAMART_VERSION=v0.0

# Run tests
poetry run python tests --verbose --catch
