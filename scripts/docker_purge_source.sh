#!/bin/sh
docker run -ti --rm --network datamart_default -v $PWD/scripts:/scripts -v $PWD/volumes/dataset-cache:/dataset_cache -e ELASTICSEARCH_HOSTS=elasticsearch:9200 -e LAZO_SERVER_HOST=lazo_server -e LAZO_SERVER_PORT=50051 datamart_coordinator python /scripts/purge_source.py "$1"
