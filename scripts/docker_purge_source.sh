#!/bin/sh
docker run -ti --rm --network datamart_default -v $PWD/scripts:/scripts -e ELASTICSEARCH_HOSTS=elasticsearch:9200 datamart_coordinator python /scripts/purge_source.py "$1"
