#!/bin/sh
cd "$(dirname "$(dirname "$0")")"
PROJ="$(basename "$(pwd)")"
docker run -ti --rm --network ${PROJ}_default -v $PWD/scripts:/scripts -v /home/ubuntu/index.20190114:/index -w /index -e ELASTICSEARCH_HOSTS=elasticsearch:9200 ${PROJ}_coordinator python /scripts/export_all.py
