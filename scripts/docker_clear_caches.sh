#!/bin/sh
cd "$(dirname "$(dirname "$0")")"
PROJ="$(basename "$(pwd)")"
docker run -ti --rm --network ${PROJ}_default -v $PWD/scripts:/scripts -v $PWD/volumes/dataset-cache:/dataset_cache -v $PWD/volumes/query-cache:/cache ${PROJ}_coordinator python /scripts/clear_caches.py
