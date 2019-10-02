#!/bin/sh
docker run -ti --rm --network datamart_default -v $PWD/scripts:/scripts -v $PWD/volumes/dataset-cache:/dataset_cache -v $PWD/volumes/query-cache:/cache datamart_coordinator python /scripts/clear_caches.py
