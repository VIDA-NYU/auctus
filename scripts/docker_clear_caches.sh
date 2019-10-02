#!/bin/sh
docker run -ti --rm --network datamart_default -v $PWD/scripts:/scripts datamart_coordinator python /scripts/clear_caches.py
