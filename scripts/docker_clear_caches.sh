#!/bin/sh
envfile="$(dirname "$(dirname "$0")")/.env"
if [ -e "$envfile" ]; then
    . "$envfile"
fi
docker run -ti --rm --network datamart_default -v $PWD/scripts:/scripts datamart_coordinator python /scripts/clear_caches.py
