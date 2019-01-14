#!/bin/sh
docker run -ti --rm --network datamart_default -v $PWD/scripts:/scripts -v /home/ubuntu/index.20190114:/index -w /index -e ELASTICSEARCH_HOSTS=elasticsearch:9200 datamart_coordinator python /scripts/export_all.py
