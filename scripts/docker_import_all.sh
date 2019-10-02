#!/bin/sh
cd "$(dirname "$(dirname "$0")")"
PROJ="$(basename "$(pwd)")"
docker run -ti --rm --network ${PROJ}_default -v $PWD/scripts:/scripts -v /home/ubuntu/index.20190114:/index -e ELASTICSEARCH_HOSTS=elasticsearch:9200 -e AMQP_HOST=rabbitmq -e AMQP_USER=$AMQP_USER -e AMQP_PASSWORD=$AMQP_PASSWORD -e LAZO_SERVER_HOST=lazo_server -e LAZO_SERVER_PORT=50051 ${PROJ}_coordinator python /scripts/import_all.py /index
