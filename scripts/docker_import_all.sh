#!/bin/sh
envfile="$(dirname "$(dirname "$0")")/.env"
if [ -e "$envfile" ]; then
    . "$envfile"
fi
docker run -ti --rm --network datamart_default -v $PWD/scripts:/scripts -v /home/ubuntu/index.20190114:/index -e ELASTICSEARCH_HOSTS=elasticsearch:9200 -e AMQP_HOST=rabbitmq -e AMQP_USER=$AMQP_USER -e AMQP_PASSWORD=$AMQP_PASSWORD -e LAZO_SERVER_HOST=lazo_server -e LAZO_SERVER_PORT=50051 datamart_coordinator python /scripts/import_all.py /index
