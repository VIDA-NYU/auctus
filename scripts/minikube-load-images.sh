#!/bin/bash

# This script loads images from the local Docker daemon into the Minikube VM
# It is useful when you want to try locally-built images without pushing them to a registry

set -eu
set -o pipefail

if [ "x${DOCKER_HOST-}" = x ]; then
    echo "DOCKER_HOST is not set; running 'eval \$(minikube docker-env)" >&2
    eval $(minikube docker-env)
else
    echo "DOCKER_HOST is set" >&2
fi

IMAGES="
    busybox
    docker.elastic.co/elasticsearch/elasticsearch:7.6.2
    remram/rabbitmq:3.7.8
    redis:5.0
    registry.gitlab.com/vida-nyu/datamart/lazo-index-service:0.2.0
    datamart_coordinator
    datamart_apiserver
    datamart_profiler
    datamart_frontend
    datamart_test_discoverer
"
NB_IMAGES=$(set -f; set -- $IMAGES; echo $#)

i=0
for img in $IMAGES; do
    i=$(( i + 1 ))
    echo "$i/$NB_IMAGES - $img"
    sudo -g docker env DOCKER_HOST= docker save $img \
        | sudo -g docker env DOCKER_HOST="$DOCKER_HOST" DOCKER_TLS_VERIFY="$DOCKER_TLS_VERIFY" DOCKER_CERT_PATH="$DOCKER_CERT_PATH" docker load
done
