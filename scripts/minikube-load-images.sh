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
    docker.elastic.co/elasticsearch/elasticsearch:7.10.2
    quay.io/remram44/rabbitmq:3.7.11
    redis:6.2
    minio/minio:RELEASE.2020-10-18T21-54-12Z
    jaegertracing/all-in-one
    registry.gitlab.com/vida-nyu/auctus/lazo-index-service:0.7.1
    auctus_cache-cleaner
    auctus_coordinator
    auctus_apiserver
    auctus_profiler
    auctus_frontend
    auctus_test-discoverer
"
NB_IMAGES=$(set -f; set -- $IMAGES; echo $#)

i=0
for img in $IMAGES; do
    i=$(( i + 1 ))
    echo "$i/$NB_IMAGES - $img"
    sudo -g docker env DOCKER_HOST= docker save $img \
        | sudo -g docker env DOCKER_HOST="$DOCKER_HOST" DOCKER_TLS_VERIFY="$DOCKER_TLS_VERIFY" DOCKER_CERT_PATH="$DOCKER_CERT_PATH" docker load
done
