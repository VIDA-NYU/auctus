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

DOCKER_HOST= sudo -g docker docker save busybox docker.elastic.co/elasticsearch/elasticsearch:7.3.1 remram/rabbitmq:3.7.8 datamart_coordinator datamart_query datamart_profiler datamart_example_discoverer \
    | docker load
