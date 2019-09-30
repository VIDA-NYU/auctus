#!/bin/sh

set -eux

sudo(){
  if [ $(id -u) = 0 ]; then
    "$@"
  else
    command sudo "$@"
  fi
}

# Build the SCDP jar
#docker-compose build profiler
#docker run --rm -i datamart_profiler cat /usr/src/app/lib_profiler/datamart_profiler/scdp.jar \
#    >"$(dirname "$0")/../lib_profiler/datamart_profiler/scdp.jar"

# Set up volume permissions
mkdir -p volumes/prometheus && sudo chown -R 65534:65534 volumes/prometheus
mkdir -p volumes/elasticsearch && sudo chown -R 1000:0 volumes/elasticsearch
mkdir -p volumes/grafana && sudo chown -R 427:472 volumes/grafana
