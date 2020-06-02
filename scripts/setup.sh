#!/bin/sh

set -eux

sudo(){
  if [ $(id -u) = 0 ]; then
    "$@"
  else
    command sudo "$@"
  fi
}

# Set up volume permissions
mkdir -p volumes/datasets && sudo chown -R 998 volumes/datasets
mkdir -p volumes/cache && sudo chown -R 998 volumes/cache
mkdir -p volumes/prometheus && sudo chown -R 65534:65534 volumes/prometheus
mkdir -p volumes/elasticsearch && sudo chown -R 1000:0 volumes/elasticsearch
mkdir -p volumes/grafana && sudo chown -R 472:472 volumes/grafana
