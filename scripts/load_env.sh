#!/bin/bash

sed 's/^/export /' .env >.env.sh && \
. ".env.sh" >/dev/null && \
rm ".env.sh" && \
export DATAMART_VERSION=$(git describe)
export DATAMART_GEO_DATA="$(pwd)/lib_geo/data"
