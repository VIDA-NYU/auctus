#!/bin/sh

# Build the SCDP jar
docker-compose build profiler
docker run --rm -i datamart_profiler cat /usr/src/app/lib_profiler/datamart_profiler/scdp.jar \
    >"$(dirname "$0")/../lib_profiler/datamart_profiler/scdp.jar"
