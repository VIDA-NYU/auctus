#!/bin/sh

docker run --rm -i datamart_profiler cat /usr/src/app/scdp.jar \
    >"$(dirname "$0")/../datamart_profiler/scdp.jar"
