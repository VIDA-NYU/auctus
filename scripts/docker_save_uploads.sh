#!/bin/sh
docker exec -i datamart_profiler_1 sh -c 'tar zc /datasets/datamart.upload.*' >uploads.tar.gz
