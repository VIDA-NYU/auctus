#!/bin/sh
docker run -ti --rm --network datamart_default -v $PWD/volumes/datasets:/datasets datamart_coordinator sh -c 'tar zc /datasets/datamart.upload.*' >uploads.tar.gz
