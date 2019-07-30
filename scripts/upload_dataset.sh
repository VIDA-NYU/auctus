#!/bin/sh

set -eu

if [ "$#" != 3 ]; then
    echo "Usage: upload_dataset.sh <filename.csv> \"<name>\" \"<description>\"" >&2
    exit 2
fi
exec curl -X POST \
    -F "file=@$1;filename=$(basename "$1")" -F "name=$2" -F "description=$3" \
    http://localhost:8001/upload
