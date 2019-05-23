#!/bin/sh

set -eux

cd "$(dirname "$0")"

# POST JSON
curl -v -s -X POST -H 'Content-type: application/json' -d '{"dataset": {"keywords": ["taxi"]}}' http://localhost:8002/search

# POST multipart/form-data as file
curl -v -s -X POST --form "query=@upload.query.json;filename=query" http://localhost:8002/search

# POST multipart/form-data as form-data
curl -v -s -X POST --form 'query={"dataset": {"keywords": ["taxi"]}}' http://localhost:8002/search
