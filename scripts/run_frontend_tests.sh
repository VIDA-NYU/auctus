#!/bin/bash

cd "$(dirname "$(dirname "$0")")"

set -eu

# Run frontend tests
docker build -t auctus_frontend_npm -f frontend/Dockerfile --target build .
docker run -ti --name auctus_npm_test --rm auctus_frontend_npm sh -c "CI=true npm run test"
