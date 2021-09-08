#!/bin/sh
set -eux

cd "$(dirname "$0")/.."

VERSION=$(git describe)

# Build
docker build -t auctus --build-arg version=$VERSION .
docker build -t auctus_frontend -f frontend/Dockerfile .

# Push
docker tag auctus registry.gitlab.com/vida-nyu/auctus/auctus:$VERSION
docker push registry.gitlab.com/vida-nyu/auctus/auctus:$VERSION
docker tag auctus_frontend registry.gitlab.com/vida-nyu/auctus/auctus/frontend:$VERSION
docker push registry.gitlab.com/vida-nyu/auctus/auctus/frontend:$VERSION
