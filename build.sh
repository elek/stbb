#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

export CGO_ENABLED=0

#echo "==> go vet"
#go vet ./...

echo "==> go build"
go build ./...

echo "==> go test"
go test ./...

#echo "==> go install"
#go install

echo "OK"
