#!/bin/bash

glide install
REPO_PATH=github.com/laincloud/deployd
GIT_SHA=$(git rev-parse --short HEAD || echo "GitNotFound")

# Set GO_LDFLAGS="-s" for building without symbols for debugging.
GO_LDFLAGS="$GO_LDFLAGS -X ${REPO_PATH}/version.GitSHA=${GIT_SHA}"

docker run --rm -v $GOPATH:/go -e GOPATH=/go -e GOBIN=/go/src/github.com/laincloud/deployd/bin golang:1.10.1 go install -ldflags "$GO_LDFLAGS" github.com/laincloud/deployd
