#!/bin/bash

glide install

docker run --rm -v $GOPATH:/go -e GOPATH=/go -e GOBIN=/go/src/github.com/laincloud/deployd/bin golang:1.8.1 go install github.com/laincloud/deployd