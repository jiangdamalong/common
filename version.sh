#!/bin/bash
ver=$(git log --format=%ai\;%H -n 1)
branch=$(git rev-parse --abbrev-ref HEAD)
rm -rf version.go
cat version.go.template | sed "s/DEFAULT_COMMON_VERSION/${branch};${ver}/g" > version.go

