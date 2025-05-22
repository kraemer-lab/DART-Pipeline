#!/bin/sh
ARCH="$(uname -m)"
if [ "$ARCH" = "x86_64" ]; then
    docker build -t dart-pipeline .
else
    # use buildx to cross-build
    docker buildx build --platform=linux/amd64 -t dart-pipeline .
fi
