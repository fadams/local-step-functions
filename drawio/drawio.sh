#!/bin/bash
################################################################################
# Runs up a standalone draw.io server
# http://localhost:8888
################################################################################

docker run --rm \
    -p 8888:8080 \
    -u $(id -u):$(id -g) \
    draw

