#!/bin/bash
################################################################################
# Runs up a standalone draw.io server
# http://localhost:8888
################################################################################

# TODO container seems to need to be run as user root. Need to work out how to
# run correctly using -u $(id -u):$(id -g)

docker run --rm \
    -p 8888:8080 \
    draw

