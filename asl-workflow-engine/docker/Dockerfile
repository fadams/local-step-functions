FROM python:3.11-alpine

COPY py/asl_workflow_engine /usr/local/lib/asl_workflow_engine/asl_workflow_engine
COPY py/statelint /usr/local/lib/asl_workflow_engine/statelint
COPY py/setup.py /usr/local/lib/asl_workflow_engine
COPY py/workflow_engine /usr/local/bin

# update-ca-certificates generates a somewhat confusing/worrying warning:
# WARNING: ca-certificates.crt does not contain exactly one certificate or CRL: skipping
# This is however apparently "normal" (though other Linux distros don't do this)
# https://github.com/gliderlabs/docker-alpine/issues/30
# Explicitly add libstdc++ as its used by ujson
RUN apk update && apk upgrade && \
    apk add ca-certificates libstdc++ gcc g++ && \
    update-ca-certificates && \
    pip install -e /usr/local/lib/asl_workflow_engine && \
    # Remove packages only needed during installation from runtime container
    pip uninstall -y setuptools wheel pip && \
    apk del gcc g++ && \
    rm -rf /var/cache/apk/* && \
    # Add unprivileged "asluser" group and user. The uid/gid
    # chosen is a bit arbitrary and 55674 doesn't have any
    # particular significance.
    addgroup -S -g 55674 asluser && \
    adduser -S -u 55674 -G asluser asluser

USER asluser

WORKDIR /tmp

ENTRYPOINT ["/usr/local/bin/workflow_engine"]

#-------------------------------------------------------------------------------
# Build the image (note using .. not . here to reference the Docker context)
# docker build -t asl-workflow-engine -f ./Dockerfile ..

# To debug Docker builds:
# Force output display to ensure you can see the output of each RUN command
# docker build --progress=plain -t asl-workflow-engine -f ./Dockerfile ..
# or disable BuildKit if the old build system is preferred
# DOCKER_BUILDKIT=0 docker build -t asl-workflow-engine -f ./Dockerfile ..

# docker run --rm asl-workflow-engine

# docker run --rm -it asl-workflow-engine sh

