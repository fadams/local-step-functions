FROM python:3.6-alpine

COPY py/asl_workflow_engine /usr/src/asl_workflow_engine/
COPY py/setup.py /usr/src/

WORKDIR /usr/src

# This generates a somewhat confusing/worrying warning:
# WARNING: ca-certificates.crt does not contain exactly one certificate or CRL: skipping
# This is however apparently "normal" (though other distros don't do this)
# https://github.com/gliderlabs/docker-alpine/issues/30
RUN apk update && apk add ca-certificates && update-ca-certificates && \
    mv asl_workflow_engine/config.json . && pip install -e . && \
    rm -rf /var/cache/apk/*

CMD ["python", "asl_workflow_engine/workflow_engine.py"]

#-------------------------------------------------------------------------------
# Build the image (note using .. not . here to reference the Docker context)
# docker build -t asl-workflow-engine -f ./Dockerfile ..

# docker run --rm asl-workflow-engine

# docker run --rm -it asl-workflow-engine sh
