FROM python:3.6-alpine

COPY py/asl_workflow_engine /usr/src/asl_workflow_engine/
COPY py/setup.py /usr/src/

WORKDIR /usr/src

# update-ca-certificates generates a somewhat confusing/worrying warning:
# WARNING: ca-certificates.crt does not contain exactly one certificate or CRL: skipping
# This is however apparently "normal" (though other Linux distros don't do this)
# https://github.com/gliderlabs/docker-alpine/issues/30
RUN apk update && apk upgrade && \
    apk add \
      ca-certificates \
      gcc \
      g++ && \
    update-ca-certificates && \
    mv asl_workflow_engine/config.json . && pip install -e . && \
    # Remove packages only needed during installation from runtime container
    pip uninstall -y setuptools wheel pip && \
    apk del gcc g++ && \
    rm -rf /var/cache/apk/* && \
    adduser -S -u 55674 asluser root

USER asluser

CMD ["python", "asl_workflow_engine/workflow_engine.py"]

#-------------------------------------------------------------------------------
# Build the image (note using .. not . here to reference the Docker context)
# docker build -t asl-workflow-engine -f ./Dockerfile ..

# docker run --rm asl-workflow-engine

# docker run --rm -it asl-workflow-engine sh

