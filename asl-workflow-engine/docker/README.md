# Docker

An Alpine based [Dockerfile](Dockerfile) for the [asl-workflow-engine](..) is as follows:
```
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
    rm -rf /var/cache/apk/*

CMD ["python", "asl_workflow_engine/workflow_engine.py"]
```
To build the image:
```
docker build -t asl-workflow-engine -f ./Dockerfile ..
```
Note that this uses `..` not `.` to reference the Docker context as we need to look in the parent directory.

To launch an asl-workflow-engine container instance:
```
touch ASL_store.json
docker run --rm -it \
    -u $(id -u):$(id -g) \
    -p 4584:4584 \
    -e USE_STRUCTURED_LOGGING=TRUE \
    -e EVENT_QUEUE_CONNECTION_URL="amqp://$(hostname -I | awk '{print $1}'):5672?connection_attempts=20&retry_delay=10&heartbeat=0" \
    -v $PWD/ASL_store.json:/usr/src/ASL_store.json \
    asl-workflow-engine
```
This example launch script makes use of the USE_STRUCTURED_LOGGING and EVENT_QUEUE_CONNECTION_URL environment variables. The configuration environment variables are described in detail in the [Configuration](../documentation/configuration.md) section of the documentation.