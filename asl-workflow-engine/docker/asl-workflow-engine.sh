#!/bin/bash
################################################################################
# Runs up an instance of the ASL Workflow Engine
# Exposes RestAPI on port 4584 and connects to RabbitMQ broker specified in
# the environment variables.
################################################################################

touch ASL_store.json
docker run --rm -it \
    -u $(id -u):$(id -g) \
    -p 4584:4584 \
    -e USE_STRUCTURED_LOGGING=TRUE \
    -e EVENT_QUEUE_CONNECTION_URL="amqp://$(hostname -I | awk '{print $1}'):5672?connection_attempts=20&retry_delay=10&heartbeat=0" \
    -e JAEGER_AGENT_HOST=$(hostname -I | awk '{print $1}') \
    -v $PWD/ASL_store.json:/usr/src/ASL_store.json \
    asl-workflow-engine

