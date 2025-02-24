#!/bin/bash
################################################################################
# Runs up an instance of the ASL Workflow Engine
# Exposes RestAPI on port 4584 and connects to RabbitMQ broker specified in
# the environment variables.
################################################################################

mkdir -p asl_store
docker run --rm -it \
    -u $(id -u):$(id -g) \
    -p 4584:4584 \
-e TRACER_IMPLEMENTATION=OpenTelemetry \
-e OTEL_EXPORTER_TYPE=otlp-proto-grpc \
    -e USE_STRUCTURED_LOGGING=FALSE \
    -e EVENT_QUEUE_CONNECTION_URL="amqp://$(hostname -I | awk '{print $1}'):5672?connection_attempts=20&retry_delay=10&heartbeat=0" \
    -e EVENT_QUEUE_QUEUE_IMPLEMENTATION="AMQP-0.9.1-asyncio" \
    -e JAEGER_AGENT_HOST=$(hostname -I | awk '{print $1}') \
-e OTEL_EXPORTER_JAEGER_AGENT_HOST=$(hostname -I | awk '{print $1}') \
-e OTEL_EXPORTER_OTLP_ENDPOINT=http://$(hostname -I | awk '{print $1}'):4317 \
    -v $PWD/asl_store:/tmp \
    asl-workflow-engine

# https://opentelemetry-python.readthedocs.io/en/latest/sdk/environment_variables.html
#-e TRACER_IMPLEMENTATION=OpenTelemetry \
#-e OTEL_EXPORTER_TYPE=otlp-proto-grpc \
#-e OTEL_EXPORTER_JAEGER_AGENT_HOST=$(hostname -I | awk '{print $1}') \
#-e OTEL_EXPORTER_OTLP_ENDPOINT=http://$(hostname -I | awk '{print $1}'):4317 \

