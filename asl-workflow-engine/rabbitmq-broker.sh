#!/bin/bash
################################################################################
# Runs up a standalone RabbitMQ broker
# Exposes AMQP 0.9.1 on port 5672 and management interface on 15672
################################################################################

docker run --rm \
    -h rabbitmq \
    -u $(id -u):$(id -g) \
    -p 5672:5672 \
    -p 15672:15672 \
    rabbitmq:3-management

