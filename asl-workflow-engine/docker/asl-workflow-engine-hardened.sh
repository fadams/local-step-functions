#!/bin/bash
################################################################################
# Runs up an instance of the ASL Workflow Engine
# Exposes RestAPI on port 4584 and connects to RabbitMQ broker specified in
# the environment variables.
#
# seccomp:
# In this example the ASL Workflow Engine container is hardened by applying
# a restrictive seccomp profile to reduce the syscall surface to only the 69
# syscalls actually required by the application as opposed to the Docker
# default which "disables around 44 system calls out of 300+"
#
# The seccomp.json was created by building Dockerfile.strace and running the
# strace-enabled container for long enough to collect a representative strace
# log, then run through a tool to generate the profile from the strace log.
#
# Important note. The --security-opt no-new-privileges option is necessary,
# as in addition to preventing container processes from gaining additional
# privileges (preventing su, sudo, suid, etc.) it also causes any seccomp
# filters to be applied later, after privileges have been dropped which means
# we can apply a more restrictive set of filters.
#
# Without --security-opt no-new-privileges the container won't start as our
# seccomp profile blocks syscalls required to launch the container.
#
# Capabilities:
# By default, Docker has a default set of permitted capabilities documented in:
# https://docs.docker.com/engine/reference/run/
# AUDIT_WRITE, CHOWN, DAC_OVERRIDE, FOWNER, FSETID, KILL, MKNOD,
# NET_BIND_SERVICE, NET_RAW, SETFCAP, SETGID, SETPCAP, SETUID, SYS_CHROOT
# We set --cap-drop all as this application does not require capabilities.
# https://man7.org/linux/man-pages/man7/capabilities.7.html
#
# This article gives quite a good deep-dive on capabilities and what they do.
# https://www.redhat.com/en/blog/secure-your-containers-one-weird-trick
#
# Read only root filesystem:
# The --read-only flag mounts the container’s root filesystem as read only
# prohibiting writes to locations other than the specified volumes for the
# container. N.B. here we are bind-mounting -v $PWD/asl_store:/tmp if we
# want to use in-container /tmp mount a tmpfs using --tmpfs /tmp
# https://docs.docker.com/engine/reference/commandline/run/#mount-volume--v---read-only
################################################################################

mkdir -p asl_store
docker run --rm -it \
    --security-opt no-new-privileges \
    --security-opt seccomp=$PWD/seccomp.json \
    --cap-drop all \
    --read-only \
    -u $(id -u):$(id -g) \
    -p 4584:4584 \
    -e USE_STRUCTURED_LOGGING=FALSE \
    -e EVENT_QUEUE_CONNECTION_URL="amqp://$(hostname -I | awk '{print $1}'):5672?connection_attempts=20&retry_delay=10&heartbeat=0" \
    -e EVENT_QUEUE_QUEUE_TYPE="AMQP-0.9.1-asyncio" \
    -e JAEGER_AGENT_HOST=$(hostname -I | awk '{print $1}') \
    -v $PWD/asl_store:/tmp \
    asl-workflow-engine

