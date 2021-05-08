#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
This creates an OpenTracing tracer instance. It will default to the null
opentracing tracer and use JaegerTracing if configured to do so.
This module also "patches" the AWS boto3 Python SDK to allow tracing to be
enabled on clients in a fairly transparent non-intrusive way. To do this it
is important to call create_tracer prior to any boto3.client calls, though this
is likely to be a fairly natural pattern.
"""

import sys
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3

import opentracing  # Provides a default null opentracing.tracer instance

from asl_workflow_engine.logger import init_logging


def instrument_StartExecution(params, **kwargs):
    """
    boto3 event handler to create an OpenTracing span when Step Function
    StartExecution is called and then to inject (serialise) the span ID onto
    an HTTP header in the StepFunctions REST API so that the trace can be
    tracked through the Step Function execution and out to the Tasks it calls.

    Some background on extending boto3 via its events subsystem:
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/events.html
    https://github.com/boto/botocore/issues/902
    https://github.com/boto/botocore/blob/develop/botocore/handlers.py#L513
    """

    """
    Start an OpenTracing trace for the boto3 StartExecution request.
    https://opentracing.io/guides/python/tracers/ standard tags are from
    https://opentracing.io/specification/conventions/
    """
    with opentracing.tracer.start_active_span(
        operation_name="StartExecution",
        child_of=opentracing.tracer.active_span,
        tags={
            "component": "boto3",
            "boto3.service_name": "stepfunctions",
            "span.kind": "producer",  # Maybe "client" as it's logically RPC
            "peer.address": params["url"]
        }
    ) as scope:
        params["headers"].update(
            inject_span("http_headers", scope.span, create_tracer.logger)
        )

def patch_boto3():
    """
    Patch boto3 to enable OpenTracing. This first intercepts boto3.client
    calls, and if the client is a stepfunctions client an event handler is
    registered on the StartExecution call that creates a span and injects
    it into the REST API HTTP headers.
    """
    try:
        import boto3

        # Save the original boto3.client function to use in the wrapper.
        boto3_client = boto3.client

        def client_wrapper(*args, **kwargs):
            client = boto3_client(*args, **kwargs)

            if args[0] == "stepfunctions":
                """
                Access boto3 event system and Register instrument_StartExecution
                function to the StartExecution event before-call hook.
                """
                client.meta.events.register(
                    "before-call.stepfunctions.StartExecution",
                    instrument_StartExecution
                )
            return client

        # Patch boto3 to use client_wrapper
        boto3.client = client_wrapper

    except Exception as e:
        create_tracer.logger.warning("Failed to add Tracer to boto3: {}".format(e))

def create_tracer(service_name, config, use_asyncio=False):
    create_tracer.logger = init_logging(service_name)
    create_tracer.logger.info("Creating OpenTracing Tracer")

    # Store default tracer in case creating concrete implementation fails.
    tracer = opentracing.tracer
    if config.get("implementation") == "Jaeger":
        try:
            # import deferred until Jaeger is selected in config.
            import jaeger_client
            import tornado.ioloop

            """
            If Implementation = Jaeger get the Jaeger config from the config
            dict if available, if not present create a sane default config.
            """
            jaeger_config = config.get("config")
            if not jaeger_config:
                jaeger_config = {
                    "sampler": {
                        "type": "const",
                        "param": 1
                    },
                    "logging": False
                }

            jaeger = jaeger_client.Config(
                service_name=config.get("service_name", service_name),
                config=jaeger_config,
            )

            """
            The init_logging(log_name="tornado") is important, though a bit
            obtuse. Without it all subsequent log messages generated will
            be duplicated. The issue is that "under the covers" Jaeger uses
            the tornado https://www.tornadoweb.org async networking library.
            Tornado's IOLoop creates a log handler if necessary when it
            is started, because if there were no handler configured
            you'd never see any of its event loop exception messages.
            The default handler is created for the root logger and ends up
            resulting in duplicate messages for other logs. By explicitly
            adding a handler for the tornado logger, as the following line
            does, the logging should be correctly handled. See:
            https://stackoverflow.com/questions/30373620/why-does-ioloop-in-tornado-seem-to-add-root-logger-handler
            """
            init_logging(log_name="tornado")

            """
            If we are using asyncio we want the tracer to use the main asyncio
            event loop rather than create a new ThreadLoop (which is the default
            behaviour unless a tornado IOLoop is passed. In recent versions of
            Tornado that delegates to the asyncio event loop so getting the
            current tornado IOLoop and passing that to initialize_tracer will
            cause the tracer to use the main event loop.
            """
            if use_asyncio:
                jaeger.initialize_tracer(io_loop=tornado.ioloop.IOLoop.current())
            else:
                jaeger.initialize_tracer()

            create_tracer.logger.info("Jaeger Tracer initialised")
        except Exception as e:
            create_tracer.logger.warning("Failed to initialise Jaeger Tracer : {}".format(e))
            opentracing.tracer = tracer

    patch_boto3()

def span_context(format, carrier, logger):
    """
    Boilerplate to extract the parent OpenTracing SpanContext from the carrier.
    Log a message and start new span if we can't extract SpanContext.
    See https://opentracing.io/docs/overview/inject-extract/
    """
    try:
        span_context = opentracing.tracer.extract(format, carrier)

    except Exception:
        logger.error("Missing trace-id property, unable to deserialise "
                     "SpanContext. Will have to create new trace")
        span_context = opentracing.tracer.start_span(operation_name="dangling_process")

    return span_context

def inject_span(format, span, logger):
    """
    Boilerplate to inject the OpenTracing SpanContext into a carrier in order
    to transport it via HTTP headers, AMQP message headers or ASL Context
    https://opentracing.io/docs/overview/inject-extract/
    """
    carrier = {}
    opentracing.tracer.inject(span, format, carrier)
    logger.debug("Tracing active span: carrier {}".format(carrier))

    return carrier

