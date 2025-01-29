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
opentracing tracer and use either OpenTelemetry or the legacy JaegerTracing
client if configured to do so. For the purposes of migration to OpenTelemetry
the initial ASL Engine default will be JaegerTracing and require explicit
configuration to OpenTelemetry. IDC when that has proved stable the default
will become OpenTelemetry, with the aim of eventually removing Jaeger client.

This module also "patches" the AWS boto3 Python SDK to allow tracing to be
enabled on clients in a fairly transparent non-intrusive way. To do this it
is important to call create_tracer prior to any boto3.client calls, though this
is likely to be a fairly natural pattern.
"""

import sys
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3

import os, opentracing  # Provides a default null opentracing.tracer instance

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

    # Store default tracer in case creating concrete implementation fails.
    tracer = opentracing.tracer
    if config.get("implementation") == "Jaeger":
        create_tracer.logger.info("Creating Jaeger Tracer")
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

    elif config.get("implementation") == "OpenTelemetry":
        create_tracer.logger.info("Creating OpenTelemetry Tracer")
        try:
            """
            Some useful Documents on using OpenTelemetry with Jaeger:
            https://medium.com/jaegertracing/introducing-native-support-for-opentelemetry-in-jaeger-eb661be8183c
            https://msalinas92.medium.com/integrating-a-python-api-with-jaeger-using-opentelemetry-3885e0c80db0
            https://last9.io/blog/how-to-use-jaeger-with-opentelemetry/
            https://opentelemetry.io/docs/migration/opentracing/

            OpenTelemetry Client needs several packages to be installed
            https://opentelemetry.io/docs/languages/python/
            Core API and SDK packages
            pip install opentelemetry-api
            pip install opentelemetry-sdk

            In addition, there are several extension packages which can be
            installed separately (these use ports 4317, 4318, 6831
            respectively) note exporter-jaeger-thrift is considered deprecated
            as Jaeger now supports OTLP natively, but is included here in case
            of any migration issues when using OTLP.
            pip install opentelemetry-exporter-otlp-proto-grpc
            pip install opentelemetry-exporter-otlp-proto-http
            pip install opentelemetry-exporter-jaeger-thrift

            pip install opentelemetry-instrumentation-{instrumentation}
            pip install opentelemetry-opentracing-shim


            By default OpenTelemetry uses the "traceparent" header for
            prpagation, configured via the OTEL_PROPAGATORS env var which
            defaults to "tracecontext,baggage".
            https://dmathieu.com/en/development/opentelemetry-propagation/
            https://opentelemetry-python.readthedocs.io/en/stable/api/propagate.html
            https://opentelemetry.io/docs/languages/python/instrumentation/#change-the-default-propagation-format
            https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/

            To propagate Jaeger's "uber-trace-id" to/from W3C "traceparent"
            headers when injecting/extracting spans we need Jaeger propagator:
            pip install opentelemetry-propagator-jaeger
            (or pip install opentelemetry-propagator-aws-xray for AWS X-Ray)
            and modify the env var "tracecontext,baggage,jaeger".

            ASL Engine respects that OTEL_PROPAGATORS env var but also provided
            convenience configuration via OTEL_ADDITIONAL_PROPAGATORS env var
            or additional_propagators JSON so we only have to configure any
            additional propagators whilst also respecting the defaults which
            makes it easier to temporarily include Jaeger whilst migrating
            and then remove it to avoid unnecessary propagation headers.

            The config argument contains OpenTelemetry configuration of the form:
            {
                "implementation": "OpenTelemetry",
                "service_name": "asl_workflow_engine",
                "config": {
                    "exporter": "otlp-proto-grpc",
                    "additional_propagators": "jaeger",
                    "sampler": {
                        "type": "probabilistic",
                        "param": 0.01
                    }
                }
            }
            The sampler config mirrors the Jaeger Client values for ease of
            transition and in this example maps to the OpenTelemetry values:
            OTEL_TRACES_SAMPLER=parentbased_traceidratio
            OTEL_TRACES_SAMPLER_ARG=0.01
            """

            # Explicitly add a handler for the opentelemetry logger
            init_logging(log_name="opentelemetry")

            # For exporter, additional_propagators, etc. configuration.
            otel_config = config.get("config", {})

            # Initialise propagators before importing
            ap = otel_config.get("additional_propagators", "")
            propagators = "tracecontext,baggage," + ap if ap else "tracecontext,baggage"

            # Set OTEL_PROPAGATORS with new propagators unless it is already set.
            if "OTEL_PROPAGATORS" in os.environ:
                propagators = os.environ.get("OTEL_PROPAGATORS")
            else:
                os.environ["OTEL_PROPAGATORS"] = propagators

            # imports deferred until OpenTelemetry is selected in config.
            from opentelemetry import trace
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider

            """
            Use OpenTracing Shim for OpenTelemetry to ease migration from
            OpenTracing to OpenTelemetry, as the shim allows OpenTracing spans
            to be used directly without requiring code rewrites.
            https://opentelemetry-python.readthedocs.io/en/stable/shim/opentracing_shim/opentracing_shim.html
            """
            from opentelemetry.shim import opentracing_shim

            """
            Get service name from env or config. Set env with the result.
            It is possible to configure programmatically via a construct like:
            trace.set_tracer_provider(
                TracerProvider(
                    resource=Resource.create({"service.name": service_name})
                )
            )
            but the approach used here allows OTEL env vars to be respected
            whilst also allowing JSON config and providing useful defaults. 
            """
            service_name = os.environ.get(
                "OTEL_SERVICE_NAME", config.get("service_name", service_name)
            )
            os.environ["OTEL_SERVICE_NAME"] = service_name

            """
            Configure sampler. The default of parentbased_traceidratio/0.01 is
            (I think) equivalent to Jaeger Client probabilistic/0.01. It seems
            particularly important to use sampling with the OpenTelemetry
            Client, especially when using the jaeger-thrift exporter which
            causes a fairly big performance hit compared to the Jaeger Client
            if set with the default parentbased_always_on sampler.
            """
            sampler = otel_config.get("sampler", {})
            sampler_type = os.environ.get(
                "OTEL_TRACES_SAMPLER", sampler.get("type", "const")
            )

            sampler_arg = os.environ.get(
                "OTEL_TRACES_SAMPLER_ARG", sampler.get("param", "1")
            )

            # Map from Jaeger Client config values to OpenTelemetry values
            if sampler_type == "const":
                arg = str(sampler_arg).lower()
                if arg in ['false', '0', 'none']:
                    sampler_type = "parentbased_always_off"
                    sampler_arg = "0.0"
                else:
                    sampler_type = "parentbased_always_on"
                    sampler_arg = "1.0"
            elif sampler_type == "probabilistic":
                sampler_type = "parentbased_traceidratio"

            # Configured values in env will be read by TracerProvider()
            os.environ["OTEL_TRACES_SAMPLER"] = sampler_type
            os.environ["OTEL_TRACES_SAMPLER_ARG"] = str(sampler_arg)

            # Initialise OpenTelemetry tracer.
            trace.set_tracer_provider(TracerProvider())

            """
            For now default to use our AsyncBatchSpanProcessor as that behaves
            in a very similar way to the Jaeger Client Reporter when supplied
            with the configuration we set below for the jaeger.thrift exporter.
            These settings significantly improve throughput performance for the
            case where jaeger.thrift exporter is used and where always_on/const
            sampling has been used. For OTLP exporters or where probabilistic
            sampling has been used the default queue/batch settings work well.
            """
            use_async_bsp_config = os.environ.get("OTEL_USE_ASYNC_BSP", "true")
            use_async_bsp = (
                use_asyncio and
                str(use_async_bsp_config).lower() not in ['false', '0', 'none']
            )

            # Valid values "otlp-proto-grpc"/"otlp-proto-http"/"jaeger-thrift"
            exporter_type = otel_config.get("exporter", "otlp-proto-grpc")
            exporter = None
            if exporter_type == "otlp-proto-grpc":
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
                exporter = OTLPSpanExporter()
            elif exporter_type == "otlp-proto-http":
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
                exporter = OTLPSpanExporter()
            elif exporter_type == "jaeger-thrift":
                """
                This exporter is considered deprecated as Jaeger now supports
                OTLP natively, but is included here in case of any migration
                issues when using OTLP. It will eventually be removed.
                """
                from opentelemetry.exporter.jaeger.thrift import JaegerExporter
                exporter = JaegerExporter()
                #exporter = JaegerExporter(udp_split_oversized_batches=True)

                """
                The config here Matches the queue_capacity and batch_size values
                used in Jaeger Client Reporter:
                https://github.com/jaegertracing/jaeger-client-python/blob/master/jaeger_client/reporter.py
                Setting OTEL_BSP_SYNC_EXPORT to True causes the exporter.export
                method to be called directly again following the approach used
                by Jaeger Client to call agent.emitBatch.
                """
                if use_async_bsp:
                    os.environ["OTEL_BSP_MAX_QUEUE_SIZE"] = "100"
                    os.environ["OTEL_BSP_MAX_EXPORT_BATCH_SIZE"] = "10"
                    os.environ["OTEL_BSP_SYNC_EXPORT"] = "True"
            else:
                raise NotImplementedError(f"Invalid exporter '{exporter_type}'")

            # Configure the trace processor, adding the exporter.
            if use_async_bsp:
                create_tracer.logger.info("Using AsyncBatchSpanProcessor")
                from asl_workflow_engine.otel_async_batch_span_processor import (
                    AsyncBatchSpanProcessor
                )
                trace.get_tracer_provider().add_span_processor(
                    AsyncBatchSpanProcessor(exporter)
                )
            else:
                create_tracer.logger.info("Using BatchSpanProcessor")
                from opentelemetry.sdk.trace.export import BatchSpanProcessor
                trace.get_tracer_provider().add_span_processor(
                    BatchSpanProcessor(exporter)
                )

            # Create an OpenTracing shim and assign it to the global tracer.
            opentracing.tracer = opentracing_shim.create_tracer(
                trace.get_tracer_provider()
            )

            create_tracer.logger.info(
                f"OpenTelemetry Tracer initialised, exporter: {exporter_type}, propagators: {propagators}, sampler: ({sampler_type}, arg: {sampler_arg})"
            )
        except Exception as e:
            create_tracer.logger.warning("Failed to initialise OpenTelemetry Tracer : {}".format(e))
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

