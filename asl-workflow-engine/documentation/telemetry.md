# Tracing/Telemetry
The ASL Workflow Engine has supported OpenTracing for some time and now also supports OpenTelemetry.

For the moment the default remains OpenTracing to allow for controlled migration to OpenTelemetry by actively setting OpenTelemetry configuration. In due course, however, OpenTelemetry is likely to become the default.

There are two (mostly) equivalent ways of specifying the configuration: `config.json` or `environment variables` and reasonable defaults have been included for most configurable fields. Note however that much of the lower level internal configuration of the OpenTelemetry SDK is via environment variables, which have not been exposed in the `config.json`. The relevant section of `config.json` is the `tracer` field an example for the Jaeger Client is:

`config.json`
```json
{
    ...
    "tracer": {
        "implementation": "Jaeger",
        "service_name": "asl_workflow_engine",
        "config": {
            "sampler": {
                "type": "probabilistic",
                "param": 0.01
            },
            "logging": false
        }
    },
    ...
}
```
and for OpenTelemetry is:

`config.json`
```json
{
    ...
    "tracer": {
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
    },
    ...
}
```

Equivalent environment variables for the main configuration if the tracer are as follows:

**TRACER_IMPLEMENTATION** defaults to "Jaeger" and may be set to either "Jaeger" or "OpenTelemetry" to set the tracer implementation.

**JAEGER_SAMPLER_TYPE** is a Jaeger specific environment variable that corresponds to the `tracer.config.sampler.type` field and defaults to "probabilistic".

**JAEGER_SAMPLER_PARAM** is a Jaeger specific environment variable that corresponds to the `tracer.config.sampler.param` field and defaults to 0.01.

**OTEL_TRACES_SAMPLER** is an OpenTelemetry specific environment variable specified in the [OpenTelemetry SDK](https://opentelemetry-python.readthedocs.io/en/latest/sdk/environment_variables.html) that sets the sampler to be used for traces. Sampling is a mechanism to control the noise introduced by OpenTelemetry by reducing the number of traces collected and sent to the backend Default: “parentbased_always_on”

**OTEL_TRACES_SAMPLER_ARG** is an OpenTelemetry specific environment variable specified in the [OpenTelemetry SDK](https://opentelemetry-python.readthedocs.io/en/latest/sdk/environment_variables.html) it will only be used if OTEL_TRACES_SAMPLER is set. Each Sampler type defines its own expected input, if any. Invalid or unrecognized input is ignored, i.e. the SDK behaves as if OTEL_TRACES_SAMPLER_ARG is not set.

Note that the `tracer.config.sampler.type` and `tracer.config.sampler.param` fields set in the default `config.json` are respected by the ASL Workflow Engine OpenTelemetry implementation. That is to say the default values of "probabilistic"/0.01 causes OTEL_TRACES_SAMPLER to be implicitly set to "parentbased_always_on" and OTEL_TRACES_SAMPLER_ARG to be implicitly set to "0.01" without needing to explicitly set these environment variables. For more information on sampling see the [OpenTelemetry SDK documentation](https://opentelemetry.io/docs/concepts/sampling/).

One of the issues that may be encountered when migrating to OpenTelemetry is where an OpenTracing ecosystem is already in place. One example of this is where most of the ecosystem currently uses OpenTracing and a Jaeger Server may be configured to use an Agent collecting spans transported via the Jaegert thrift protocol. Fortunately OpenTelemetry supports a range of "exporters".

**OTEL_EXPORTER_TYPE** allows the ASL Workflow Engine to select the required exporter. Supported values are: "otlp-proto-grpc", "otlp-proto-http" and "jaeger-thrift". The default value is "otlp-proto-grpc" and OTLP over gRPC is supported natively by the Jaeger Server, but that requires configuration which may not be in place in an ecosystem currently set up for OpenTelemetry so setting OTEL_EXPORTER_TYPE to "jaeger-thrift" exports Jager thrift over UDP by default to `localhost:6831`.

**OTEL_EXPORTER_JAEGER_AGENT_HOST** and **OTEL_EXPORTER_JAEGER_AGENT_PORT** allow the host and port of the Jaeger Server to be configured when "jaeger-thrift" has been selected as the exporter.

**OTEL_EXPORTER_OTLP_ENDPOINT** allows the URI endpoint to be configured when "otlp-proto-grpc" or "otlp-proto-http" has been selected as the exporter. The default is "http://localhost:4317" when "otlp-proto-grpc" has been selected or "http://localhost:4318" when "otlp-proto-http" has been selected.

Another issue that may be encountered in a mixed OpenTracing/OpenTelemetry ecosystem relates to span [propagation](https://opentelemetry-python.readthedocs.io/en/stable/api/propagate.html). By default OpenTelemetry uses the "traceparent" header for propagation, but Jaeger OpenTracing Client uses "uber-trace-id". The way to mitigate this is to include opentelemetry-propagator-jaeger and configure the CompositePropagator to make use of it.

**OTEL_PROPAGATORS** specified in the [OpenTelemetry SDK](https://opentelemetry.io/docs/languages/sdk-configuration/general/#otel_propagators) allows this value to be set. The default is "tracecontext,baggage" and to include the support for "uber-trace-id" this may be set to "tracecontext,baggage,jaeger". Although the ASL Workflow Engine supports OTEL_PROPAGATORS, for convenience it also supports OTEL_ADDITIONAL_PROPAGATORS such that if that is set to "jaeger" OTEL_PROPAGATORS will implicitly be set to "tracecontext,baggage,jaeger" as in most cases the W3C "traceparent" header is likely to be required.

Note that to facilitate smooth migration between OpenTracing and OpenTelemetry the ASL Workflow Engine by default implicitly sets OTEL_PROPAGATORS to "tracecontext,baggage,jaeger" so it is only necessary to set OTEL_PROPAGATORS or OTEL_ADDITIONAL_PROPAGATORS if this is not required.

One other "quirk" with OpenTelemetry is that it has not (yet) got an SDK that natively uses asyncio. This means that the BatchSpanProcessor used to batch spans prior to exporting runs as a worker thread. For the OTLP exporters this is (mostly) OK, but for Jaeger thrift it can cause performance problems especially where const/always_on sampling is used. Jaeger Client's Reporter is Jaeger's equivalent of BatchSpanProcessor and that uses tornado/asyncio coroutines and much smaller queue and batch sizes as the spans are exported over UDP and it's important to fit the batch into the maximum UDP packet size. A side effect of Jaeger's Reporter approach is that under load it will drop spans in preference to slowing down the system being observed. The ASL Workflow Engine has implemented an AsyncBatchSpanProcessor that behaves more like Jaeger Client's Reporter when the "jaeger-thrift" exporter has been specified.

**OTEL_USE_ASYNC_BSP** defaults to "true" to select AsyncBatchSpanProcessor, but if it is explicitly set to "false" the ASL Workflow Engine will revert to using the BatchSpanProcessor.

