# Configuration
Many aspects of the ASL Workflow Engine are configurable, with the configuration being broken down into various subsystems like event_queue, notifier, state_engine, rest_api, tracer and metrics. There are two equivalent ways of specifying the configuration: `config.json` or `environment variables` and reasonable defaults have been included for most configurable fields.

`config.json`
```json
{
    "event_queue": {
        "comment": "instance_id should be unique for each instance of the workflow engine in a cluster. It will be used to create a unique per-instance event queue.",
        "queue_name": "asl_workflow_events",
        "instance_id": "9256f7e8-55ec-47ea-885e-8ee1f922efa8",
        "queue_implementation": "AMQP-0.9.1-asyncio",
        "connection_url": "amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=0",
        "connection_options": "",
        "shared_event_consumer_capacity": 1000,
        "instance_event_consumer_capacity": 1000,
        "reply_to_consumer_capacity": 100
    },
    "notifier": {
        "comment": "The topic the workflow engine will publish notifications to. These are rather like AWS CloudWatch events. This currently maps to an AMQP topic exchange and if it is not the default amq.topic then an address string describing the node should be used with full x-declare syntax so that the exchange is declared before it is used.",
        "topic": "{\"node\": {\"x-declare\": {\"exchange\": \"asl_workflow_engine\", \"exchange-type\": \"topic\", \"durable\": true}}}"
    },
    "state_engine": {
        "comment": "The store_url is either a simple JSON file, e.g. ASL_store.json or a Redis URL, e.g. redis://localhost:6379?connection_attempts=20&retry_delay=10. The execution_ttl is the time to live, in seconds, for the execution metadata used by DescribeExecution and GetExecutionHistory. The initial default is one day 60x60x24 = 86400. Whether to raise or lower this value will depend on the available memory of the Redis server. The execution_ttl is only honoured by the Redis store and is ignored by the simple store.",
        "store_url": "ASL_store.json",
        "execution_ttl": 86400
    },
    "rest_api": {
        "host": "0.0.0.0",
        "port": 4584,
        "region": "local"
    },
    "tracer": {
        "implementation": "Jaeger",
        "service_name": "asl_workflow_engine",
        "config": {
            "sampler": {
                "type": "const",
                "param": 1
            },
            "logging": false
        }
    },
    "metrics": {
        "implementation": "Prometheus",
        "config": {
            "port": 9091
        }
    }
}
```

Equivalent environment variables (note that not every field available in the `config.json` is currently configurable using environment variables):

```
EVENT_QUEUE_QUEUE_NAME
EVENT_QUEUE_INSTANCE_ID
EVENT_QUEUE_QUEUE_IMPLEMENTATION
EVENT_QUEUE_QUEUE_TYPE
EVENT_QUEUE_CONNECTION_URL
EVENT_QUEUE_CONNECTION_OPTIONS
EVENT_QUEUE_SHARED_EVENT_CONSUMER_CAPACITY
EVENT_QUEUE_INSTANCE_EVENT_CONSUMER_CAPACITY
EVENT_QUEUE_REPLY_TO_CONSUMER_CAPACITY

NOTIFIER_TOPIC

STATE_ENGINE_STORE_URL
STATE_ENGINE_EXECUTION_TTL

REST_API_HOST
REST_API_PORT
REST_API_REGION
```

**EVENT_QUEUE_QUEUE_NAME** defaults to "asl_workflow_events" and is the name of the shared event queue as described in [Clustering and Scaling](clustering_and_scaling.md).

**EVENT_QUEUE_INSTANCE_ID** is a unique identifier for the ASL Workflow Engine instance. A UUID is the most obvious ID to use here though it could be any value that uniquely identified each instance in a cluster. This value is used in conjunction with EVENT_QUEUE_QUEUE_NAME to form the name of the per-instance event queue.

**EVENT_QUEUE_QUEUE_IMPLEMENTATION** defaults to "AMQP-0.9.1-asyncio". The values AMQP-0.9.1-asyncio and the original AMQP-0.9.1 are currently the only supported types. The -asyncio suffix selects the asyncio transport for AMQP and REST API and is now the default. **Note** that the intention is to deprecate the blocking IO path in the near future.

The intention of making the queue implementation configurable is to allow there to be different implementations of event queue supported, for example "AMQP-0.10", "AMQP-1.0", "AWS-SQS", etc. The value is used internally in a factory method to create the required concrete event queue implementation.

**EVENT_QUEUE_QUEUE_TYPE** defaults to "classic". The values classic and quorum are currently the only supported types and represent the RabbitMQ classic and quorum queue type respectively. Using quorum queues with a clustered RabbitMQ broker may increase resilience, however be aware that it might reduce throughput performance.

**EVENT_QUEUE_CONNECTION_URL** specifies the Connection URL to the messaging fabric. The default is "amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=0" which is a typical RabbitMQ Connection URL that also allows connection retries to be configured. Note that for most deployments the hostname part of the URL will certainly need to be changed from the default value of localhost.

**EVENT_QUEUE_CONNECTION_OPTIONS** is currently unused. The intention is to provide a mechanism to allow additional connection options to be passed directly to the underlying messaging client.

**EVENT_QUEUE_SHARED_EVENT_CONSUMER_CAPACITY** is the "capacity" or "prefetch" value set on the AMQP "shared_event_consumer", which is the message consumer that consumes from the "shared" event queue. That is the queue common to all ASL engine instances (AKA the "input queue"), which is the one configured by EVENT_QUEUE_QUEUE_NAME. The capacity determines the number of unacked messages that may be delivered before message dispatch is stopped. The default value is 1000, but if the number of concurrent executions is large this might need to be increased.

**EVENT_QUEUE_INSTANCE_EVENT_CONSUMER_CAPACITY** is the "capacity" or "prefetch" value set on the AMQP "instance_event_consumer", which is the message consumer that consumes from the per ASL instance event queue. That is the one configured by combining EVENT_QUEUE_QUEUE_NAME and EVENT_QUEUE_INSTANCE_ID. The capacity determines the number of unacked messages that may be delivered before message dispatch is stopped. The default value is 1000, but if the number of concurrent executions is large this might need to be increased. In particular if there are lots of Task states in progress awaiting RPC responses then the number of unacked messages can grow quite large.

**EVENT_QUEUE_REPLY_TO_CONSUMER_CAPACITY**
is the "capacity" or "prefetch" value set on the AMQP "reply_to_consumer", which is the message consumer that consumes from the reply-to queue that is created to handle the replies from the Task state RPC requests. The capacity determines the number of unacked messages that may be delivered before message dispatch is stopped. The default value is 100, but if there are lots of Task states in progress it is possible that a burst of responses might occur. These responses should be handled quickly by the ASL engine, so the value shouldn't need to be as high as for the input queues.

**NOTIFIER_TOPIC** represents a "connection string" to be used by the messaging system to configure the "topic" that [Notification Events](notification_events.md) will be published to. The actual format is rather dependent on the underlying messaging system, but the default value allows the underlying RabbitMQ implementation to be declaratively configured in a fairly sophisticated way. The default:
```
 "{\"node\": {\"x-declare\": {\"exchange\": \"asl_workflow_engine\", \"exchange-type\": \"topic\", \"durable\": true}}}"
 ```
 means perform an AMQP 0.9.1 "exchange-declare" request with an exchange name of "asl_workflow_engine", and exchange type of "topic", making the exchange durable. In other words it requests that the message broker create a topic exchange called "asl_workflow_engine" if that exchange doesn't already exist.
 
**STATE_ENGINE_STORE_URL** specifies the Connection URL to a "store" that holds the ASL state machine, the execution metadata and the execution history used by the observability API. For simple **non-clustered** cases the ASL state machines may be stored in a simple JSON store with a name like "ASL_store.json", in this case the observability information is held in memory. For production environments it is recommended to use the Redis store. In this case the URL takes the form "redis://localhost:6379?connection_attempts=20&retry_delay=10" which specifies the Redis server to use. Like the Event Queue URL the Redis URL allows connection retries to the Redis server to be configured.

**STATE_ENGINE_EXECUTION_TTL** specifies the time-to-live (expiry time) in seconds of execution and execution history metadata used by the observability APIs. The default is one day 60x60x24 = 86400 seconds. The optimum value for this is will depend on the execution rate and the available memory of the Redis server.

**REST_API_HOST** specifies the hostname/IP/interface to start the REST API on. In most cases the default of "0.0.0.0" should be used.

**REST_API_PORT** specifies the port that the REST API should listen on.

**REST_API_REGION** specifies the fallback "region" that will be used when constructing internal ARNs where the information is not available by other means. In general the default value of "local" should be used.

In addition to the main configuration options there are a few low-level options that are only configurable via environment variables:

**USE_STRUCTURED_LOGGING** specifies whether to use more human readable traditional logging of more machine readable structured logging. This defaults to false and thus presents traditional logs, but for production environments it is recommended that this be set to true.

**LOG_LEVEL** specifies the log level to use from the usual choices of: **DEBUG**, **INFO**, **WARN**, **ERROR**, **CRITICAL**. The default level is **INFO**.

**DISABLE_EXECUTIONS_STORE** and **DISABLE_HISTORY_STORE** are intended to be used when the Redis store is enabled to selectively disable storing either the execution metadata used by DescribeExecution or the execution history used by GetExecutionHistory. They are only really intended to be used during performance testing to gauge the impact that observability might have of execution throughput.
