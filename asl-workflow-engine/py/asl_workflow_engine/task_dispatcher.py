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

import sys
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3


import math, os, time, uuid, opentracing, urllib.parse
from datetime import datetime, timezone
from aioprometheus import Counter, Histogram

#from asl_workflow_engine.metrics_summary import TimeWindowSummary as Summary
from asl_workflow_engine.metrics_summary import BasicSummary as Summary

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.open_tracing_factory import span_context, inject_span
from asl_workflow_engine.messaging_exceptions import *
from asl_workflow_engine.arn import *

try:  # Attempt to use ujson if available https://pypi.org/project/ujson/
    import ujson as json
except:  # Fall back to standard library json
    import json

# https://docs.aws.amazon.com/step-functions/latest/dg/limits.html
MAX_DATA_LENGTH = 262144  # Max length of the input or output JSON string.

class TaskDispatcher(object):
    def __init__(self, state_engine, config):
        """
        :param state_engine: The StateEngine calling this TaskDispatcher
        :type state_engine: StateEngine
        :param config: Configuration dictionary
        :type config: dict
        """
        self.logger = init_logging(log_name="asl_workflow_engine")
        self.logger.info("Creating TaskDispatcher, using {} JSON parser".format(json.__name__))

        """
        Get the messaging peer.address, e.g. the Broker address for use
        by OpenTracing later.
        """
        queue_config = config.get("event_queue", {})
        peer_address_string = queue_config.get("connection_url",
                                               "amqp://localhost:5672")
        try:
            parsed_peer_address = urllib.parse.urlparse(peer_address_string)
        except Exception as e:
            self.logger.error(
                "Invalid peer address found: {}".format(peer_address_string),
                exc_info=e
            )
            raise e        
        
        self.peer_address = parsed_peer_address.hostname
        if parsed_peer_address.port:
            self.peer_address = self.peer_address + ":" + str(parsed_peer_address.port)
        if parsed_peer_address.scheme:
            self.peer_address = parsed_peer_address.scheme + "://" + self.peer_address

        # Get the channel capacity/prefetch value for the reply_to consumer
        capacity = queue_config.get("reply_to_consumer_capacity", 100)
        self.reply_to_capacity = int(float(capacity))  # Handles numbers or strings

        self.state_engine = state_engine

        """
        Some services that we integrate Task States with might be request/
        response in the ASL sense, as in we would want to not move to the next
        state until the response has been received, but they might be async
        in implementation terms, such as the case for rpcmessage. This is
        conceptually RPC like so logically behaves like a Lambda call, but is
        implemented over a messaging fabric with queues.

        In order to deal with this we need to be able to associate requests
        with their subsequent responses, so this pending_requests dictionary
        maps requests with their callbacks using correlation IDs.
        """
        self.pending_requests = {}

        """
        In some cases we might wish to "cancel" pending requests, but the
        State Engine generally indexes using the event ID. To cater for this
        we also have the cancellers dict to map event ID to request ID
        """
        self.cancellers = {}

        """
        Prometheus metrics intended to emulate Stepfunction CloudWatch metrics.
        https://docs.aws.amazon.com/step-functions/latest/dg/procedure-cw-metrics.html
        """
        self.task_metrics = {}
        metrics_config = config.get("metrics", {})
        if metrics_config.get("implementation", "") == "Prometheus":
            ns = metrics_config.get("namespace", "")
            ns = ns + "_" if ns else ""
            
            self.task_metrics = {
                "LambdaFunctionTime": Summary(
                    ns + "LambdaFunctionTime",
                    "The interval, in milliseconds, between the time the " +
                    "Lambda function is scheduled and the time it closes."
                ),
                "LambdaFunctionsFailed": Counter(
                    ns + "LambdaFunctionsFailed",
                    "The number of failed Lambda functions."
                ),
                "LambdaFunctionsScheduled": Counter(
                    ns + "LambdaFunctionsScheduled",
                    "The number of scheduled Lambda functions."
                ),
                "LambdaFunctionsSucceeded": Counter(
                    ns + "LambdaFunctionsSucceeded",
                    "The number of successfully completed Lambda functions."
                ),
                "LambdaFunctionsTimedOut": Counter(
                    ns + "LambdaFunctionsTimedOut",
                    "The number of Lambda functions that time out on close."
                ),
                #"ServiceIntegrationTime": Summary(
                #    ns + "ServiceIntegrationTime",
                #    "The interval, in milliseconds, between the time the " +
                #    "Service Task is scheduled and the time it closes."
                #),
                #"ServiceIntegrationsFailed": Counter(
                #    ns + "ServiceIntegrationsFailed",
                #    "The number of failed Service Tasks."
                #),
                "ServiceIntegrationsScheduled": Counter(
                    ns + "ServiceIntegrationsScheduled",
                    "The number of scheduled Service Tasks."
                ),
                "ServiceIntegrationsSucceeded": Counter(
                    ns + "ServiceIntegrationsSucceeded",
                    "The number of successfully completed Service Tasks."
                ),
                #"ServiceIntegrationsTimedOut": Counter(
                #    ns + "ServiceIntegrationsTimedOut",
                #    "The number of Service Tasks that time out on close."
                #)
            }


    def start(self, session):
        """
        Connect to the messaging fabric to enable Task States to integrate with
        "rpcmessage" based services as described in execute_task.
        """
        #self.reply_to = session.consumer()  # reply_to with normal priority
        """
        Increase the consumer priority of the reply_to consumer.
        See https://www.rabbitmq.com/consumer-priority.html
        N.B. This syntax uses the JMS-like Address String which gets parsed into
        implementation specific constructs. The link/x-subscribe is an
        abstraction for AMQP link subscriptions, which in AMQP 0.9.1 maps to
        channel.basic_consume and alows us to pass the exclusive and arguments
        parameters. NOTE HOWEVER that setting the consumer priority is RabbitMQ
        specific and it might well not be possible to do this on other providers.
        """
        self.reply_to = session.consumer(
            '; {"link": {"x-subscribe": {"arguments": {"x-priority": 10}}}}'
        )

        # Enable consumer prefetch
        self.logger.info("Setting reply_to.capacity to {}".format(self.reply_to_capacity))
        self.reply_to.capacity = self.reply_to_capacity
        self.reply_to.set_message_listener(self.handle_rpcmessage_response)
        self.producer = session.producer()

        # Handle non-existent AMQP rpcmessage processors - see callback comments.
        self.producer.set_return_callback(self.handle_unroutable_rpcmessage)

        #print(self.reply_to.name)
        #print(self.producer.name)

    # asyncio version of the start() method above
    async def start_asyncio(self, session):
        """
        Connect to the messaging fabric to enable Task States to integrate with
        "rpcmessage" based services as described in execute_task.
        """
        #self.reply_to = await session.consumer()  # reply_to with normal priority
        """
        Increase the consumer priority of the reply_to consumer.
        See https://www.rabbitmq.com/consumer-priority.html
        N.B. This syntax uses the JMS-like Address String which gets parsed into
        implementation specific constructs. The link/x-subscribe is an
        abstraction for AMQP link subscriptions, which in AMQP 0.9.1 maps to
        channel.basic_consume and alows us to pass the exclusive and arguments
        parameters. NOTE HOWEVER that setting the consumer priority is RabbitMQ
        specific and it might well not be possible to do this on other providers.
        """
        self.reply_to = await session.consumer(
            '; {"link": {"x-subscribe": {"arguments": {"x-priority": 10}}}}'
        )

        # Enable consumer prefetch
        self.logger.info("Setting reply_to.capacity to {}".format(self.reply_to_capacity))
        self.reply_to.capacity = self.reply_to_capacity
        await self.reply_to.set_message_listener(self.handle_rpcmessage_response)
        self.producer = await session.producer()

        # Handle non-existent AMQP rpcmessage processors - see callback comments.
        self.producer.set_return_callback(self.handle_unroutable_rpcmessage)

        #print(self.reply_to.name)
        #print(self.producer.name)

    def handle_unroutable_rpcmessage(self, message):
        """
        It is possible that a Task Resource might specify an AMQP rpcmessage
        processor that hasn't been started. It is impossible for the ASL Engine
        to detect if the rpcmessage processor exists per se, however in general
        the resource part of the Task's Resource ARN is the name of the
        processor's queue and if the processor uses autodelete queues the queue
        lifecycle should mirror the processor lifecycle.

        We publish RPC messages with the mandatory flag set
        https://www.rabbitmq.com/reliability.html#routing
        and pass this method as the return_callback to handle AMQP Basic.Return.
        """
        error_message = "Specified Task Resource arn:aws:rpcmessage:local::function:{} is not currently available".format(message.subject)
        error = {"errorType": "States.TaskFailed", "errorMessage": error_message}
        error_as_bytes = json.dumps(error).encode('utf-8')

        message.body = error_as_bytes

        """
        Delegate to "real" response handler as we should be able to handle this
        like any other Task error given the error object we've just added.

        We check the correlation_id is in self.pending_requests before
        delegating, because an unroutable rpcmessage will result in immediate
        cancellation of all Tasks in Map Iterators or Parallel Branches and this
        check suppresses spurious info logs that are only really relevant where
        we have *real* "orphaned" RPC responses coming from *actual* resources.
        """
        if message.correlation_id in self.pending_requests:
            self.handle_rpcmessage_response(message)

    def handle_rpcmessage_response(self, message):
        """
        This is a message listener receiving messages from the reply_to queue
        for this workflow engine instance.
        TODO cater for the case where requests are sent but responses never
        arrive, this scenario will cause self.pending_requests to "leak" as
        correlation_id keys get added but not removed. This situation should be
        improved as we add code to handle Task state "rainy day" scenarios such
        as Timeouts etc. so park for now, but something to be aware of.
        """
        correlation_id = message.correlation_id
        request = self.pending_requests.get(correlation_id)
        if request:
            try:
                del self.pending_requests[correlation_id]
                state_machine, execution_arn, resource_arn, callback, sched_time, timeout_id, rpcmessage_task_span = request
                with opentracing.tracer.scope_manager.activate(
                    span=rpcmessage_task_span,
                    finish_on_close=True
                ) as scope:
                    # Cancel the timeout previously set for this request.
                    self.state_engine.event_dispatcher.clear_timeout(timeout_id)
                    if callable(callback):
                        message_body = message.body
                        """
                        First check if the response has exceeded the 262144
                        character quota described in Stepfunction Quotas page.
                        https://docs.aws.amazon.com/step-functions/latest/dg/limits.html
                        We do the test here as we have the raw JSON string handy.
                        """
                        if len(message_body) > MAX_DATA_LENGTH:
                            result = {"errorType": "States.DataLimitExceeded"}
                        else:
                            message_body_as_string = message_body.decode("utf8")
                            result = json.loads(message_body_as_string)
                        error_type = result.get("errorType")
                        if error_type:
                            error_message = result.get("errorMessage", "")
                            opentracing.tracer.active_span.set_tag("error", True)
                            opentracing.tracer.active_span.log_kv(
                                {
                                    "event": error_type,
                                    "message": error_message,
                                }
                            )

                            self.state_engine.update_execution_history(
                                state_machine,
                                execution_arn,
                                "LambdaFunctionFailed",
                                {
                                    "error": error_type,
                                    "cause": error_message,
                                },
                            )

                            if self.task_metrics:
                                """
                                When Lambda times out it returns JSON including
                                "errorType": "TimeoutError"
                                See the following for an example illustration
                                https://stackoverflow.com/questions/65036533/my-lambda-is-throwing-a-invoke-error-timeout
                                rpcmessage processors should follow the same
                                convention so we can trap processor timeout
                                errors and provide metrics on these.
                                """

                                """
                                The following is commented out because although
                                the documentation in https://docs.aws.amazon.com/step-functions/latest/dg/procedure-cw-metrics.html for LambdaFunctionsTimedOut says: 
                                "The number of Lambda functions that time out
                                on close." it seems that real AWS Stepfunctions
                                actually use the LambdaFunctionsTimedOut metric
                                to record the count of timed out Task states
                                """
                                """
                                if error_type == "TimeoutError":
                                    self.task_metrics["LambdaFunctionsTimedOut"].inc(
                                        {"LambdaFunctionArn": resource_arn}
                                    )
                                """

                                self.task_metrics["LambdaFunctionsFailed"].inc(
                                    {"LambdaFunctionArn": resource_arn}
                                )
                        else:
                            self.state_engine.update_execution_history(
                                state_machine,
                                execution_arn,
                                "LambdaFunctionSucceeded",
                                {
                                    "output": message_body_as_string,
                                },
                            )

                            if self.task_metrics:
                                self.task_metrics["LambdaFunctionsSucceeded"].inc(
                                    {"LambdaFunctionArn": resource_arn}
                                )

                        if self.task_metrics:
                            duration = (time.time()  * 1000.0) - sched_time
                            self.task_metrics["LambdaFunctionTime"].observe(
                                {"LambdaFunctionArn": resource_arn}, duration
                            )

                        callback(result)
            except ValueError as e:
                self.logger.error(
                    "Response {} does not contain valid JSON".format(message.body)
                )
        else:
            with opentracing.tracer.start_active_span(
                operation_name="Task",
                child_of=span_context("text_map", message.properties, self.logger),
                tags={
                    "component": "task_dispatcher",
                    "message_bus.destination": self.reply_to.name,
                    "span.kind": "consumer",
                    "peer.address": self.peer_address
                }
            ) as scope:
                self.logger.info("Response {} has no matching requestor".format(message))
                scope.span.set_tag("error", True)
                scope.span.log_kv(
                    {
                        "event": "No matching requestor",
                        "message": "Response has no matching requestor",
                    }
                )


        message.acknowledge(multiple=False)

    def set_timeout_canceller(self, id, callback, timeout_id):
        #print("set_timeout_canceller " + str(id))
        self.cancellers[id] = (callback, timeout_id)

    def remove_canceller(self, id):
        #print("remove_canceller " + str(id))
        if id in self.cancellers:
            del self.cancellers[id]

        #print(self.cancellers)

    def cancel_task(self, id):
        #print("cancel_task " + str(id))

        task = self.cancellers.get(id)
        if task:
            del self.cancellers[id]
            if isinstance(task, str):  # Represents Task correlation_id
                # Do lookup in case timeout fires after a successful response.
                request = self.pending_requests.get(task)
                if request:
                    del self.pending_requests[task]
                    state_machine, execution_arn, resource_arn, callback, sched_time, timeout_id, task_span = request
            else:  # Tuple representing Wait state cancellation information
                callback, timeout_id = task

                # Cancel the timeout previously set for this request.
                self.state_engine.event_dispatcher.clear_timeout(timeout_id)

            if callback and callable(callback):
                error = {
                    "errorType": "Task.Terminated",
                    "errorMessage": "Task has been Terminated",
                }

                callback(error)

        #print(self.cancellers)
        #print()
        #print(self.pending_requests.keys())

    def execute_task(self, resource_arn, parameters, callback, timeout, context, id):
        """
        Look up stateMachineArn to get State Machine as we need that later to
        call state_engine.update_execution_history. We do it here to capture
        in the execute_task closure, so it may be used in error_callback too.
        """
        state_machine_arn = context["StateMachine"]["Id"]
        state_machine = self.state_engine.asl_store.get_cached_view(state_machine_arn)

        """
        In the EventDispatcher constructor we have a "Connection Factory" for
        the event queue that lets the messaging implementation used be set
        via the configuration e.g. AMQP-0.9.1-asyncio. Part of that is to set
        the Message implementation class on event_dispatcher globals(), so
        we can do the import of the Message class from event_dispatcher.
        """
        from asl_workflow_engine.event_dispatcher import Message

        """
        Use the value of the “Resource” field to determine the type of the task
        to execute. For real AWS Step Functions the service integrations are
        described in the following link:
        https://docs.aws.amazon.com/step-functions/latest/dg/concepts-service-integrations.html
        For now the emphasis will be on executing FaaS functions, initially via
        AMQP 0.9.1 request/response, Oracle Fn Project and OpenFaaS and also
        (hopefully) real AWS Lambda.

        The intention is to specify resources as "Amazon Resource Names" as
        detailed in the following link:
        https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html

        Clearly for real AWS services this is essential, so for Lambda we'd use:
        arn:aws:lambda:region:account-id:function:function-name
        however, for consistency it also makes sense to follow this pattern even
        for non-AWS resources. The initial proposal is for the following formats:

        For async messaging based (e.g. AMQP) RPC invoked functions/services:
        arn:aws:rpcmessage:local::function:function-name

        TODO
        In addition, this resource supports the following Parameters in the
        Task state in order to control the configuration of the messaging
        system used to transport the RPC.

        "Parameters": {
            "URL": "amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=0",
            "Type": "AMQP-0.9.1",
            "Queue", "Worker's input queue name",
            "Message.$": "$"
        }

        If the Parameters field is omitted from the ASL then the messaging
        connection used to connect to the event queue shall be used and the
        "effective parameters" passed to execute_task shall simply be the
        Task State's input, however if the Parameters field is included then
        the "effective parameters" passed to execute_task shall be as above
        where Message will be set to the Task State's input. In this case the
        Resource ARN should have the function-name omitted. This is to allow
        us to disambiguate when we want to call the resource ARN directly
        and when we need to supplement the ARN with the parameters.
        
        For OpenFaaS (https://www.openfaas.com) functions:
        arn:aws:openfaas:local::function:function-name

        For Fn (https://fnproject.io) functions
        arn:aws:fn:local::function:function-name

        As these are all essentially Function As A Service approaches the "ARN"
        format is largely the same as for AWS Lambda except the service namespace
        part is rpcmessage, openfaas or fn to reflect the actual service, the
        region is "local" and the account-id is omitted.

        If the supplied resource starts with $ the resource will be treated as
        an environment variable and the real ARN will be looked up from there.
        
        Timeout is in ms as a floating point number.
        """

        def error_callback(carrier, error, callback):
            """
            A wrapper for use when returning errors to the calling Task.
            The wrapper is mainly to provide OpenTracing boilerplate.
            The carrier should be provided in the case where we don't have the
            original span relating to the request. In that case we create a new
            span to represent this error from the information provided in
            carrier, which should be an OpenTracing Carrier. If we do have the
            original span then carrier should be None, and the span should be 
            managed in the code calling this function.
            We pass in the callback to be used by this function as it may be
            called from something asynchronous like a timeout, where the
            callback may have been stored in pending_requests rather than using
            the callback wrapped in the closure of execute_task.
            """
            scope = None
            if carrier:
                scope = opentracing.tracer.start_active_span(
                    operation_name="Task",
                    child_of=span_context("text_map", carrier, self.logger),
                )
            else:
                scope = opentracing.tracer.scope_manager.active
            
            with scope:
                opentracing.tracer.active_span.set_tag("error", True)
                opentracing.tracer.active_span.log_kv(
                    {
                        "event": error["errorType"],
                        "message": error["errorMessage"],
                    }
                )

                if error["errorType"] == "States.Timeout":
                    execution_arn = context["Execution"]["Id"]
                    self.state_engine.update_execution_history(
                        state_machine,
                        execution_arn,
                        "LambdaFunctionTimedOut",
                        {
                            "error": error["errorType"],
                            "cause": error["errorMessage"],
                        },
                    )

                    """
                    It appears that real AWS Stepfunctions use the
                    LambdaFunctionsTimedOut metric to record the count of Task
                    State timeouts and record actual Lambda timeouts using the
                    LambdaFunctionsFailed metric.
                    """
                    if self.task_metrics:
                        self.task_metrics["LambdaFunctionsTimedOut"].inc(
                            {"LambdaFunctionArn": resource_arn}
                        )

                if callable(callback):
                    callback(error)

        """
        If resource_arn starts with $ then attempt to look up its value from
        the environment, and if that fails return its original value.
        """
        if resource_arn.startswith("$"):
            resource_arn = os.environ.get(resource_arn[1:], resource_arn)
        # If resource_arn still starts with $ its value isn't in the environment
        if resource_arn.startswith("$"):
            message = "Specified Task Resource {} is not available on the environment".format(
                resource_arn
            )
            error = {"errorType": "InvalidResource", "errorMessage": message}
            error_callback(context.get("Tracer", {}), error, callback)
            return

        arn = parse_arn(resource_arn)
        service = arn["service"]  # e.g. rpcmessage, fn, openfaas, lambda
        resource_type = arn["resource_type"]  # Should be function most times
        resource = arn["resource"]  # function-name

        """
        Define nested functions as handlers for each supported service type.

        That the methods are prefixed with "asl_service_" is a mitigation against
        accidentally or deliberately placing an unsupported service type in the ARN.
        """
        def asl_service_rpcmessage():
            """
            Publish message to the required rpcmessage worker resource. The
            message body is a JSON string containing the "effective parameters"
            passed from the StateEngine and the message subject is the resource
            name, which in turn maps to the name of the queue that the resource
            has set up to listen on. In order to ensure that responses from
            Task resources return to the correct calling Task the message is
            populated with a reply to address representing this this workflow
            engine's reply_to queue as well as a correlation ID to ensure that
            response messages can be correctly tracked irrespective of the order
            that they are returned - as that might be quite different from the
            order that the requests were sent.

            Setting content_type to application/json isn't necessary for correct
            operation, however it is the correct thing to do:
            https://www.ietf.org/rfc/rfc4627.txt.
            """
            # print("asl_service_rpcmessage")
            # print(service)
            # print(resource_type)
            # print(resource)
            # print(parameters)
            # TODO deal with the case of delivering to a different broker.

            # Associate response callback with this request via correlation ID.
            correlation_id = str(uuid.uuid4())

            # Map the event ID for the Task to request ID.
            self.cancellers[id] = correlation_id

            """
            Create a timeout in case the rpcmessage invocation fails.
            The timeout sends an error response to the calling Task state and
            deletes the pending request.
            """
            def on_timeout():
                # Do lookup in case timeout fires after a successful response.
                request = self.pending_requests.get(correlation_id)
                if request:
                    del self.pending_requests[correlation_id]
                    state_machine, execution_arn, resource_arn, callback, sched_time, timeout_id, task_span = request
                    error = {
                        "errorType": "States.Timeout",
                        "errorMessage": "State or Execution ran for longer " +
                            "than the specified TimeoutSeconds value",
                    }
                    with opentracing.tracer.scope_manager.activate(
                        span=task_span,
                        finish_on_close=True
                    ) as scope:
                        error_callback(None, error, callback)

            """
            Start an OpenTracing trace for the rpcmessage request.
            https://opentracing.io/guides/python/tracers/ standard tags are from
            https://opentracing.io/specification/conventions/
            """
            execution_arn = context["Execution"]["Id"]
            with opentracing.tracer.start_active_span(
                operation_name="Task",
                child_of=span_context("text_map", context.get("Tracer", {}), self.logger),
                tags={
                    "component": "task_dispatcher",
                    "resource_arn": resource_arn,
                    "message_bus.destination": resource,
                    "span.kind": "producer",
                    "peer.address": self.peer_address,
                    "execution_arn": execution_arn
                },
                finish_on_close=False
            ) as scope:
                parameters_as_string = json.dumps(parameters)
                """
                We also pass the span to pending_requests so we can use
                it when receiving a response, and in case of a timeout.
                """
                carrier = inject_span("text_map", scope.span, self.logger)
                message = Message(
                    parameters_as_string,
                    properties=carrier,
                    content_type="application/json",
                    subject=resource,
                    reply_to=self.reply_to.name,
                    correlation_id=correlation_id,
                            # Give the RPC Message a TTL equivalent to the ASL
                            # Task State (or Execution) timeout period. Both are ms.
                    expiration=timeout,
                    mandatory=True,  # Ensure unroutable messages are returned
                )

                timeout_id = self.state_engine.event_dispatcher.set_timeout(
                    on_timeout, timeout
                )

                """
                The service response message is handled by handle_rpcmessage_response()
                If the response occurs before the timeout expires the timeout
                should be cancelled, so we store the timeout_id as well as the
                required callback in the dict keyed by correlation_id.
                As mentioned above we also store the OpenTracing span so that if a 
                timeout occurs we can raise an error on the request span.
                """
                self.pending_requests[correlation_id] = (
                    state_machine,
                    execution_arn,
                    resource_arn,
                    callback,
                    time.time() * 1000,
                    timeout_id,
                    scope.span
                )
                self.producer.send(message)

                self.state_engine.update_execution_history(
                    state_machine,
                    execution_arn,
                    "LambdaFunctionScheduled",
                    {
                        "input": parameters_as_string,
                        "resource": resource_arn,
                        "timeoutInSeconds": math.ceil(timeout/1000)
                    },
                )

                if self.task_metrics:
                    """
                    https://docs.aws.amazon.com/step-functions/latest/dg/procedure-cw-metrics.html
                    It's not totally clear what LambdaFunctionsScheduled and
                    LambdaFunctionsStarted *actually* mean from the AWS docs.
                    With the ASL Engine we are using an AMQP RPC pattern so
                    requests are queued and the ASL Engine can't really tell
                    whether the worker/processor has actually started, so it is
                    most truthful just to use the LambdaFunctionsScheduled
                    metric as we can't know LambdaFunctionsStarted.
                    """
                    self.task_metrics["LambdaFunctionsScheduled"].inc(
                        {"LambdaFunctionArn": resource_arn}
                    )


        # def asl_service_openfaas():  # TODO
        #    print("asl_service_openfaas")

        # def asl_service_fn():  # TODO
        #    print("asl_service_fn")

        # def asl_service_lambda():  # TODO
        #    print("asl_service_lambda")

        def asl_service_states():
            """
            The service part of the Resource ARN might be "states" for a number
            of Service Integrations, so we must further demultiplex based on
            the resource_type. Initially just support states startExecution to
            allow us to invoke another state machine execution.
            """
            if resource_type == "states" and resource == "startExecution":
                asl_service_states_startExecution()
            else:
                asl_service_InvalidService()

        def asl_service_states_startExecution():
            """
            Service Integration to stepfunctions. Initially this is limited to
            integrating with stepfunctions running on this local ASL Workflow
            Engine, however it should be possible to integrate with *real* AWS
            stepfunctions too in due course.

            Up until September 2019 real AWS stepfunctions didn't have a direct
            Service Integration to other stepfunctions and required a lambda to
            do this, so be aware that many online examples likely illustrate the
            use of a lambda as a proxy to the child stepfunction but this link
            documents the new more direct service integration:            
            https://docs.aws.amazon.com/step-functions/latest/dg/connect-stepfunctions.html

            The resource ARN should be of the form:
            arn:aws:states:region:account-id:states:startExecution

            The Task must have a Parameters field of the form:

            "Parameters": {
                "Input": "ChildStepFunctionInput",
                "StateMachineArn": "ChildStateMachineArn",
                "Name": "OptionalExecutionName"
            },

            if the execution Name is not specified in the Parameters a UUID will
            be assigned by the service.

            TODO: At the moment this integration only supports the direct
            request/response stepfunction integration NOT the "run a job" (.sync)
            or "wait for callback" (.waitForTaskToken) forms so at the moment
            we can't yet wait for the child stepfunction to complete or wait
            for a callback. To achieve this we *probably* need to poll
            ListExecutions with statusFilter="SUCCEEDED" comparing the response
            with our executionArn, which is mostly fairly straightforward but
            becomes more involved in a clustered environment. The callback
            integration might be slightly easier to implement as it shouldn't
            need any polling but one oddity is that there doesn't seem to be
            a stepfunctions integration for SendTaskSuccess (or SendTaskFailure
            or SendTaskHeartbeat) as those are the APIs that relate to
            triggering or cancelling a callback by implication there is no
            direct mechanism yet for a child stepfunction to actually do the
            callback other than proxying via a lambda. My guess is that this is
            an accidental omission that was missed when implementing the
            stepfunctions integration and will be added IDC.
            """
            child_execution_name = parameters.get("Name", str(uuid.uuid4()))

            child_state_machine_arn = parameters.get("StateMachineArn")
            if not child_state_machine_arn:
                message = "TaskDispatcher asl_service_states_startExecution: " \
                          "StateMachineArn must be specified"
                error = {"errorType": "MissingRequiredParameter", "errorMessage": message}
                error_callback(context.get("Tracer", {}), error, callback)
                return

            arn = parse_arn(child_state_machine_arn)
            region = arn.get("region", "local")
            child_state_machine_name = arn["resource"]

            execution_arn = context["Execution"]["Id"]

            child_execution_arn = create_arn(
                service="states",
                region=region,
                account=arn["account"],
                resource_type="execution",
                resource=child_state_machine_name + ":" + child_execution_name,
            )

            # Look up stateMachineArn to get child State Machine
            child_state_machine = self.state_engine.asl_store.get_cached_view(
                child_state_machine_arn
            )
            if not child_state_machine:
                message = "TaskDispatcher asl_service_states_startExecution: " \
                          "State Machine {} does not exist".format(
                    child_state_machine_arn
                )
                error = {"errorType": "StateMachineDoesNotExist", "errorMessage": message}
                error_callback(context.get("Tracer", {}), error, callback)
                return

            """
            Start an OpenTracing trace for the child StartExecution request.
            https://opentracing.io/guides/python/tracers/ standard tags are from
            https://opentracing.io/specification/conventions/
            """
            with opentracing.tracer.start_active_span(
                operation_name="Task",
                child_of=span_context("text_map", context.get("Tracer", {}), self.logger),
                tags={
                    "component": "task_dispatcher",
                    "resource_arn": resource_arn,
                    "execution_arn": execution_arn,
                    "child_execution_arn": child_execution_arn,
                }
            ) as scope:
                # Create the execution context and the event to publish to launch
                # the requested new state machine execution.
                # https://stackoverflow.com/questions/8556398/generate-rfc-3339-timestamp-in-python
                start_time = datetime.now(timezone.utc).astimezone().isoformat()
                child_context = {
                    "Tracer": inject_span("text_map", scope.span, self.logger),
                    "Execution": {
                        "Id": child_execution_arn,
                        "Input": parameters.get("Input", {}),
                        "Name": child_execution_name,
                        "RoleArn": child_state_machine.get("roleArn"),
                        "StartTime": start_time,
                    },
                    "State": {"EnteredTime": start_time, "Name": ""},  # Start state
                    "StateMachine": {
                        "Id": child_state_machine_arn,
                        "Name": child_state_machine_name
                    },
                }

                # Publish event that launches the new state machine execution.
                event = {"data": parameters.get("Input", {}), "context": child_context}
                self.state_engine.event_dispatcher.publish(
                    event, start_execution=True
                )

                parameters_as_string = json.dumps(parameters)
                self.state_engine.update_execution_history(
                    state_machine,
                    execution_arn,
                    "TaskScheduled",
                    {
                        "parameters": parameters_as_string,
                        "region": region,
                        "resource": resource_arn,
                        "resourceType": "execution",
                        "timeoutInSeconds": math.ceil(timeout/1000)
                    },
                )

                """
                The Stepfunction metrics specified in the docs:
                https://docs.aws.amazon.com/step-functions/latest/dg/procedure-cw-metrics.html
                describe Service Integration Metrics, which would include child
                Stepfunctions. The "dimension" for these, however is the
                resource ARN of the integrated service. In practice that is
                less than ideal as it only really describes the startExecution
                service so the ARN is the same irrespective of the state machine.
                """
                if self.task_metrics:
                    self.task_metrics["ServiceIntegrationsScheduled"].inc(
                        {"ServiceIntegrationResourceArn": resource_arn}
                    )


                result = {"executionArn": execution_arn, "startDate": time.time()}

                """
                For "fire and forget" launching should this be TaskSubmitted?
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_TaskSubmittedEventDetails.html
                """
                result_as_string = json.dumps(result)
                self.state_engine.update_execution_history(
                    state_machine,
                    execution_arn,
                    "TaskSucceeded",
                    {
                        "output": result_as_string,
                        "resource": resource_arn,
                        "resourceType": "execution",
                    },
                )

                """
                For now we only support "fire and forget" launching of child
                Stepfunctions and *not* synchronous ones that wait for the
                child Stepfunction to complete nor waitForTaskToken style
                callbacks. As it it fire and forget the ServiceIntegration
                is deemed successful if the request is successfully dispatched.
                TODO the other ServiceIntegration metrics are really only useful
                for synchronous (not fire and forget) execution invocations.
                """
                if self.task_metrics:
                    self.task_metrics["ServiceIntegrationsSucceeded"].inc(
                        {"ServiceIntegrationResourceArn": resource_arn}
                    )

                callback(result)

        def asl_service_InvalidService():
            message = "TaskDispatcher ARN {} refers to unsupported service".format(
                resource_arn
            )
            error = {"errorType": "InvalidService", "errorMessage": message}
            error_callback(context.get("Tracer", {}), error, callback)

        """
        Given the required service from the resource_arn dynamically invoke the
        appropriate service handler. The lambda provides a default handler.
        The "asl_service_" prefix mitigates the risk of the service value
        executing an arbitrary function, so disable semgrep warning.
        """
        # nosemgrep
        locals().get("asl_service_" + service, asl_service_InvalidService)()

