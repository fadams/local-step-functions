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
                "ServiceIntegrationTime": Summary(
                    ns + "ServiceIntegrationTime",
                    "The interval, in milliseconds, between the time the " +
                    "Service Task is scheduled and the time it closes."
                ),
                "ServiceIntegrationsFailed": Counter(
                    ns + "ServiceIntegrationsFailed",
                    "The number of failed Service Tasks."
                ),
                "ServiceIntegrationsScheduled": Counter(
                    ns + "ServiceIntegrationsScheduled",
                    "The number of scheduled Service Tasks."
                ),
                "ServiceIntegrationsSucceeded": Counter(
                    ns + "ServiceIntegrationsSucceeded",
                    "The number of successfully completed Service Tasks."
                ),
                "ServiceIntegrationsTimedOut": Counter(
                    ns + "ServiceIntegrationsTimedOut",
                    "The number of Service Tasks that time out on close."
                )
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
            del self.pending_requests[correlation_id]
            state_machine, execution_arn, resource_arn, callback, sched_time, timeout_id, task_span = request
            with opentracing.tracer.scope_manager.activate(
                span=task_span,
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
                        try:
                            message_body_as_string = message_body.decode("utf8")
                            result = json.loads(message_body_as_string)
                        except ValueError as e:
                            error_message = ("Response {} does not contain "
                                "valid JSON").format(message.body)
                            result = {
                                "errorType": "States.Runtime",
                                "errorMessage": error_message
                            }
                            self.logger.error(error_message)

                    error_type = None
                    if isinstance(result, dict):
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
                            state_machine, execution_arn,
                            "LambdaFunctionFailed",
                            {
                                "error": error_type,
                                "cause": error_message,
                            },
                        )

                        if self.task_metrics: 
                            self.task_metrics["LambdaFunctionsFailed"].inc(
                                {"LambdaFunctionArn": resource_arn}
                            )
                    else:
                        self.state_engine.update_execution_history(
                            state_machine, execution_arn,
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

    def handle_sfn_response(self, correlation_id, input, output, execution_detail):
        """
        Called by state_engine.end_execution() to handle the responses (the
        output from ExecutionsSucceeded or ExecutionFailed) for any
        *synchronously* called child state machines.

        A pending_request will only be present for the case of synchronously
        executed child state machines, so the lookup of pending_requests might
        legitimately more often than not return None.
        """
        request = self.pending_requests.get(correlation_id)
        if request:
            del self.pending_requests[correlation_id]
            state_machine, execution_arn, resource_arn, callback, sched_time, timeout_id, task_span = request

            # Cancel the timeout previously set for this request.
            self.state_engine.event_dispatcher.clear_timeout(timeout_id)

            if not isinstance(execution_detail , dict):  # May be (non JSON) RedisDict
                execution_detail = dict(execution_detail)

            """
            In addition to using handle_sfn_response() to deal with responses
            for synchronously launched child state functions we also use it
            to handle the response to the StartSyncExecution API request. In
            that case the behaviour is slightly different and handled by the
            aws_api_StartSyncExecution method in RestAPI. We use the "fake"
            Resource ARN aws_api_StartSyncExecution set in that method to
            allow us to do a "short circuit" callback and return execution_detail.
            """
            if resource_arn == "aws_api_StartSyncExecution":
                callback(execution_detail)
                return

            with opentracing.tracer.scope_manager.activate(
                span=task_span,
                finish_on_close=True
            ) as scope:
                if callable(callback):
                    """
                    The child stepfunction Task response is largely the same as
                    https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeExecution.html
                    also known as the execution detail. One difference is that
                    for the API response the keys are lowerCamelCase whereas
                    for the internal stepfunction results the keys are Pascal
                    Case so we need to clone execution_detail and capitalize.
                    Also for the .sync:2 resource the Input and Output fields
                    are JSON not String.
                    """
                    result = {
                        k.capitalize(): v for k, v in execution_detail.items()
                    }
                    resource_type = "states"
                    if resource_arn.endswith(".sync:2"):
                        resource = "startExecution.sync:2"
                        result["Input"] = input
                        if "Output" in result:
                            result["Output"] = output
                    elif resource_arn.endswith(".sync"):
                        resource = "startExecution.sync"
                    else:
                        resource = "sfn:startSyncExecution"
                        resource_type = "aws-sdk"

                    error_type = result.get("Error")
                    result_as_string = json.dumps(result)
                    if error_type:
                        error_message = result.get("Cause")
                        opentracing.tracer.active_span.set_tag("error", True)
                        opentracing.tracer.active_span.log_kv(
                            {
                                "event": "States.TaskFailed",
                                "message": result_as_string,
                            }
                        )

                        self.state_engine.update_execution_history(
                            state_machine, execution_arn,
                            "TaskFailed",
                            {
                                "error": "States.TaskFailed",
                                "cause": result_as_string,
                                "resource": resource,
                                "resourceType": resource_type,
                            },
                        )

                        if self.task_metrics: 
                            self.task_metrics["ServiceIntegrationsFailed"].inc(
                                {"ServiceIntegrationResourceArn": resource_arn}
                            )
                    else:
                        self.state_engine.update_execution_history(
                            state_machine, execution_arn,
                            "TaskSucceeded",
                            {
                                "output": result_as_string,
                                "resource": resource,
                                "resourceType": resource_type,
                            },
                        )

                        if self.task_metrics:
                            self.task_metrics["ServiceIntegrationsSucceeded"].inc(
                                {"ServiceIntegrationResourceArn": resource_arn}
                            )

                    if self.task_metrics:
                        duration = (time.time()  * 1000.0) - sched_time
                        self.task_metrics["ServiceIntegrationTime"].observe(
                            {"ServiceIntegrationResourceArn": resource_arn}, duration
                        )

                    callback(result)

    def set_function_canceller(self, id, task_id, execution_arn):
        self.cancellers[id] = {
            "Type": "Function",
            "TaskID": task_id,
            "Execution": execution_arn,
            "Callback": None
        }
        #print("----- set_function_canceller " + str(id))
        #print(self.cancellers[id])

    def set_sfn_canceller(self, id, task_id, execution_arn):
        self.cancellers[id] = {
            "Type": "StepFunction",
            "TaskID": task_id,
            "Execution": execution_arn,
            "Callback": None
        }
        #print("----- set_sfn_canceller " + str(id))
        #print(self.cancellers[id])

    def set_timeout_canceller(self, id, task_id, callback, execution_arn):
        self.cancellers[id] = {
            "Type": "Timeout",
            "TaskID": task_id,
            "Execution": execution_arn,
            "Callback": callback
        }
        #print("----- set_timeout_canceller " + str(id))
        #print(self.cancellers[id])

    def remove_canceller(self, id):
        #print("remove_canceller " + str(id))
        if id in self.cancellers:
            del self.cancellers[id]

        #print(self.cancellers)

    def cancel_task(self, id):
        #print("cancel_task " + str(id))
        canceller = self.cancellers.get(id)
        if canceller:
            error = {
                "errorType": "Task.Terminated",
                "errorMessage": "Task has been Terminated",
            }

            task_type = canceller.get("Type")
            task_id = canceller.get("TaskID")
            del self.cancellers[id]
            if task_type == "Timeout":  # Wait state cancellation information
                #print("----- CANCELLING WAIT STATE -----")
                callback = canceller.get("Callback")

                # Cancel the timeout previously set for this request.
                self.state_engine.event_dispatcher.clear_timeout(task_id)
                if callable(callback):
                    callback(error)
            else:  # Function or Stepfunction
                #print("----- CANCELLING TASK STATE -----")
                # Do lookup in case timeout fires after a successful response.
                request = self.pending_requests.get(task_id)
                if request:  # Get callback and span from request (other fields ignored)
                    del self.pending_requests[task_id]
                    state_machine, execution_arn, resource_arn, callback, sched_time, timeout_id, task_span = request

                    with opentracing.tracer.scope_manager.activate(
                        span=task_span,
                        finish_on_close=True
                    ) as scope:
                        if callable(callback):
                            callback(error)

            """
            If the Task being cancelled is a child Stepfunction search the
            outstanding cancellers for any whose associated Execution ARN
            matches the Task ID (which is the ARN of the child Stepfunction
            launched by the Task for the case of Stepfunction Tasks).
            We store the IDs of any matching cancellers in a list using list
            comprehension then iterate that, cancelling the Task.
            Note that this search is a linear search, so less than ideal, but
            this block is only called for cancelled Stepfunctions and the
            number of items in self.cancellers should (on average) be modest
            as it only stores Tasks or Wait states currently awaiting results
            at any given point in time.
            """
            if task_type == "StepFunction":
                l = [k for k, v in self.cancellers.items() if v.get("Execution") == task_id]
                for child_id in l:
                    self.cancel_task(child_id)

        #print(self.cancellers)
        #print()
        #print(self.pending_requests.keys())

    def execute_task(self, resource_arn, parameters, callback, timeout, context, id):
        """
        Look up stateMachineArn to get State Machine as we need that later to
        call state_engine.update_execution_history. We do it here to capture in
        the execute_task closure, so it may be used in send_error_callback too.
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

        def send_error_callback(carrier, error):
            """
            A wrapper for use when returning errors to the calling Task.
            The wrapper is mainly to provide OpenTracing boilerplate.
            The carrier should be provided in the case where we don't have the
            original span relating to the request. In that case we create a new
            span to represent this error from the information provided in
            carrier, which should be an OpenTracing Carrier. If we do have the
            original span then carrier should be None, and the span should be 
            managed in the code calling this function.
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
                    update_type = "TaskTimedOut"
                    if service == "rpcmessage" or service == "lambda":
                        update_type = "LambdaFunctionTimedOut"
                    self.state_engine.update_execution_history(
                        state_machine, execution_arn,
                        update_type,
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
                        if service == "rpcmessage" or service == "lambda":
                            self.task_metrics["LambdaFunctionsTimedOut"].inc(
                                {"LambdaFunctionArn": resource_arn}
                            )
                        else:
                            self.task_metrics["ServiceIntegrationsTimedOut"].inc(
                                {"ServiceIntegrationResourceArn": resource_arn}
                            )

                if callable(callback):
                    callback(error)


        def timeout_callback(correlation_id):
            """
            If a Task timeout occurs when invoking rpcmessage or startExecution
            retrieve the original request and generate an error message passing
            that to send_error_callback. The actual timeout function is nested
            in asl_service_rpcmessage or asl_service_states_startExecution in
            order to capture the corelation id in a closure, it then delegates
            here as to allow both rpcmessage and startExecution to use it.
            """
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
                    send_error_callback(None, error)


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
            send_error_callback(context.get("Tracer", {}), error)
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

            """
            Create a timeout handler in case the rpcmessage invocation fails.
            The timeout_callback sends an error response to the calling Task
            state and deletes the pending request, using the correlation_id
            captured here by the on_timeout closure to lookup the request.
            """
            def on_timeout():
                timeout_callback(correlation_id)

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

                # Map the event ID for the Task to request ID.
                self.set_function_canceller(id, correlation_id, execution_arn)

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
                    state_machine, execution_arn,
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
            the resource_type. Initially just support states startExecution and
            its variants allow us to invoke another state machine execution.
            startExecution launches a child state machine in an asynchronous
            "fire and forget" manner and simply returns the executionArn and
            startDate immediately and the calling Task state doesn't wait for
            the child state machine to complete. On the other hand
            startExecution.sync, startExecution.sync:2 and for EXPRESS workflows
            aws-sdk:sfn:startSyncExecution all launch the child state machine
            synchronously, where the calling Task will wait for the child state
            machine to complete
            https://awsteele.com/blog/2021/10/12/nested-express-step-functions.html
            https://stackoverflow.com/a/69548324
            """
            if resource_type == "states":
                if (resource == "startExecution" or
                    resource == "startExecution.sync" or
                    resource == "startExecution.sync:2"):
                    asl_service_states_startExecution()
                else:
                    asl_service_InvalidService()
            elif resource_type == "aws-sdk":
                if resource == "sfn:startSyncExecution":
                    asl_service_states_startExecution()
                else:
                    asl_service_InvalidService()
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

            For asynchronous execution of child sate machines the resource ARN
            should be of the form:
            arn:aws:states:region:account-id:states:startExecution

            For synchronous execution of child state machines:
            arn:aws:states:region:account-id:states:startExecution.sync
            arn:aws:states:region:account-id:states:startExecution.sync:2

            For synchronous execution of EXPRESS child state machines:
            arn:aws:states:region:account-id:aws-sdk:sfn:startSyncExecution

            The Task must have a Parameters field of the form:

            "Parameters": {
                "Input": "ChildStepFunctionInput",
                "StateMachineArn": "ChildStateMachineArn",
                "Name": "OptionalExecutionName"
            },

            if the execution Name is not specified in the Parameters a UUID will
            be assigned by the service.
            """

            """
            EXPRESS workflows do not support job-run (.sync) or callback
            (.waitForTaskToken) service integration patterns.
            https://docs.aws.amazon.com/step-functions/latest/dg/concepts-standard-vs-express.html
            Note however that STANDARD workflows can use these to launch
            child EXPRESS workflows as illustrated in this AWS example:
            https://docs.aws.amazon.com/step-functions/latest/dg/sample-project-express-selective-checkpointing.html

            EXPRESS workflows do, however, support being invoked by the
            StartSyncExecution API call, and both STANDARD and EXPRESS workflows
            can launch EXPRESS child executions by using the awd-skd integration
            arn:aws:states:region:account-id:aws-sdk:sfn:startSyncExecution
            """
            if ((resource == "startExecution.sync" or
                 resource == "startExecution.sync:2") and
                state_machine.get("type") == "EXPRESS"):
                message = "TaskDispatcher asl_service_states_startExecution: " \
                          "Express state machine does not support '.sync' " \
                          "service integration"
                error = {"errorType": "InvalidResourceArn", "errorMessage": message}
                send_error_callback(context.get("Tracer", {}), error)
                return

            child_execution_name = parameters.get("Name", str(uuid.uuid4()))

            child_state_machine_arn = parameters.get("StateMachineArn")
            if not child_state_machine_arn:
                message = "TaskDispatcher asl_service_states_startExecution: " \
                          "StateMachineArn must be specified"
                error = {"errorType": "MissingRequiredParameter", "errorMessage": message}
                send_error_callback(context.get("Tracer", {}), error)
                return

            # Look up stateMachineArn to get child State Machine
            child_state_machine = self.state_engine.asl_store.get_cached_view(
                child_state_machine_arn
            )
            if child_state_machine:
                # sfn:startSyncExecution is only supported by EXPRESS workloads 
                if (resource == "sfn:startSyncExecution" and
                    child_state_machine.get("type") != "EXPRESS"):
                    message = "TaskDispatcher asl_service_states_startExecution: " \
                              "The sfn:startSyncExecution service integration " \
                              "is only supported by Express workflows"
                    error = {"errorType": "InvalidResourceArn", "errorMessage": message}
                    send_error_callback(context.get("Tracer", {}), error)
                    return
            else:  # Child state machine doesn't exist.
                message = "TaskDispatcher asl_service_states_startExecution: " \
                          "State Machine {} does not exist".format(
                    child_state_machine_arn
                )
                error = {"errorType": "StateMachineDoesNotExist", "errorMessage": message}
                send_error_callback(context.get("Tracer", {}), error)
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

            """
            Create a timeout handler in case a (synchronous) child state
            machine invocation fails. The timeout_callback sends an error
            response to the calling Task state and deletes the pending request,
            using the child_execution_arn captured here by the on_timeout
            closure to lookup the request.
            """
            def on_timeout():
                timeout_callback(child_execution_arn)

            """
            If the resource is simply startExecution then the child
            state machine is launched "asynchronously" where the parent
            execution simply carries on without waiting for the result.
            The other startExecution resources like startExecution.sync
            launch the execution "synchronously" where the Task state
            will not progress until the child execution completes.
            """
            async_child = True if resource == "startExecution" else False

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
                },
                finish_on_close=async_child
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

                """
                If the child state machine is launched synchronously we handle
                the success/failure response in handle_sfn_response().
                If the response occurs before the timeout expires the timeout
                should be cancelled, so we store the timeout_id as well as the
                required callback in the dict keyed by child_execution_arn.
                """
                if not async_child:  # E.g. it's launched synchronously
                    # Map the event ID for the Task to request ID.
                    self.set_sfn_canceller(id, child_execution_arn, execution_arn)

                    timeout_id = self.state_engine.event_dispatcher.set_timeout(
                        on_timeout, timeout
                    )

                    """
                    The service response message is handled by handle_sfn_response()
                    If the response occurs before the timeout expires the timeout
                    should be cancelled, so we store the timeout_id as well as the
                    required callback in the dict keyed by correlation_id.
                    As mentioned above we also store the OpenTracing span so that if a 
                    timeout occurs we can raise an error on the request span.
                    """
                    self.pending_requests[child_execution_arn] = (
                        state_machine,
                        execution_arn,
                        resource_arn,
                        callback,
                        time.time() * 1000,
                        timeout_id,
                        scope.span
                    )


                # Publish event that launches the new state machine execution.
                event = {"data": parameters.get("Input", {}), "context": child_context}
                self.state_engine.event_dispatcher.publish(
                    event, use_shared_queue=async_child
                )

                parameters_as_string = json.dumps(parameters)
                self.state_engine.update_execution_history(
                    state_machine, execution_arn,
                    "TaskScheduled",
                    {
                        "parameters": parameters_as_string,
                        "region": region,
                        "resource": resource_arn,
                        "resourceType": "states",
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

                """
                For "fire and forget"/async child executions we trigger the
                Task state on_response() handler immediately with the result.
                For synchronous child Stepfunction executions we return
                immediately after scheduling the invocation and the
                on_response() handler will be called at some point later when
                the handle_sfn_response() method eventually gets called when
                the child execution ends.
                """
                if async_child:
                    result = {"executionArn": execution_arn, "startDate": time.time()}

                    """
                    For "fire and forget" launching should this be TaskSubmitted?
                    https://docs.aws.amazon.com/step-functions/latest/apireference/API_TaskSubmittedEventDetails.html
                    """
                    result_as_string = json.dumps(result)
                    self.state_engine.update_execution_history(
                        state_machine, execution_arn,
                        "TaskSucceeded",
                        {
                            "output": result_as_string,
                            "resource": resource_arn,
                            "resourceType": "states",
                        },
                    )

                    """
                    For "fire and forget" launching of child Stepfunctions the      
                    ServiceIntegration is deemed successful if the request is
                    successfully dispatched.
                    """
                    if self.task_metrics:
                        self.task_metrics["ServiceIntegrationsSucceeded"].inc(
                            {"ServiceIntegrationResourceArn": resource_arn}
                        )

                    callback(result)  # Trigger Task state on_response() handler.


        def asl_service_InvalidService():
            message = "TaskDispatcher ARN {} refers to unsupported service".format(
                resource_arn
            )
            error = {"errorType": "InvalidService", "errorMessage": message}
            send_error_callback(context.get("Tracer", {}), error)

        """
        Given the required service from the resource_arn dynamically invoke the
        appropriate service handler. The lambda provides a default handler.
        The "asl_service_" prefix mitigates the risk of the service value
        executing an arbitrary function, so disable semgrep warning.
        """
        # nosemgrep
        locals().get("asl_service_" + service, asl_service_InvalidService)()

