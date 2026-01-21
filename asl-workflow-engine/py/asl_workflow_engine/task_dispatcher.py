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
MAX_DATA_LENGTH = 262144  # Max length of the input or output JSON string (256*1024).

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

        self.queue_type = queue_config.get("queue_type", "classic")

        # For quorum queues append -qq to the base name to visually distinguish.
        suffix = "-qq" if self.queue_type == "quorum" else ""

        instance_id = queue_config.get("instance_id", "")
        self.reply_to_queue_name = "asl_workflow_reply_to" + suffix + "-" + instance_id

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
        There are some scenarios whereby a response might arrive from say an
        rpcmessage request, but a pending_requests value for it may not be set.
        This may occur if the ASL Engine is restarted and the response to a
        previously made request then arrives, or where a Task is cancelled and
        the response from the invoked task subsequently arrives.

        We want to retain orphaned_responses for a short while as the Task
        Event message may be redelivered after a restart and we'd like to be
        able to associate the response with the Task.
        """
        self.handle_orphaned_responses_is_scheduled = False
        self.orphaned_responses = {}
        self.orphaned_response_retention_ms = queue_config.get(
            "orphaned_response_retention_ms"
        )
        self.startup_time = time.time()  # Used to determine uptime after restart

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
        x_declare = ""
        if self.queue_type == "quorum":
            x_declare = ', "x-declare": {"arguments": {"x-queue-type": "quorum"}}'

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
            self.reply_to_queue_name +
            '; {"node": {"durable": true' + x_declare + '}, ' + 
            '"link": {"x-subscribe": {"arguments": {"x-priority": 10}}}}'
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
        x_declare = ""
        if self.queue_type == "quorum":
            x_declare = ', "x-declare": {"arguments": {"x-queue-type": "quorum"}}'

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
            self.reply_to_queue_name +
            '; {"node": {"durable": true' + x_declare + '}, ' + 
            '"link": {"x-subscribe": {"arguments": {"x-priority": 10}}}}'
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

    def branch_has_terminated(self, execution_arn, branch_id):
        """
        This helper method looks up the Branch Metadata for the given
        execution ARN then finds the current Branch results for the given
        Branch ID and checks if the "terminated" field has been set.

        If Branch results are marked as terminated then all the response
        handlers like handle_rpcmessage_response, handle_sfn_response
        and timeout_callback will return a Task.Terminated error instead
        of their regular response.

        This test is more localised than branch_has_terminated in state_engine.
        Calling cancel_task also causes the callback to error with
        Task.Terminated, but this branch_has_terminated test also turns out to
        be necessary given the case where there are Map/Parallel with only
        Task States. In that case if all Tasks fail simultaneously, say with
        the same timeout, then each Task response will be a terminal state and
        needs to be checked for termination *before* the next State transition.
        """
        branch_metadata = self.state_engine.branch_metadata
        if branch_id and execution_arn in branch_metadata:
            # Get the dict containing all the branch results for this execution
            all_branch_results = branch_metadata[execution_arn].results
            branch_results = all_branch_results[branch_id]
            if branch_results.get("terminated"):
                return True
        return False

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
        error_message = f"Function not found: arn:aws:rpcmessage:local::function:{message.subject}"
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

        Its primary purpose is to handle rpcmessage response messages, but is
        also handles SendTaskSuccess/SendTaskFailure
        """

        def log_and_acknowledge_orphaned_responses():
            """
            When Tasks get cancelled or if the ASL Engine is restarted before a
            Task is completed then it is possible for the response Message to
            not have an associated pending_requests entry, e.g. be "orphaned".
            However, for the case of a restart the Task State Event messages
            are redelivered, so there is an edge case where a response from the
            invocation arrives before the redelivered Task State message. We
            therefore want to hold on to orphaned_responses for a while before
            logging and acknowledging in case they aren't in fact actually
            orphaned. To accomplish this log_and_acknowledge_orphaned_responses
            is called indirectly via a timeout and orphaned_responses is
            checked to see if there is still an entry for the correlation_id
            and thus still orphaned.
            Note that this nested function captures the message instance from
            the parent function in its closure and uses that captured value
            when the timeout is triggered.
            """
            if message.correlation_id in self.orphaned_responses:
                """
                The responses to invoke.waitForTaskToken rpcmessage requests
                are ignored as the *actual* Task response for those will be
                from SendTaskSuccess/SendTaskFailure. If we get "orphaned"
                responses from those the reason is likely to be some processing
                that calls SendTaskSuccess then sends an rpcmessage response
                so we don't need to log, just acknowledge.
                """
                if not message.correlation_id.endswith(".waitForTaskToken"):
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

                del self.orphaned_responses[message.correlation_id]
                message.acknowledge(multiple=False)


        correlation_id = message.correlation_id

        """
        Process the correlation_id to check if it had a ".invoke" or
        ".waitForTaskToken" suffix appended when it was stored. These are
        used to disambiguate, as the Task response handling for functions
        invoked using the "long form" invoke syntax is slightly different
        than that for the "short form" and TaskToken callbacks also behave
        slightly differently.
        https://docs.aws.amazon.com/step-functions/latest/dg/connect-lambda.html#connect-lambda-api-examples
        """
        request_has_invoke = False  # Set if "dictionary of metadata" response is required.
        request_has_waitForTaskToken = False
        is_callback = False  # E.g. a SendTaskSuccess/SendTaskFailure response.
        if correlation_id.endswith(".waitForTaskToken"):
            is_callback = (
                "x-SendTaskSuccess" in message.properties or
                "x-SendTaskFailure" in message.properties
            )

            """
            If the message is NOT a message generated by a SendTaskSuccess
            or SendTaskFailure callback API call, in other words if it is
            a response message directly from the rpcmessage call, then we
            set request_has_waitForTaskToken as we want to ignore any
            rpcmessage responses that aren't error responses.

            This logic is in place because a .invoke.waitForTaskToken
            request _could_ have *two* responses one being the rpcmessage
            response to the invoke and the other being the callback. Both
            appear very similar with the exception of the message property
            added to the callback message and they are handled similarly so
            this logic disambiguates them.
            """
            if not is_callback:
                request_has_waitForTaskToken = True
        elif correlation_id.endswith(".invoke"):
            request_has_invoke = True

        """
        If request_has_waitForTaskToken is set above any non-error rpcmessage
        response is ignored, as the *actual* Task completion is via
        SendTaskSuccess or SendTaskFailure API calls, however if the rpcmessage
        response is an error then that is *not* ignored and the Task will be
        failed, so we must check whether the response is an error before we do
        much else.
        """

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
                result_as_string = message_body.decode("utf8")
                result = json.loads(result_as_string)
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

        """
        If the request Resource was a .invoke.waitForTaskToken and the
        response *wasn't* an error then we acknowledge any response message
        and return without removing/resolving the pending_requests. This
        is because the *actual* Task response should come via
        SendTaskSuccess/SendTaskFailure which will be sent via its own
        message. In other words a .invoke.waitForTaskToken request *could*
        receive two responses, the first direct from the processor, which
        will be ignored unless it is an error, the second from the
        SendTaskSuccess/SendTaskFailure API calls.
        """
        if request_has_waitForTaskToken and error_type == None:
            message.acknowledge(multiple=False)
            return

        request = self.pending_requests.get(correlation_id)
        if request:
            """
            First remove any orphaned response for this correlation_id, as it is
            no longer orphaned if present in pending_requests. Note that we'll
            only have orphaned responses on edge cases where we have a restart
            and rpcmessage responses get delivered before their Task States get
            reconstructed from redelivered Task messages.
            """
            orphaned_response = self.orphaned_responses.get(correlation_id)
            if orphaned_response:
                m, timeout_id = orphaned_response
                # Cancel the timeout previously set for this orphaned_response.
                self.state_engine.event_dispatcher.clear_timeout(timeout_id)
                del self.orphaned_responses[correlation_id]

            del self.pending_requests[correlation_id]

            (
                state_machine,
                execution_arn,
                resource_arn,
                callback,
                branch_id,
                sched_time,
                timeout_id,
                task_span
            ) = request

            arn = parse_arn(resource_arn)
            resource = arn["resource"]
            resource_type = arn["resource_type"]

            with opentracing.tracer.scope_manager.activate(
                span=task_span,
                finish_on_close=True
            ) as scope:
                # Cancel the timeout previously set for this request.
                self.state_engine.event_dispatcher.clear_timeout(timeout_id)
                if callable(callback):
                    if self.branch_has_terminated(execution_arn, branch_id):
                        error_type = "Task.Terminated"
                        error_message = "Task has been Terminated"
                        result = {
                            "errorType": error_type,
                            "errorMessage": error_message,
                        }

                    if error_type:
                        error_message = result.get("errorMessage", "")
                        opentracing.tracer.active_span.set_tag("error", True)
                        opentracing.tracer.active_span.log_kv(
                            {
                                "event": error_type,
                                "message": error_message,
                            }
                        )

                        """
                        The arn:aws:states:::rpcmessage:invoke "long form"
                        resource and direct function ARN resource have different
                        log, history, and metric values. Note that the
                        "dimension" for ServiceIntegrationsFailed, is the
                        resource ARN of the integrated service for consistency
                        with AWS, which is arguably less useful than the
                        function ARN might be.
                        """
                        if error_type == "Task.Terminated":
                            pass  # Don't update history for Task.Terminated
                        elif (request_has_invoke or
                              is_callback or
                              request_has_waitForTaskToken):
                            self.state_engine.update_execution_history(
                                state_machine, execution_arn,
                                "TaskFailed",
                                {
                                    "error": error_type,
                                    "cause": error_message,
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
                        """
                        If invocation is from arn:aws:states:::rpcmessage:invoke
                        "long form" resource (rather than using function ARN
                        directly) when the Task result is returned the function
                        output is nested inside a dictionary of metadata. When
                        the function ARN is used directly the task result
                        contains only the function output.
                        https://docs.aws.amazon.com/step-functions/latest/dg/connect-lambda.html#connect-lambda-api-examples
                        """
                        if request_has_invoke:
                            result = {
                                "ExecutedVersion": "$LATEST",
                                "Payload": result,
                                "SdkResponseMetadata":{
                                    "RequestId": correlation_id
                                },
                                "StatusCode": 200
                            }
                            # Need to convert to string for update_execution_history
                            result_as_string = json.dumps(result)

                        """
                        The arn:aws:states:::rpcmessage:invoke "long form"
                        resource and direct function ARN resource have different
                        log, history, and metric values. Note that the
                        "dimension" for ServiceIntegrationsSucceeded, is the
                        resource ARN of the integrated service for consistency
                        with AWS, which is arguably less useful than the
                        function ARN might be.
                        """
                        if request_has_invoke or is_callback:
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
                        else:
                            self.state_engine.update_execution_history(
                                state_machine, execution_arn,
                                "LambdaFunctionSucceeded",
                                {
                                    "output": result_as_string,
                                },
                            )

                            if self.task_metrics:
                                self.task_metrics["LambdaFunctionsSucceeded"].inc(
                                    {"LambdaFunctionArn": resource_arn}
                                )

                    if self.task_metrics:
                        duration = (time.time()  * 1000.0) - sched_time

                        if request_has_invoke or is_callback:
                            self.task_metrics["ServiceIntegrationTime"].observe(
                                {"ServiceIntegrationResourceArn": resource_arn},
                                duration
                            )
                        else:
                            self.task_metrics["LambdaFunctionTime"].observe(
                                {"LambdaFunctionArn": resource_arn},
                                duration
                            )

                    callback(result)

            message.acknowledge(multiple=False)
        else:  # If Message correlation_id not in self.pending_requests
            uptime = (time.time() - self.startup_time)*1000  # Uptime in millis
            if uptime < self.orphaned_response_retention_ms:
                """
                Handle an edge case where a restart has occurred and *both*
                an errored rpcmessage response *and* a TaskToken response
                happen to have been published, which is likely rare, but
                plausible depending on how a processor handles such as case and
                the order of responses. In normal operation these will be
                handled in sequence and the pending request will be present,
                but on a restart both of these responses might arrive before
                the pending request has been reconstructed from the redelivered
                Task State. As these responses will have the *same* correlation
                ID we must check and select the errored rpcmessage response
                and dropping the TaskToken response.
                """
                if correlation_id in self.orphaned_responses:
                    if request_has_waitForTaskToken:
                        """
                        If current response is an rpcmessage error response
                        then replace the currently stored orphaned response,
                        remembering to acknowledge the original stored message
                        and reusing the original timeout_id.
                        """
                        m, timeout_id = self.orphaned_responses[correlation_id]
                        m.acknowledge(multiple=False)
                        self.orphaned_responses[correlation_id] = (message, timeout_id)
                    elif is_callback:
                        """
                        If the current response is a TaskToken callback it means
                        the currently stored orphaned response is an rpcmessage
                        error response, so retain that and acknowledge the
                        current message.
                        """
                        message.acknowledge(multiple=False)
                else:
                    """
                    Defer logging and acknowledging any "orphaned" response messages
                    as upon a restart of the ASL Engine it is possible for Task
                    responses from the original Task invocation to arrive before the
                    pending Task's state is reconstructed.
                    """
                    timeout_id = self.state_engine.event_dispatcher.set_timeout(
                        log_and_acknowledge_orphaned_responses,
                        self.orphaned_response_retention_ms
                    )
                    self.orphaned_responses[correlation_id] = (message, timeout_id)
            else:
                """
                If the uptime is more than the retention period for orphaned
                responses directly call log_and_acknowledge_orphaned_responses()
                """
                self.orphaned_responses[correlation_id] = (message, None)
                log_and_acknowledge_orphaned_responses()


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

            (
                state_machine,
                execution_arn,
                resource_arn,
                callback,
                branch_id,
                sched_time,
                timeout_id,
                task_span
            ) = request

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

                    if self.branch_has_terminated(execution_arn, branch_id):
                        error_type = "Task.Terminated"
                        error_message = "Task has been Terminated"
                        result = {
                            "errorType": error_type,
                            "errorMessage": error_message,
                        }
                    else:
                        error_type = result.get("Error")
                        error_message = result.get("Cause")

                    result_as_string = json.dumps(result)
                    if error_type:
                        opentracing.tracer.active_span.set_tag("error", True)
                        opentracing.tracer.active_span.log_kv(
                            {
                                "event": "States.TaskFailed",
                                "message": result_as_string,
                            }
                        )

                        if error_type != "Task.Terminated":
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

    def set_function_canceller(self, event_id, task_id, execution_arn):
        self.cancellers[event_id] = {
            "Type": "Function",
            "TaskID": task_id,
            "Execution": execution_arn,
            "Callback": None
        }
        #print("----- set_function_canceller " + str(event_id))
        #print(self.cancellers[event_id])

    def set_sfn_canceller(self, event_id, task_id, execution_arn):
        self.cancellers[event_id] = {
            "Type": "StepFunction",
            "TaskID": task_id,
            "Execution": execution_arn,
            "Callback": None
        }
        #print("----- set_sfn_canceller " + str(event_id))
        #print(self.cancellers[event_id])

    def set_timeout_canceller(self, event_id, task_id, callback, execution_arn):
        self.cancellers[event_id] = {
            "Type": "Timeout",
            "TaskID": task_id,
            "Execution": execution_arn,
            "Callback": callback
        }
        #print("----- set_timeout_canceller " + str(event_id))
        #print(self.cancellers[event_id])

    def remove_canceller(self, event_id):
        #print("remove_canceller " + str(event_id))
        if event_id in self.cancellers:
            del self.cancellers[event_id]

        #print(self.cancellers)

    def cancel_task(self, event_id):
        #print("cancel_task " + str(event_id))
        canceller = self.cancellers.get(event_id)
        if canceller:
            error = {
                "errorType": "Task.Terminated",
                "errorMessage": "Task has been Terminated",
            }

            task_type = canceller.get("Type")
            task_id = canceller.get("TaskID")
            del self.cancellers[event_id]
            if task_type == "Timeout":  # Wait state cancellation information
                #print("----- CANCELLING WAIT STATE -----")
                callback = canceller.get("Callback")

                # Cancel the timeout previously set for this request.
                self.state_engine.event_dispatcher.clear_timeout(task_id)
                if callable(callback):
                    callback(error)
            else:  # Function or Stepfunction
                #print("----- CANCELLING TASK STATE -----")
                # Do lookup in case cancel occurs after a successful response.
                request = self.pending_requests.get(task_id)
                if request:  # Get callback and span from request (other fields ignored)
                    del self.pending_requests[task_id]

                    (
                        state_machine,  # ignored
                        execution_arn,  # ignored
                        resource_arn,   # ignored
                        callback,
                        branch_id,      # ignored
                        sched_time,     # ignored
                        timeout_id,     # ignored
                        task_span
                    ) = request

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

    def handle_orphaned_responses(self):
        """
        Orphaned responses may occur if the ASL Engine is restarted and the
        response to a previously made request then arrives, or where a Task is
        cancelled and the response from the invoked task subsequently arrives.
        These could be "resolved" when the message representing the Task State
        gets redelivered, but the delivery order of the response and the
        redelivered Task is indeterminate.

        To cater for this edge case we schedule a periodic check of Orphaned
        responses, initially triggered by State Transition Events but will also
        re-schedule itself if any orphaned_responses remain.

        Note the use of copy() as we might delete items whilst iterating
        in general orphaned_responses should be small though and only non-zero
        for a short period after start-up.
        """
        for correlation_id, orphaned_response in self.orphaned_responses.copy().items():
            message, timeout_id = orphaned_response
            if correlation_id in self.pending_requests:
                # Directly call response handler with the stored message.
                self.handle_rpcmessage_response(message)

        # Reset flag to allow handle_orphaned_responses to be scheduled again.
        self.handle_orphaned_responses_is_scheduled = False
        self.schedule_orphaned_response_handler()

    def schedule_orphaned_response_handler(self):
        """
        Schedule handle_orphaned_responses onto the event loop if any
        orphaned_responses remain and it is not already scheduled to run.
        """
        if (len(self.orphaned_responses) == 0 or
            self.handle_orphaned_responses_is_scheduled):
            return

        self.handle_orphaned_responses_is_scheduled = True
        self.state_engine.event_dispatcher.set_timeout(
            self.handle_orphaned_responses, 1000
        )

    def execute_task(self, resource_arn, parameters, callback, timeout, is_task_timeout, context, event_id, redelivered):
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

            This method must handle two distinct tracing scenarios:

            1. If `carrier` is provided, we DO NOT have an existing active span.
               In this case we create a new active span here, and therefore
               this method OWNS the span scope lifecycle and MUST close it.

            2. If `carrier` is None, an active span is already in scope and is
               owned by the caller. In this case this method MUST NOT close the
               scope, as doing so would detach the OpenTelemetry context twice.

            Important OpenTelemetry / OpenTracing shim gotcha:
            --------------------------------------------------
            OpenTelemetry enforces strict context attach/detach semantics.
            A scope may only be closed once. Calling `with scope:` or
            `scope.close()` on a scope obtained via `scope_manager.active` will
            "double close" the underlying context token and raise a runtime
            error something like:

            ERROR    - opentelemetry.context : Failed to detach context
            Traceback (most recent call last):
              File ".../opentelemetry/context/__init__.py", line 155, in detach
            _RUNTIME_CONTEXT.detach(token)
              File ".../opentelemetry/context/contextvars_context.py", line 53,
                in detach self._current_context.reset(token)
            RuntimeError: <Token used var=<ContextVar name='current_context'
                default={} at 0x7f69b43299e0> at 0x7f69b1d419c0> has already
            been used once

            To safely support both cases, this method:
             - only closes a scope when it explicitly created it
             - never enters or closes an already-active scope
            """
            scope = None
            if carrier:
                scope = opentracing.tracer.start_active_span(
                    operation_name="Task",
                    child_of=span_context("text_map", carrier, self.logger),
                )

            try:
                opentracing.tracer.active_span.set_tag("error", True)
                opentracing.tracer.active_span.log_kv(
                    {
                        "event": error["errorType"],
                        "message": error["errorMessage"],
                    }
                )

                # Only update history and metrics if a task not execution timeout.
                if is_task_timeout and error["errorType"] == "States.Timeout":
                    execution_arn = context["Execution"]["Id"]
                    update_type = "TaskTimedOut"
                    if resource_type == "function":
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
            finally:
                if scope:
                    scope.close()


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

                (
                    state_machine,  # ignored
                    execution_arn,
                    resource_arn,   # ignored
                    callback,       # ignored
                    branch_id,
                    sched_time,     # ignored
                    timeout_id,     # ignored
                    task_span
                ) = request

                if self.branch_has_terminated(execution_arn, branch_id):
                    error = {
                        "errorType": "Task.Terminated",
                        "errorMessage": "Task has been Terminated",
                    }
                else:
                    state_timeout = int(timeout/1000 + 0.5)
                    error = {
                        "errorType": "States.Timeout",
                        "errorMessage": "State ran for longer than " +
                            "the specified timeout value of " +
                            f"{state_timeout} seconds.",
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
        region = arn["region"]  # e.g. real AWS region or "local" for ASL Engine
        resource_type = arn["resource_type"]  # Should be function most times
        resource = arn["resource"]  # function-name

        """
        If the request comes from a Task State in a Branch or Iterator get
        the Branch ID. We use it to check if the Branch or Iterator has been
        terminated as a result of a Parallel or Map State failure and if so
        we sent a Task.Terminated error message as the Task response.
        """
        branch_id = None
        context_state = context["State"]
        if "Branch" in context_state:
            # Get ID of the item at the top of the Branch metadata stack
            branch_id = context_state["Branch"][-1]["ID"]
 

        """
        Define nested functions as handlers for each supported service type.

        That the methods are prefixed with "asl_service_" is a mitigation against
        accidentally or deliberately placing an unsupported service type in the ARN.
        """
        def asl_service_rpcmessage():
            """
            Publish message to the required rpcmessage worker resource. The
            message body is a JSON string containing the "effective parameters"
            passed from the StateEngine and the message subject is the function
            name, which in turn maps to the name of the queue that the resource
            has set up to listen on. In order to ensure that responses from
            Task resources return to the correct calling Task the message is
            populated with a reply to address representing this workflow
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

            """
            Associate response callback with this request via a correlation ID.
            We now use event_id as the correlation ID, as that is now a UUID
            attached to the Event Messages, which represent State transitions.
            Because they are associated with the Messages these IDs survive
            ASL Engine restarts, so Task responses can be correlated with the
            requesting Task State even after a restart.
            """
            correlation_id = event_id

            """
            There are two alternative syntaxes for invoking functions as per:
            https://docs.aws.amazon.com/step-functions/latest/dg/connect-lambda.html
            The first has a "Resource" like "arn:aws:states:::rpcmessage:invoke"
            and Parameters specifying FunctionName and Payload. In the second
            form you can invoke a function by simply specifying a function ARN
            directly in the "Resource" field, which has been the only form
            hitherto supported by the ASL Engine.

            We need to collect the same function invocation information from
            each. However, as the response format is different we need to be
            able to disambiguate response format based on invocation approach
            so we append the resource as a suffix to the correlation_id.
            """
            if resource_type == "rpcmessage" and (
                resource == "invoke" or resource == "invoke.waitForTaskToken"):
                # Called using "long form" invocation syntax e.g.
                # e.g. "Resource": "arn:aws:states:local::rpcmessage:invoke",
                function_arn = parameters.get("FunctionName")
                if function_arn:
                    # Add suffix to correlation_id to allow disambiguation
                    suffix = ".invoke" if resource == "invoke" else ".waitForTaskToken"
                    correlation_id = correlation_id + suffix
                    arn = parse_arn(function_arn)
                    function_name = arn["resource"]

                    # The *actual* function arguments are held in the Payload
                    # field (if present).
                    function_payload = parameters.get("Payload", {})
                else:
                    message = f"TaskDispatcher ARN {resource_arn}: " \
                               "Parameters.FunctionName must be specified"
                    error = {"errorType": "MissingRequiredParameter",
                             "errorMessage": message}
                    send_error_callback(context.get("Tracer", {}), error)
                    return
            else:
                # Called using "short form" invocation syntax e.g.
                # "Resource": "arn:aws:rpcmessage:local::function:FunctionName",
                function_arn = resource_arn
                function_name = resource
                function_payload = parameters

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
                    "function_arn": function_arn,
                    "message_bus.destination": function_name,
                    "span.kind": "producer",
                    "peer.address": self.peer_address,
                    "execution_arn": execution_arn
                },
                finish_on_close=False
            ) as scope:
                # Map the event ID for the Task to request ID.
                self.set_function_canceller(event_id, correlation_id, execution_arn)

                timeout_id = self.state_engine.event_dispatcher.set_timeout(
                    on_timeout, timeout
                )

                """
                The service response message is handled by handle_rpcmessage_response()
                If the response occurs before the timeout expires the timeout
                should be cancelled, so we store the timeout_id as well as the
                required callback in the dict keyed by correlation_id.
                We also pass the OpenTracing span to pending_requests so we can
                use it when receiving a response, and in case of a timeout.
                """
                self.pending_requests[correlation_id] = (
                    state_machine,
                    execution_arn,
                    resource_arn,
                    callback,
                    branch_id,
                    time.time() * 1000,
                    timeout_id,
                    scope.span
                )

                """
                Actually invoke the Task by creating and publishing the request
                Message and updating the Execution History and metrics.

                Note that if the Task State is re-entered, for example due to
                the ASL Engine being restarted and the outstanding Task State
                transition Message being redelivered then we do not re-invoke
                the Task.

                The original behaviour was to re-invoke as it is simpler, but
                for expensive or long lived Tasks it is sub-optimal to invoke
                them again, so we now try to avoid it which adds complexity.
                """
                if not redelivered:
                    payload_as_string = json.dumps(function_payload)
                    carrier = inject_span("text_map", scope.span, self.logger)
                    message = Message(
                        payload_as_string,
                        properties=carrier,
                        content_type="application/json",
                        subject=function_name,
                        reply_to=self.reply_to.name,
                        correlation_id=correlation_id,
                        # Give the RPC Message a TTL equivalent to the ASL
                        # Task State (or Execution) timeout period. Both are ms.
                        expiration=timeout,
                        mandatory=True,  # Ensure unroutable messages are returned
                    )

                    self.producer.send(message)

                    """
                    The type of history/log event differs by resource type.
                    If the resource is a Function ARN then the event is
                    LambdaFunctionScheduled, but if it is a generic invoke
                    then the event is TaskScheduled for consistency with
                    AWS Stepfunctions.
                    """
                    if resource_type == "function":
                        self.state_engine.update_execution_history(
                            state_machine, execution_arn,
                            "LambdaFunctionScheduled",
                            {
                                "input": payload_as_string,
                                "resource": function_arn,
                                "timeoutInSeconds": math.ceil(timeout/1000)
                            },
                        )
                    else:
                        parameters_as_string = json.dumps(parameters)
                        self.state_engine.update_execution_history(
                            state_machine, execution_arn,
                            "TaskScheduled",
                            {
                                "parameters": parameters_as_string,
                                "region": region,
                                "resource": resource,
                                "resourceType": resource_type,
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

                        The type of metric differs by resource type.
                        If the resource is a Function ARN then the metric is
                        LambdaFunctionsScheduled, but if it is a generic invoke
                        then the metric is ServiceIntegrationsScheduled for
                        consistency with AWS Stepfunctions.

                        The "dimension" for ServiceIntegrationsScheduled, however
                        is the resource ARN of the integrated service. In
                        practice that is less than ideal as it only really
                        describes the invoke service, so the ARN is the
                        same irrespective of the invoked function. It's likely
                        more useful to have the dimention be the function ARN
                        as with the "short-form" invoke, but the resource ARN
                        is consistent with AWS Stepfunctions.
                        """
                        if resource_type == "function":
                            self.task_metrics["LambdaFunctionsScheduled"].inc(
                                {"LambdaFunctionArn": function_arn}
                            )
                        else:
                            self.task_metrics["ServiceIntegrationsScheduled"].inc(
                                {"ServiceIntegrationResourceArn": resource_arn}
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
            the resource_type.

            Initially just support rpcmessage:invoke, states:startExecution and
            its variants to allow us to invoke another state machine execution.
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

            """
            TODO IDC check for region == "local" for resource_type == "states"
            or resource_type == "aws-sdk" below. These do StartExecution on
            the ASL Engine by using the internal messaging, but when support
            for invoking real AWS services is added we need to disambiguate
            between "local" invocations vice AWS service invocations.
            That would be a potentially breaking change as it would require
            local Resource ARNs to *explicitly* begin arn:aws:states:local:
            whereas for now arn:aws:states:: would also work.
            """
            if resource_type == "states":
                if (resource == "startExecution" or
                    resource == "startExecution.sync" or
                    resource == "startExecution.sync:2" or
                    resource == "startExecution.waitForTaskToken"):
                    asl_service_states_startExecution()
                else:
                    asl_service_InvalidService()
            elif resource_type == "aws-sdk":
                if resource == "sfn:startSyncExecution":
                    asl_service_states_startExecution()
                else:
                    asl_service_InvalidService()
            elif resource_type == "rpcmessage":
                if (resource == "invoke" or
                    resource == "invoke.waitForTaskToken"):
                    asl_service_rpcmessage()
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

            For asynchronous execution of child state machines the resource ARN
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
            can launch EXPRESS child executions by using the aws-sdk integration
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

            #child_execution_name = parameters.get("Name", str(uuid.uuid4())) # Deprecated
            child_execution_name = parameters.get("Name", event_id)
            child_execution_arn = create_arn(
                service="states",
                region=region,
                account=arn["account"],
                resource_type="execution",
                resource=child_state_machine_name + ":" + child_execution_name,
            )

            """
            Associate response callback with this request via a correlation ID.
            For startExecution.sync, startExecution.sync:2, and
            sfn:startSyncExecution we use child_execution_arn as the
            correlation ID because the child execution knows its own ARN so
            can easily signal its completion using that. For 
            startExecution.waitForTaskToken we use event_id plus the suffix
            ".waitForTaskToken" so we can reuse the rpcmessage callback handler.
            """
            if resource == "startExecution.waitForTaskToken":
                correlation_id = event_id + ".waitForTaskToken"
            else:
                correlation_id = child_execution_arn

            """
            Create a timeout handler in case a (synchronous) child state
            machine invocation fails. The timeout_callback sends an error
            response to the calling Task state and deletes the pending request,
            using the correlation_id captured here by the on_timeout
            closure to lookup the request.
            """
            def on_timeout():
                timeout_callback(correlation_id)

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
            execution_arn = context["Execution"]["Id"]
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
                """
                If the child state machine is launched synchronously we handle
                the success/failure response in handle_sfn_response().
                """
                if not async_child:  # E.g. it's launched synchronously
                    # Map the event ID for the Task to request ID.
                    self.set_sfn_canceller(event_id, correlation_id, execution_arn)

                    timeout_id = self.state_engine.event_dispatcher.set_timeout(
                        on_timeout, timeout
                    )

                    """
                    The service response message is handled by handle_sfn_response()
                    If the response occurs before the timeout expires the timeout
                    should be cancelled, so we store the timeout_id as well as the
                    required callback in the dict keyed by correlation_id.
                    We also pass the OpenTracing span to pending_requests so we can
                    use it when receiving a response, and in case of a timeout.
                    """
                    self.pending_requests[correlation_id] = (
                        state_machine,
                        execution_arn,
                        resource_arn,
                        callback,
                        branch_id,
                        time.time() * 1000,
                        timeout_id,
                        scope.span
                    )

                """
                Actually invoke the Task by creating and publishing the sfn
                event Message and updating the Execution History and metrics.

                Note that if the Task State is re-entered, for example due to
                the ASL Engine being restarted and the outstanding Task State
                transition Message being redelivered then we do not re-invoke
                the Task.

                The original behaviour was to re-invoke as it is simpler, but
                for expensive or long lived Tasks it is sub-optimal to invoke
                them again, so we now try to avoid it which adds complexity.
                """
                if not redelivered:
                    """
                    Create the execution context and the event to publish to launch
                    the requested new state machine execution.
                    """
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
                        event, use_shared_queue=async_child
                    )

                    parameters_as_string = json.dumps(parameters)
                    self.state_engine.update_execution_history(
                        state_machine, execution_arn,
                        "TaskScheduled",
                        {
                            "parameters": parameters_as_string,
                            "region": region,
                            "resource": resource,
                            "resourceType": resource_type,
                            "timeoutInSeconds": math.ceil(timeout/1000)
                        },
                    )

                    if self.task_metrics:
                        """
                        The Stepfunction metrics specified in the docs:
                        https://docs.aws.amazon.com/step-functions/latest/dg/procedure-cw-metrics.html
                        describe Service Integration Metrics, which would include
                        child Stepfunctions. The "dimension" for these, however
                        is the resource ARN of the integrated service. In
                        practice that is less than ideal as it only really
                        describes the startExecution service so the ARN is the
                        same irrespective of the state machine.
                        """
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
                    result = {"executionArn": child_execution_arn, "startDate": time.time()}

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
                            "resource": resource,
                            "resourceType": resource_type,
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

