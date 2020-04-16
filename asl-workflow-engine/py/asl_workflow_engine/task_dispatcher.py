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


import json, os, time, uuid, opentracing
from datetime import datetime, timezone

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.open_tracing_factory import span_context, inject_span
from asl_workflow_engine.messaging_exceptions import *
from asl_workflow_engine.arn import *

class TaskDispatcher(object):
    def __init__(self, state_engine, config):
        """
        :param state_engine: The StateEngine calling this TaskDispatcher
        :type state_engine: StateEngine
        :param config: Configuration dictionary
        :type config: dict
        """
        self.logger = init_logging(log_name="asl_workflow_engine")
        self.logger.info("Creating TaskDispatcher")

        """
        Get the messaging peer.address, e.g. the Broker address for use
        by OpenTracing later.
        """
        peer_address = config.get("event_queue", {}).get("connection_url",
                                                         "amqp://localhost:5672")
        self.peer_address = peer_address.split("?")[0]

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
        
    def start(self, session):
        """
        Connect to the messaging fabric to enable Task States to integrate with
        "rpcmessage" based services as described in execute_task.
        """
        self.reply_to = session.consumer()
        # Enable consumer prefetch
        self.reply_to.capacity = 100
        self.reply_to.set_message_listener(self.handle_rpcmessage_response)
        self.producer = session.producer()

        # print(self.reply_to.name)
        # print(self.producer.name)

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
            correlation_id = message.correlation_id
            request = self.pending_requests.get(correlation_id)
            if request:
                try:
                    del self.pending_requests[correlation_id]
                    callback, timeout_id, carrier = request
                    # Cancel the timeout previously set for this request.
                    self.state_engine.event_dispatcher.clear_timeout(timeout_id)
                    if callable(callback):
                        result = json.loads(message.body.decode("utf8"))
                        error_type = result.get("errorType")
                        if error_type:
                            opentracing.tracer.active_span.set_tag("error", True)
                            opentracing.tracer.active_span.log_kv(
                                {
                                    "event": error_type,
                                    "message": result.get("errorMessage", ""),
                                }
                            )

                        callback(result)
                except ValueError as e:
                    self.logger.error(
                        "Response {} does not contain valid JSON".format(message.body)
                    )
            else:
                self.logger.info("Response {} has no matching requestor".format(message))

        message.acknowledge()

    def execute_task(self, resource_arn, parameters, callback, timeout, context):
        # TODO this import should be handled by the "Connection Factory for the
        # event queue" code in the constructor, but not sure yet how to do
        # this cleanly.
        from asl_workflow_engine.amqp_0_9_1_messaging import Message
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
        """

        def error_callback(carrier, error, callback):
            """
            A wrapper for for use when returning errors to the calling Task.
            The wrapper is mainly to provide OpenTracing boilerplate.
            We pass in the callback to be used by this function as it may be
            called from something asynchronous like a timeout, where the
            callback may have been stored in pending_requests rather than using
            the callback wrapped in the closure of execute_task.
            """
            with opentracing.tracer.start_active_span(
                operation_name="Task",
                child_of=span_context("text_map", carrier, self.logger),
            ) as scope:
                opentracing.tracer.active_span.set_tag("error", True)
                opentracing.tracer.active_span.log_kv(
                    {
                        "event": error["errorType"],
                        "message": error["errorMessage"],
                    }
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

            # Associate response callback with this request via correlation ID
            correlation_id = str(uuid.uuid4())

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
                    callback, timeout_id, carrier = request
                    error = {
                        "errorType": "States.Timeout",
                        "errorMessage": "State or Execution ran for longer " \
                            "than the specified TimeoutSeconds value",
                    }
                    error_callback(carrier, error, callback)

            """
            Start an OpenTracing trace for the rpcmessage request.
            https://opentracing.io/guides/python/tracers/ standard tags are from
            https://opentracing.io/specification/conventions/
            """
            with opentracing.tracer.start_active_span(
                operation_name="Task",
                child_of=span_context("text_map", context.get("Tracer", {}), self.logger),
                tags={
                    "component": "task_dispatcher",
                    "resource_arn": resource_arn,
                    "message_bus.destination": resource,
                    "span.kind": "producer",
                    "peer.address": self.peer_address,
                    "execution_arn": context["Execution"]["Id"]
                }
            ) as scope:
                """
                We also pass tracer carrier to pending_requests so we can use
                it in case of a timeout. For a normal response we use the
                tracer from the response message instead.
                """
                carrier = inject_span("text_map", scope.span, self.logger)
                message = Message(
                    json.dumps(parameters),
                    properties=carrier,
                    content_type="application/json",
                    subject=resource,
                    reply_to=self.reply_to.name,
                    correlation_id=correlation_id,
                )

                timeout_id = self.state_engine.event_dispatcher.set_timeout(
                    on_timeout, timeout
                )

                """
                The service response message is handled by handle_rpcmessage_response()
                If the response occurs before the timeout expires the timeout
                should be cancelled, so we store the timeout_id as well as the
                required callback in the dict keyed by correlation_id.
                As mentioned above we also store the serialised OpenTracing
                span context so that if a timeout occurs we can use the request
                span as the parent context.
                """
                self.pending_requests[correlation_id] = (callback, timeout_id, carrier)
                self.producer.send(message)

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
            execution_name = parameters.get("Name", str(uuid.uuid4()))

            state_machine_arn = parameters.get("StateMachineArn")
            if not state_machine_arn:
                message = "TaskDispatcher asl_service_states_startExecution: " \
                          "StateMachineArn must be specified"
                error = {"errorType": "MissingRequiredParameter", "errorMessage": message}
                error_callback(context.get("Tracer", {}), error, callback)
                return

            arn = parse_arn(state_machine_arn)
            state_machine_name = arn["resource"]

            execution_arn = create_arn(
                service="states",
                region=arn.get("region", "local"),
                account=arn["account"],
                resource_type="execution",
                resource=state_machine_name + ":" + execution_name,
            )

            # Look up stateMachineArn
            match = self.state_engine.asl_store.get(state_machine_arn)
            if not match:
                message = "TaskDispatcher asl_service_states_startExecution: " \
                          "State Machine {} does not exist".format(
                    state_machine_arn
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
                    "execution_arn": context["Execution"]["Id"],
                    "child_execution_arn": execution_arn,
                }
            ) as scope:
                # Create the execution context and the event to publish to launch
                # the requested new state machine execution.
                # https://stackoverflow.com/questions/8556398/generate-rfc-3339-timestamp-in-python
                start_time = datetime.now(timezone.utc).astimezone().isoformat()
                child_context = {
                    "Tracer": inject_span("text_map", scope.span, self.logger),
                    "Execution": {
                        "Id": execution_arn,
                        "Input": parameters.get("Input", {}),
                        "Name": execution_name,
                        "RoleArn": match.get("roleArn"),
                        "StartTime": start_time,
                    },
                    "State": {"EnteredTime": start_time, "Name": ""},  # Start state
                    "StateMachine": {"Id": state_machine_arn, "Name": state_machine_name},
                }

                event = {"data": parameters.get("Input", {}), "context": child_context}
                self.state_engine.event_dispatcher.publish(
                    event, start_execution=True
                )

                result = {"executionArn": execution_arn, "startDate": time.time()}
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
        """
        locals().get("asl_service_" + service, asl_service_InvalidService)()

