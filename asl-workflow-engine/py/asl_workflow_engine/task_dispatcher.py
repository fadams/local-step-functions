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
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import json, os, time, datetime, uuid
from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.messaging_exceptions import *
from asl_workflow_engine.arn import *

class TaskDispatcher(object):

    def __init__(self, state_engine, config):
        """
        :param logger: The Workflow Engine logger
        :type logger: logger
        :param config: Configuration dictionary
        :type config: dict
        """
        self.logger = init_logging(log_name='asl_workflow_engine')
        self.logger.info("Creating TaskDispatcher")
        #self.config = config["task_dispatcher"] # TODO Handle missing config
        # TODO validate that config contains the keys we need.
        self.state_engine = state_engine

        """
        Some services that we integrate Task States with might be request/
        response in the ASL sense, as in we would want to not move to the next
        state until the response has been received, but they might be async
        in implementation terms, such as the case for rpcmessage. This is
        conceptually RPC like so logically behaves like a Lambda call, but is
        implemented over a messaging fabric with queues.

        In order to deal with this we need to be able to associate requests
        with their subsequent responses, so this dictionary maps requests
        with their callbacks using correlation IDs.
        """
        self.unmatched_requests = {}

    def start(self, session):
        """
        Connect to the messaging fabric to enable Task States to integrate with
        "rpcmessage" based services as described in execute_task.
        """
        self.reply_to = session.consumer()
        self.reply_to.capacity = 100; # Enable consumer prefetch
        self.reply_to.set_message_listener(self.handle_rpcmessage_response)
        self.producer = session.producer()


        #print(self.reply_to.name)
        #print(self.producer.name)

    def handle_rpcmessage_response(self, message):
        """
        This is a message listener receiving messages from the reply_to queue
        for this workflow engine instance.
        TODO cater for the case where requests are sent but responses never
        arrive, this scenario will cause self.unmatched_requests to "leak" as
        correlation_id keys get added but not removed. This situation should be
        improved as we add code to handle Task state "rainy day" scenarios such
        as Timeouts etc. so park for now, but something to be aware of.
        """
        correlation_id = message.correlation_id
        requestor = self.unmatched_requests.get(correlation_id)
        if requestor:
            try:
                del self.unmatched_requests[correlation_id]
                if callable(requestor): requestor(json.loads(message.body.decode("utf8")))
            except ValueError as e:
                self.logger.error("Response {} does not contain valid JSON".format(message.body))
        else:
            self.logger.info("Response {} has no matching requestor".format(message))

        message.acknowledge()

    def execute_task(self, resource_arn, parameters, callback):
        # TODO this import should be handled by the "Connection Factory for the
        # event queue" code in the constructor.
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

        """
        If resource_arn starts with $ then attempt to look up its value from
        the environment, and if that fails return its original value.
        """
        if resource_arn.startswith("$"):
            resource_arn = os.environ.get(resource_arn[1:], resource_arn)
        # If resource_arn still starts with $ its value isn't in the environment
        if resource_arn.startswith("$"):
            self.logger.error("Specified Task Resource {} is not available on the environment".format(resource_arn))
            # TODO Handle error as per https://states-language.net/spec.html#errors

        arn = parse_arn(resource_arn)
        service = arn["service"] # e.g. rpcmessage, fn, openfaas, lambda
        resource_type = arn["resource_type"] # Should be function most times
        resource = arn["resource"] # function-name

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
            #print("asl_service_rpcmessage")
            #print(service)
            #print(resource_type)
            #print(resource)
            #print(parameters)
            # TODO deal with the case of delivering to a different broker.

            # Associate response callback with this request via correlation ID
            correlation_id = str(uuid.uuid4())
            self.unmatched_requests[correlation_id] = callback
            message = Message(json.dumps(parameters), content_type="application/json",
                              subject=resource, reply_to=self.reply_to.name,
                              correlation_id=correlation_id)
            #print(message)
            self.producer.send(message)
            # N.B. The service response message is handled by handle_rpcmessage_response()

        #def asl_service_openfaas():
        #    print("asl_service_openfaas")

        #def asl_service_fn():
        #    print("asl_service_fn")

        #def asl_service_lambda():
        #    print("asl_service_lambda")

        def asl_service_states():
            """
            The service part of the Resource ARN might be "states" for a number
            of Service Integrations, so we must further demultiplex based on
            the resource_type. Initially just support execution to allow us
            to invoke another state machine execution.
            """
            if resource_type == "execution": asl_service_states_execution()
            else: asl_service_InvalidService()

        def asl_service_states_execution():
            """
            Service Integration to stepfunctions. Initially this is limited to
            integrating with stepfunctions running on this local ASL Workflow
            Engine, however it should be possible to integrate with *real* AWS
            stepfunctions too in due course. Note that real AWS stepfunctions
            don't have a direct Service Integration to other stepfunctions and
            require a lambda to do this, but there is no fundamental reason why
            they couldn't (requiring an extra lambda of course generates more
            revenue so that might be the reason, but it might just be that they
            haven't got round to it.)

            The resource ARN should be of the form:
            arn:aws:states:region:account-id:execution:stateMachineName:executionName
            though note that for stepfunctions each execution should have a
            unique name, so if we name a resource like that in theory it could
            only execute once. In practice we don't *yet* handle all of the
            execution behaviour correctly, but we should eventually. The way to
            specify specific execution names (if desired) for launching via Task
            states is most likely via ASL Parameters. Using Parameters we could
            pass in the execution name in the stepfunction input and extract it
            in the Parameter's JSONPath processing.

            if the executionName is not specified a UUID will be assigned, which
            is more likely to be what is needed in practice.

            # TODO handle additional Task state parameters for stepfunctions
            integration. This will be needed to handle actual execution names
            because, as mentioned above, they should be unique and so would
            likely be passed as part of the calling stepfunction's input.

            Some more thought is needed about exactly what form any parameters
            should take, but something like the following is a starting point
            that is we'd need a way to specify the execution name and the
            stepfunction input data.

            "Parameters": {
                "ExecutionName.$": "$.name",
                "Input.$": "$.input"
            }
            """
            #print(parameters)

            print(resource.split(":"))
            split = resource.split(":")
            state_machine_name = split[0]
            execution_name = split[1] if len(split) == 2 else str(uuid.uuid4())

            state_machine_arn = create_arn(service="states",
                                           region=arn.get("region", "local"),
                                           account=arn["account"], 
                                           resource_type="stateMachine",
                                           resource=state_machine_name)

            execution_arn = create_arn(service="states",
                                       region=arn.get("region", "local"),
                                       account=arn["account"], 
                                       resource_type="execution",
                                       resource=state_machine_name + ":" +
                                                execution_name)

            # Look up stateMachineArn
            match = self.state_engine.asl_cache.get(state_machine_arn)
            if not match:
                self.logger.error("TaskDispatcher asl_service_states: State Machine {} does not exist".format(state_machine_arn))
                callback({"Error": "StateMachineDoesNotExist"})
                return

            # Create the execution context and the event to publish to launch
            # the requested new state machine execution.
            start_time = datetime.datetime.now().isoformat()
            context = {
                "Execution": {
                    "Id": execution_arn,
                    "Input": parameters,
                    "Name": execution_name,
                    "RoleArn": match.get("roleArn"),
                    "StartTime": start_time
                },
                "State": {
                    "EnteredTime": start_time,
                    "Name": "" # Start state
                },
                "StateMachine": {
                    "Id": state_machine_arn,
                    "Name": state_machine_name
                }
            }

            event = {
                "data": parameters,
                "context": context
            }

            self.state_engine.event_dispatcher.publish(event)

            result = {
                "executionArn": execution_arn,
                "startDate": time.time()
            }
            callback(result)

        def asl_service_InvalidService():
            self.logger.error("TaskDispatcher ARN {} refers to unsupported service: {}".
                              format(resource_arn, service))
            callback({"Error": "InvalidService"})

        """
        Given the required service from the resource_arn dynamically invoke the
        appropriate service handler. The lambda provides a default handler. 
        """
        locals().get("asl_service_" + service,
                     asl_service_InvalidService)()

