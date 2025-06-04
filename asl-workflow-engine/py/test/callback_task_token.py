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
# Run with:
# PYTHONPATH=.. python3 callback_task_token.py
# PYTHONPATH=.. LOG_LEVEL=DEBUG python3 callback_task_token.py
#
"""
This example is to illustrate the use of the Stepfunction Callback/TaskToken
https://docs.aws.amazon.com/step-functions/latest/dg/connect-to-resource.html#connect-wait-token
https://docs.aws.amazon.com/step-functions/latest/dg/connect-lambda.html

Note that using waitForTaskToken isn't available (as per AWS) with the "short
form" Lambda integration specifying a function ARN directly in the "Resource"
field, e.g.

"Resource": "arn:aws:lambda:us-east-1:123456789012:function:HelloFunction"

Instead the "long form" LambdaInvoke must be used, e.g.

"Resource": "arn:aws:states:::lambda:invoke.waitForTaskToken",
"Parameters": {  
  "FunctionName": "arn:aws:lambda:us-east-1:123456789012:function:HelloFunction",
  "Payload": {  
     "token.$": "$$.Task.Token"
  }
}

For the ASL Engine AMQP rpcmessage Lambda-like processor invocation that
would mean replacing a "short form" like:

"Resource": "arn:aws:rpcmessage:local::function:TaskTokenInvokeLambda",

"Resource": "arn:aws:states:local::rpcmessage:invoke.waitForTaskToken",
"Parameters": {  
  "FunctionName": "arn:aws:rpcmessage:local::function:TaskTokenInvokeLambda",
  "Payload": {  
     "token.$": "$$.Task.Token"
  }
}


Note that in the response from a "long form" LambdaInvoke the function output
is nested as a Payload field inside a dictionary of metadata as per:
https://docs.aws.amazon.com/step-functions/latest/dg/connect-lambda.html
However note also that if .waitForTaskToken is used the response from the
invoke is ignored and instead the Task Result is formed from the SendTaskSuccess
or SendTaskFailure
"""

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import asyncio, aioboto3, json, time, opentracing
from botocore.exceptions import ClientError

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.open_tracing_factory import create_tracer, span_context, inject_span, instrument_StartExecution
from asl_workflow_engine.amqp_0_9_1_messaging_asyncio import Connection, Message
from asl_workflow_engine.messaging_exceptions import *

ASL = """{
    "Comment": "This state machine demonstrates wait for callback functionality",
    "StartAt": "DirectInvoke",
    "States": {
        "DirectInvoke": {
            "Type": "Task",
            "Resource": "arn:aws:rpcmessage:local::function:BasicInvokeLambda",
            "TimeoutSeconds": 60,
            "Next": "BasicInvoke"
        },
        "BasicInvoke": {
            "Type": "Task",
            "Resource": "arn:aws:states:local::rpcmessage:invoke",
            "Parameters": {  
                "FunctionName": "arn:aws:rpcmessage:local::function:BasicInvokeLambda",
                "Payload.$": "$"
            },
            "TimeoutSeconds": 60,
            "OutputPath": "$.Payload",
            "Next": "TaskTokenInvoke"
        },
        "TaskTokenInvoke": {
            "Type": "Task",
            "Resource": "arn:aws:states:local::rpcmessage:invoke.waitForTaskToken",
            "Parameters": {  
                "FunctionName": "arn:aws:rpcmessage:local::function:TaskTokenInvokeLambda",
                "Payload": {
                    "input.$": "$",
                    "token.$": "$$.Task.Token"
                }
            },
            "TimeoutSeconds": 60,
            "Next": "StepFunctionLauncher"
        },
        "StepFunctionLauncher": {
            "Type": "Task",
            "Resource": "arn:aws:states:local::states:startExecution.waitForTaskToken",
            "Parameters": {  
                "StateMachineArn": "arn:aws:states:local:0123456789:stateMachine:callback_task_token_child",
                "Input": {
                    "input.$": "$",
                    "token.$": "$$.Task.Token"
                }
            },
            "TimeoutSeconds": 60,
            "End": true
        }
    }
}"""

"""
Simple child Stepfunction that calls a Processor which calls SendTaskSuccess.
Note that this is resolving the caller State Machine StepFunctionLauncher
Task and the ResolveCallerTaskToken Task State is itself a basic request/respone
"""
callee_ASL = """{
    "StartAt": "StartState",
    "States": {
        "StartState": {
            "Type": "Pass",
            "Next": "ResolveCallerTaskToken"
        },
        "ResolveCallerTaskToken": {
            "Type": "Task",
            "Resource": "arn:aws:rpcmessage:local::function:TaskTokenInvokeLambda",
            "TimeoutSeconds": 60,
            "End": true
        }
    }
}"""

items = ['{"lambda":"Success"}']

async def run_state_machine():
    # Initialise logger
    logger = init_logging(log_name='callback_task_token')

    # Initialise the aioboto3 client setting the endpoint_url to our local
    # ASL Workflow Engine
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    session = aioboto3.Session()
    client = session.client("stepfunctions", endpoint_url="http://localhost:4584")
    state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:callback_task_token"
    child_state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:callback_task_token_child"

    async def create_state_machines(sfn):
        # Create state machine using a dummy roleArn. If it already exists an
        # exception will be thrown, we ignore that but raise other exceptions.
        try:
            """
            The Log group ARN format is described here:
            https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/iam-access-control-overview-cwl.html
            See also:
            https://stackoverflow.com/questions/50273662/amazon-cloudwatch-how-to-find-arn-of-cloudwatch-log-group
            N.B. This is purely for illustration, destinations is a required
            field for logging levels other than OFF, but the contents of
            the destinations field is currently ignored by the ASL engine
            and only the level itself is actually used.
            """
            # Note await below as we're using aioboto3
            response = await sfn.create_state_machine(
                name="callback_task_token", definition=ASL,
                roleArn="arn:aws:iam::0123456789:role/service-role/MyRole",
                loggingConfiguration={
                    "level": "ALL",
                    "includeExecutionData": True,
                    "destinations": [
                        {"cloudWatchLogsLogGroup": {"logGroupArn": "arn:aws:logs:eu-west-2:0123456789:log-group:log_group_name"}}
                    ]
                }
            )

            # Note await below as we're using aioboto3
            response = await sfn.create_state_machine(
                name="callback_task_token_child", definition=callee_ASL,
                roleArn="arn:aws:iam::0123456789:role/service-role/MyRole",
                loggingConfiguration={
                    "level": "ALL",
                    "includeExecutionData": True,
                    "destinations": [
                        {"cloudWatchLogsLogGroup": {"logGroupArn": "arn:aws:logs:eu-west-2:0123456789:log-group:log_group_name"}}
                    ]
                }
            )
        except sfn.exceptions.StateMachineAlreadyExists as e:
            #print(e.response)

            # Note await below as we're using aioboto3
            response = await sfn.update_state_machine(
                stateMachineArn=state_machine_arn,
                definition=ASL,
                loggingConfiguration={
                    "level": "ALL",
                    "includeExecutionData": True,
                    "destinations": [
                        {"cloudWatchLogsLogGroup": {"logGroupArn": "arn:aws:logs:eu-west-2:0123456789:log-group:log_group_name"}}
                    ]
                }
            )

            # Note await below as we're using aioboto3
            response = await sfn.update_state_machine(
                stateMachineArn=child_state_machine_arn,
                definition=callee_ASL,
                loggingConfiguration={
                    "level": "ALL",
                    "includeExecutionData": True,
                    "destinations": [
                        {"cloudWatchLogsLogGroup": {"logGroupArn": "arn:aws:logs:eu-west-2:0123456789:log-group:log_group_name"}}
                    ]
                }
            )
        except ClientError as e:
            logger.error(e)
            sys.exit(1)


    async with client as sfn:  # In aioboto3 client and resource are context managers
        await create_state_machines(sfn)

        # Loop through items invoking a new state machine execution for each item
        for item in items:
            try:
                response = await sfn.start_execution(
                    stateMachineArn=state_machine_arn,
                    #name=EXECUTION_NAME, # If not specified a UUID is assigned
                    input=item
                )
            except sfn.exceptions.StateMachineDoesNotExist as e:
                logger.info(e)

                await create_state_machines(sfn)
                response = await sfn.start_execution(
                    stateMachineArn=state_machine_arn,
                    #name=EXECUTION_NAME, # If not specified a UUID is assigned
                    input=item
                )
            except ClientError as e:
                logger.error(e)


class Worker():
    def __init__(self, name):
        self.name = name
        # Initialise logger
        self.logger = init_logging(log_name=name)

    async def handler(self, message):
        print(self.name + " working")
        print(message)

        with opentracing.tracer.start_active_span(
            operation_name=self.name,
            child_of=span_context("text_map", message.properties, self.logger),
            tags={
                "component": "workers",
                "message_bus.destination": self.name,
                "span.kind": "consumer",
                "peer.address": "amqp://localhost:5672"
            }
        ) as scope:
            # Create simple reply. In a real processor **** DO WORK HERE ****
            if self.name == "TaskTokenInvokeLambda":
                # If we are a TaskTokenInvokeLambda call SendTaskSuccess
                request = json.loads(message.body.decode("utf8"))
                task_token = request.get("token")

                """
                Note that creating aioboto3 Session and Client instances are
                somewhat expensive operations and so creating them on a handler
                like a Lambda handler is sub-optimal and would likely be a bad
                idea in production code if the handler were likely to be called
                frequently. OTOH, unlike boto3, with aioboto3 the client object
                is a ContextManager so can be a pain if we want to save the
                sfn object to avoid creating on every call - we'd need a context
                stack as illustrated in the example here:
                https://aioboto3.readthedocs.io/en/latest/usage.html#aiohttp-server-example
                So for simplicity do it the more "obvious", but likely more
                expensive way.
                """
                session = aioboto3.Session()
                client = session.client("stepfunctions", endpoint_url="http://localhost:4584")
                async with client as sfn:  # In aioboto3 client and resource are context managers
                    print("sending Task Success")
                    try:
                        reply = {"reply": self.name + " SendTaskSuccess reply"}
                        await sfn.send_task_success(
                            output=json.dumps(reply),
                            taskToken=task_token
                        )
                        """
                        await sfn.send_task_failure(
                            error=self.name + "Error",  # Example Error Code
                            cause=self.name + " has failed to process",  # Example Error Message
                            taskToken=task_token
                        )
                        """
                    except sfn.exceptions.InvalidOutput as e:
                        print(e)
                    except ClientError as e:
                        print(e)
                    except Exception as e:
                        print(e)

            reply = {"reply": self.name + " reply"}

            """
            # Just used to test rpcmessage:invoke failed logs and metrics
            request = json.loads(message.body.decode("utf8"))
            if "reply" in request:
                reply = {
                    "errorType": self.name + "Error",  # Example Error Code
                    "errorMessage": self.name + " has failed to process"
                }
            """

            """
            Create the response message by reusing the request note that this
            approach retains the correlation_id, which is necessary. If a fresh
            Message instance is created we would need to get the correlation_id
            from the request Message and use that value in the response message.
            """

            """
            Start an OpenTracing trace for the rpcmessage response.
            https://opentracing.io/guides/python/tracers/ standard tags are from
            https://opentracing.io/specification/conventions/
            """
            with opentracing.tracer.start_active_span(
                operation_name=self.name,
                child_of=opentracing.tracer.active_span,
                tags={
                    "component": "workers",
                    "message_bus.destination": message.reply_to,
                    "span.kind": "producer",
                    "peer.address": "amqp://localhost:5672"
                }
            ) as scope:
                message.properties=inject_span("text_map", scope.span, self.logger)
                message.subject = message.reply_to
                message.reply_to = None
                message.body = json.dumps(reply)
                self.producer.send(message)
                message.acknowledge() # Acknowledges the original request

    async def run(self, barrier):
        # Connect to worker queue and process data.
        connection = Connection("amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=0")
        try:
            await connection.open()
            session = await connection.session()
            self.consumer = await session.consumer(
                self.name + '; {"node": {"auto-delete": true}}'
            )
            self.consumer.capacity = 100; # Enable consumer prefetch
            await self.consumer.set_message_listener(self.handler)
            self.producer = await session.producer()

            await barrier.wait()
            await connection.start(); # Wait until connection closes.
    
        except MessagingError as e:  # ConnectionError, SessionError etc.
            self.logger.error(e)
        except asyncio.CancelledError as e:
            pass

        connection.close();


async def wait_for_workers_then_run_state_machine(barrier):
    await barrier.wait()
    print("Workers have started")
    await run_state_machine()

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Initialising OpenTracing. It's important to do this before the boto3.client
    # call as create_tracer "patches" boto3 to add the OpenTracing hooks.
    create_tracer("callback_task_token", {"implementation": "Jaeger"}, use_asyncio=True)
    #create_tracer("callback_task_token", {"implementation": "Jaeger", "config": {"sampler": {"type": "probabilistic", "param": 0.01}}}, use_asyncio=True)
    #create_tracer("callback_task_token", {"implementation": "OpenTelemetry", "config": {"exporter": "jaeger-thrift"}}, use_asyncio=True)
    #create_tracer("callback_task_token", {"implementation": "OpenTelemetry", "config": {"exporter": "otlp-proto-grpc", "additional_propagators": "jaeger", "sampler": {"type": "probabilistic", "param": 0.01}}})

    workers = ["BasicInvokeLambda",
               "TaskTokenInvokeLambda",]

    # Create a Barrier to allow us to wait until all workers/"Lambdas" are
    # active before running the State Machine example that uses them.
    barrier = asyncio.Barrier(len(workers) + 1)

    # Create list of Worker tasks using list comprehension
    tasks = [loop.create_task(Worker(name=w).run(barrier)) for w in workers]

    # Add a Task to actually run the StateMachine. This Task uses the barrier
    # to wait until all the workers are running before running the StateMachine.
    tasks.append(loop.create_task(wait_for_workers_then_run_state_machine(barrier)))

    try:
        # Run the tasks concurrently
        loop.run_until_complete(asyncio.gather(*tasks))
    except KeyboardInterrupt as e:
        # Cancel tasks and run the loop again to wait for Tasks to cleanly exit.
        for t in tasks:
            t.cancel()
        loop.run_until_complete(asyncio.gather(*tasks))

    loop.close()

