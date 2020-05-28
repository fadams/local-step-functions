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
# PYTHONPATH=.. python3 step_by_step.py
#
"""
This test illustrates one step function invoking another.

This is a relatively recent AWS addition (September 2019) described here:
https://docs.aws.amazon.com/step-functions/latest/dg/connect-stepfunctions.html
https://docs.aws.amazon.com/step-functions/latest/dg/concepts-nested-workflows.html

Because it's relatively new be aware that a lot of the information online
relating to nesting stepfunctions illustrates the use of a lambda as a proxy
for example this AWS tutorial:
https://docs.aws.amazon.com/step-functions/latest/dg/tutorial-continue-new.html
Using a lambda to proxy child stepfunctions still works and is still a
legitimate approach, but it is no longer neccesary and adds an additional cost.


This test uses the AWS python SDK boto3 to access our local ASL Workflow Engine
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html
https://docs.aws.amazon.com/code-samples/latest/catalog/code-catalog-python-example_code-stepfunctions.html
"""

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import boto3, time
from botocore.exceptions import ClientError

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.open_tracing_factory import create_tracer

"""
In the Resource ARN below the execution Name is not specified, so a UUID will
be automatically assigned. The way to specify specific execution names (if so
desired) is to pass the execution name in the stepfunction input and extract it
in the Parameter's JSONPath processing e.g. something like:
"Name.$": "$.executionName"
"""
caller_ASL = """{
    "StartAt": "StepFunctionLauncher",
    "States": {
        "StepFunctionLauncher": {
            "Type": "Task",
            "Resource": "arn:aws:states:local:0123456789:states:startExecution",
            "Parameters": {  
                "Input.$": "$",
                "StateMachineArn": "arn:aws:states:local:0123456789:stateMachine:simple_state_machine"
            },
            "End": true
        }
    }
}"""

callee_ASL = """{
    "Comment": "Test Step Function",
    "StartAt": "StartState",
    "States": {
        "StartState": {
            "Type": "Pass",
            "Next": "ChoiceState"
        },
        "ChoiceState": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.lambda",
                    "StringEquals": "InternalErrorNotHandled",
                    "Next": "InternalErrorNotHandledLambda"
                },
                {
                    "Variable": "$.lambda",
                    "StringEquals": "InternalErrorHandled",
                    "Next": "InternalErrorHandledLambda"
                },
                {
                    "Variable": "$.lambda",
                    "StringEquals": "Success",
                    "Next": "SuccessLambda"
                },
                {
                    "Variable": "$.lambda",
                    "StringEquals": "Timeout",
                    "Next": "TimeoutLambda"
                }
            ],
            "Default": "FailState"
        },
        "FailState": {
            "Type": "Fail",
            "Error": "NoLambdaError",
            "Cause": "No Matches!"
        },
        "SuccessLambda": {
            "Type": "Task",
            "Resource": "arn:aws:rpcmessage:local::function:SuccessLambda",
            "Next": "WaitState"
        },
        "InternalErrorNotHandledLambda": {
            "Type": "Task",
            "Resource": "arn:aws:rpcmessage:local::function:InternalErrorNotHandledLambda",
            "Next": "EndState"
        },
        "InternalErrorHandledLambda": {
            "Type": "Task",
            "Resource": "arn:aws:rpcmessage:local::function:InternalErrorHandledLambda",
            "Next": "EndState"
        },
        "TimeoutLambda": {
            "Type": "Task",
            "Resource": "arn:aws:rpcmessage:local::function:TimeoutLambda",
            "Next": "EndState"
        },
        "EndState": {
            "Type": "Pass",
            "End": true
        },
        "WaitState": {
            "Type": "Wait",
            "Seconds":10,
            "Next": "EndState"
        }
    }
}"""

items = ['{"lambda":"Success"}',
         '{"lambda":"InternalErrorNotHandled"}',
         '{"lambda":"InternalErrorHandled"}',
         '{"lambda":"Timeout"}']

items = ['{"lambda":"Success"}']
#items = ['{"lambda":"InternalErrorNotHandled"}']
#items = ['{"lambda":"InternalErrorHandled"}']
#items = ['{"lambda":"Timeout"}']

if __name__ == '__main__':
    # Initialise logger
    logger = init_logging(log_name='step_by_step')

    # Initialising OpenTracing. It's important to do this before the boto3.client
    # call as create_tracer "patches" boto3 to add the OpenTracing hooks.
    create_tracer("step_by_step", {"implementation": "Jaeger"})

    # Initialise the boto3 client setting the endpoint_url to our local
    # ASL Workflow Engine
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    sfn = boto3.client("stepfunctions", endpoint_url="http://localhost:4584")

    caller_state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:caller_state_machine"
    state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:simple_state_machine"

    def create_state_machines():
        # Create state machine using a dummy roleArn. If it already exists an
        # exception will be thrown, we ignore that but raise other exceptions.
        try:
            response = sfn.create_state_machine(
                name="caller_state_machine", definition=caller_ASL,
                roleArn="arn:aws:iam::0123456789:role/service-role/MyRole"
            )

            response = sfn.create_state_machine(
                name="simple_state_machine", definition=callee_ASL,
                roleArn="arn:aws:iam::0123456789:role/service-role/MyRole"
            )
        except sfn.exceptions.StateMachineAlreadyExists as e:
            #print(e.response)

            response = sfn.update_state_machine(
                stateMachineArn=caller_state_machine_arn,
                definition=caller_ASL
            )

            response = sfn.update_state_machine(
                stateMachineArn=state_machine_arn,
                definition=callee_ASL
            )
        except ClientError as e:
            logger.error(e)


    create_state_machines()

    # Loop through items invoking a new state machine execution for each item
    for item in items:
        try:
            response = sfn.start_execution(
                stateMachineArn=caller_state_machine_arn,
                #name=EXECUTION_NAME, # If not specified a UUID is assigned
                input=item
            )

            """
            Get ARN of the execution just invoked on the caller_state_machine.
            Note that it's more awkward trying to get the execution ARN of
            the execution that this invokes on the callee because we let it use
            a system assigned ARN, but hey! illustrating this stuff is kind of
            the point of this example.

            Note too that, as a distributed and clustered service, the call to      
            sfn.get_execution_history() can fail even when called immediately
            after sfn.start_execution(). Here we simply wrap in a loop and if an  
            ExecutionDoesNotExist exception occurs we briefly sleep then retry.
            If polling in this way is undesireable then using notification
            events (or on real AWS StepFunctions using CloudWatch events) may
            be preferable, albeit at the cost of additional complexity.
            """
            execution_arn = response["executionArn"]
            while True:
                try:
                    history = sfn.get_execution_history(
                        executionArn=execution_arn
                    )
                    break
                except sfn.exceptions.ExecutionDoesNotExist as e:
                    time.sleep(0.1)  # Sleep for 100ms then retry

            print("Execution history for Launcher state machine execution:")
            print(execution_arn)
            print()
            print(history)
            print()

            """
            List all executions for the callee state machine. Do NOT
            use the "SUCCEEDED" statusFilter, because this example includes a
            Wait state which waits for around 10s, so the child execution will
            still only be in the RUNNING state by this point.
            """
            executions = sfn.list_executions(
                stateMachineArn=state_machine_arn
            )
            #print(executions)

        except sfn.exceptions.StateMachineDoesNotExist as e:
            logger.info(e)

            create_state_machines()
            response = sfn.start_execution(
                stateMachineArn=caller_state_machine_arn,
                #name=EXECUTION_NAME, # If not specified a UUID is assigned
                input=item
            )
        except ClientError as e:
            logger.error(e)

    time.sleep(1)  # Give OpenTracing a chance to flush buffer before exiting

