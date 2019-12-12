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
# PYTHONPATH=.. python3 error_handling1.py
#
"""
This test uses the AWS python SDK boto3 to access our local ASL Workflow Engine
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html
https://docs.aws.amazon.com/code-samples/latest/catalog/code-catalog-python-example_code-stepfunctions.html

This tests ASL retriers and catchers responding to a timeout caused by rpcmessage
workers not being available to respond to the Task request. The second retrier
should never be reached in thisw example because the only error is a timeout
and when the number of retries for States.Timeout is exceeded it jumps straight
to the catcher which is a match all and causes a transition to SuccessLambdaCatchState
"""

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import boto3, time
from botocore.exceptions import ClientError

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.open_tracing_factory import create_tracer

ASL = """{
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
                    "StringEquals": "Success",
                    "Next": "SuccessLambda"
                }
            ],
            "Default": "FailState"
        },
        "FailState": {
            "Type": "Fail",
            "Error": "NoLambdaError",
            "Cause": "No Matches!"
        },
        "SuccessLambdaCatchState": {
            "Type": "Fail",
            "Error": "States.Timeout",
            "Cause": "Lambda timed out and retries failed!"
        },
        "SuccessLambda": {
            "Type": "Task",
            "Resource": "arn:aws:rpcmessage:local::function:SuccessLambda",
            "Next": "WaitState",
            "TimeoutSeconds": 10,
            "Retry" : [
                {
                    "ErrorEquals": [ "States.Timeout" ],
                    "IntervalSeconds": 3,
                    "MaxAttempts": 2,
                    "BackoffRate": 1.5
                },
                {
                    "ErrorEquals": [ "States.ALL" ]
                }
            ],
            "Catch": [
                {
                    "ErrorEquals": [ "States.ALL" ],
                    "ResultPath": "$.error-info",
                    "Next": "SuccessLambdaCatchState"
                }
            ]
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

items = ['{"lambda":"Success"}']

if __name__ == '__main__':
    # Initialise logger
    logger = init_logging(log_name='error_handling1')

    # Initialising OpenTracing. It's important to do this before the boto3.client
    # call as create_tracer "patches" boto3 to add the OpenTracing hooks.
    create_tracer("error_handling1", {"implementation": "Jaeger"})

    # Initialise the boto3 client setting the endpoint_url to our local
    # ASL Workflow Engine
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    sfn = boto3.client("stepfunctions", endpoint_url="http://localhost:4584")
    state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:error_handling_state_machine"

    # Create state machine using a dummy roleArn. If it already exists an
    # exception will be thrown, we ignore that but raise other exceptions.
    try:
        response = sfn.create_state_machine(
            name="error_handling_state_machine", definition=ASL,
            roleArn="arn:aws:iam::0123456789:role/service-role/MyRole"
        )
    except ClientError as e:
        #print(e.response)
        message = e.response["Error"]["Message"]
        #code = e.response["Error"]["Code"]
        if message == "StateMachineAlreadyExists": # Do update instead of create
            response = sfn.update_state_machine(
                stateMachineArn=state_machine_arn,
                definition=ASL
            )
        else: raise


    # Loop through items invoking a new state machine execution for each item
    try:
        for item in items:
            response = sfn.start_execution(
            stateMachineArn=state_machine_arn,
            #name=EXECUTION_NAME, # If not specified a UUID is assigned
            input=item
        )
    except ClientError as e:
        logger.error(e.response)

    time.sleep(1)  # Give OpenTracing a chance to flush buffer before exiting

