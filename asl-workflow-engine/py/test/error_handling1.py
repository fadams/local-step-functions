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


"""

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import boto3
from botocore.exceptions import ClientError

from asl_workflow_engine.logger import init_logging

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
        "SuccessLambdaCatchState": {
            "Type": "Fail",
            "Error": "NoLambdaError",
            "Cause": "No Matches!"
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
                }
            ],
            "Catch": [
                {
                    "ErrorEquals": [ "States.ALL" ],
                    "Next": "SuccessLambdaCatchState"
                }
            ]
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
    logger = init_logging(log_name='error_handling1')

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
        self.logger.error(e.response)

