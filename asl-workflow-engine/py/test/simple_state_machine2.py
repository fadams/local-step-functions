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
# PYTHONPATH=.. python3 simple_state_machine2.py
#
"""
This test uses the AWS python SDK boto3 to access our local ASL Workflow Engine
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html
https://docs.aws.amazon.com/code-samples/latest/catalog/code-catalog-python-example_code-stepfunctions.html

Its behaviour is essentially the same as simple_state_machine1.py though this
test explicitly calls create_state_machine. The start_execution call will result
in a message being sent to the asl_workflow_events queue "under the hood".

This test is the rough equivalent of running the following on the AWS CLI
with item 1 to 4 differing by the --input below.

aws stepfunctions --endpoint http://localhost:4584 start-execution --state-machine-arn $STATE_MACHINE_ARN --input '{"lambda":"Success"}' --name success-execution

# InternalErrorNotHandled lambda
aws stepfunctions --endpoint http://localhost:4584 start-execution --state-machine-arn $STATE_MACHINE_ARN --input '{"lambda":"InternalErrorNotHandled"}' --name internal-error-not-handled-execution

# InternalErrorHandled lambda
aws stepfunctions --endpoint http://localhost:4584 start-execution --state-machine-arn $STATE_MACHINE_ARN --input '{"lambda":"InternalErrorHandled"}' --name internal-error-handled-execution

# Timeout lambda
aws stepfunctions --endpoint http://localhost:4584 start-execution --state-machine-arn $STATE_MACHINE_ARN --input '{"lambda":"Timeout"}' --name timeout-execution
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

#items = ['{"lambda":"Success"}']
#items = ['{"lambda":"InternalErrorNotHandled"}']
#items = ['{"lambda":"InternalErrorHandled"}']
#items = ['{"lambda":"Timeout"}']

if __name__ == '__main__':
    # Initialise logger
    logger = init_logging(log_name='simple_state_machine2')

    # Initialising OpenTracing. It's important to do this before the boto3.client
    # call as create_tracer "patches" boto3 to add the OpenTracing hooks.
    create_tracer("simple_state_machine2", {"implementation": "Jaeger"})
    #create_tracer("simple_state_machine2", {"implementation": "Jaeger", "config": {"sampler": {"type": "probabilistic", "param": 0.01}}})
    #create_tracer("simple_state_machine2", {"implementation": "OpenTelemetry", "config": {"exporter": "jaeger-thrift"}})

    # Initialise the boto3 client setting the endpoint_url to our local
    # ASL Workflow Engine
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    sfn = boto3.client("stepfunctions", endpoint_url="http://localhost:4584")
    state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:simple_state_machine"

    def create_state_machines():
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
            response = sfn.create_state_machine(
                name="simple_state_machine", definition=ASL,
                roleArn="arn:aws:iam::0123456789:role/service-role/MyRole",
                loggingConfiguration={
                    "level": "ALL",
                    "destinations": [
                        {"cloudWatchLogsLogGroup": {"logGroupArn": "arn:aws:logs:eu-west-2:0123456789:log-group:log_group_name"}}
                    ]
                }
            )
        except sfn.exceptions.StateMachineAlreadyExists as e:
            #print(e.response)

            response = sfn.update_state_machine(
                stateMachineArn=state_machine_arn,
                definition=ASL,
                loggingConfiguration={
                    "level": "ALL",
                    "destinations": [
                        {"cloudWatchLogsLogGroup": {"logGroupArn": "arn:aws:logs:eu-west-2:0123456789:log-group:log_group_name"}}
                    ]
                }
            )
        except ClientError as e:
            logger.error(e)
            sys.exit(1)


    create_state_machines()

    # Loop through items invoking a new state machine execution for each item
    for item in items:
        try:
            response = sfn.start_execution(
                stateMachineArn=state_machine_arn,
                #name=EXECUTION_NAME, # If not specified a UUID is assigned
                input=item
            )
        except sfn.exceptions.StateMachineDoesNotExist as e:
            logger.info(e)

            create_state_machines()
            response = sfn.start_execution(
                stateMachineArn=state_machine_arn,
                #name=EXECUTION_NAME, # If not specified a UUID is assigned
                input=item
            )
        except ClientError as e:
            logger.error(e)

    time.sleep(1)  # Give OpenTracing a chance to flush buffer before exiting

