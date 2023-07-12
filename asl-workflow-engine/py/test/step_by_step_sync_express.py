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
# PYTHONPATH=.. python3 step_by_step_sync_express.py
#
"""
This test illustrates one step function invoking another synchronously.
With step_by_step.py the child state machine is called asynchronously in a
"fire and forget" manner so the parent execution of the sync_express_caller_state_machine
completes/succeeds almost immediately whilst the child simple_state_machine
waits in a Wait state. With this example though the parent state machine
will not exit the StepFunctionLauncher Task state until the child state
machine has completed so it will not succeed until after the child state
machine has succeeded, a subtle but very important difference.

Note that using the resource aws-sdk:sfn:startSyncExecution causes the result of
the nested state machine to be returned to the calling Task as JSON.


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
            "Resource": "arn:aws:states:local::aws-sdk:sfn:startSyncExecution",
            "Parameters": {  
                "Input.$": "$",
                "StateMachineArn": "arn:aws:states:local:0123456789:stateMachine:simple_express_state_machine"
            },
            "OutputPath": "$.Output",
            "ResultSelector": {
                "Output.$": "States.StringToJson($.Output)"
            },
            "TimeoutSeconds": 15,
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
    logger = init_logging(log_name='step_by_step_sync_express')

    # Initialising OpenTracing. It's important to do this before the boto3.client
    # call as create_tracer "patches" boto3 to add the OpenTracing hooks.
    create_tracer("step_by_step_sync_express", {"implementation": "Jaeger"})

    # Initialise the boto3 client setting the endpoint_url to our local
    # ASL Workflow Engine
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    sfn = boto3.client("stepfunctions", endpoint_url="http://localhost:4584")

    sync_express_caller_state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:sync_express_caller_state_machine"
    express_state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:simple_express_state_machine"

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
                name="sync_express_caller_state_machine", definition=caller_ASL,
                type="EXPRESS",
                roleArn="arn:aws:iam::0123456789:role/service-role/MyRole",
                loggingConfiguration={
                    "level": "ALL",
                    "includeExecutionData": True,
                    "destinations": [
                        {"cloudWatchLogsLogGroup": {"logGroupArn": "arn:aws:logs:eu-west-2:0123456789:log-group:log_group_name"}}
                    ]
                }
            )

            response = sfn.create_state_machine(
                name="simple_express_state_machine", definition=callee_ASL,
                type="EXPRESS",
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

            response = sfn.update_state_machine(
                stateMachineArn=sync_express_caller_state_machine_arn,
                definition=caller_ASL,
                loggingConfiguration={
                    "level": "ALL",
                    "includeExecutionData": True,
                    "destinations": [
                        {"cloudWatchLogsLogGroup": {"logGroupArn": "arn:aws:logs:eu-west-2:0123456789:log-group:log_group_name"}}
                    ]
                }
            )

            response = sfn.update_state_machine(
                stateMachineArn=express_state_machine_arn,
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


    create_state_machines()

    # Loop through items invoking a new state machine execution for each item
    for item in items:
        try:
            response = sfn.start_execution(
                stateMachineArn=sync_express_caller_state_machine_arn,
                #name=EXECUTION_NAME, # If not specified a UUID is assigned
                input=item
            )
        except sfn.exceptions.StateMachineDoesNotExist as e:
            logger.info(e)

            create_state_machines()
            response = sfn.start_execution(
                stateMachineArn=sync_express_caller_state_machine_arn,
                #name=EXECUTION_NAME, # If not specified a UUID is assigned
                input=item
            )
        except ClientError as e:
            logger.error(e)

    time.sleep(1)  # Give OpenTracing a chance to flush buffer before exiting

