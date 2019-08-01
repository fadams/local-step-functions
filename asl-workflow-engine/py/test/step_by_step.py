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

Note that in real AWS Step Functions the pattern would be "to create a state
machine with a Lambda function that can start a new execution, continuing your
ongoing work in that new execution." - for an example see this AWS tutorial:
https://docs.aws.amazon.com/step-functions/latest/dg/tutorial-continue-new.html

It is not clear why AWS has not implemented a direct service integration for this
https://docs.aws.amazon.com/step-functions/latest/dg/concepts-service-integrations.html

For convenience local ASL Workflow Engine *has* implemented a service integration
using a resource ARN of the form:
arn:aws:states:region:account-id:execution:stateMachineName:executionName


This test uses the AWS python SDK boto3 to access our local ASL Workflow Engine
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html
https://docs.aws.amazon.com/code-samples/latest/catalog/code-catalog-python-example_code-stepfunctions.html
"""

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import boto3
from botocore.exceptions import ClientError

from asl_workflow_engine.logger import init_logging

"""
In the Resource ARN below the executionName is not specified, so a UUID will
be automatically assigned. We could alternatively have used an ARN such as:
arn:aws:states:local:0123456789:execution:simple_state_machine:executionName
however for stepfunctions each execution should have a unique name, so if we
name a resource like that in theory it could only execute once. In practice
we don't *yet* handle all of the execution behaviour correctly, but we should
eventually. The way to specify specific execution names (if so desired) for
launching via Task states is most likely via ASL Parameters. Using Parameters
we could pass in the execution name in the stepfunction input and extract it
in the Parameter's JSONPath processing. 
"""
caller_ASL = """{
    "StartAt": "StepFunctionLauncher",
    "States": {
        "StepFunctionLauncher": {
            "Type": "Task",
            "Resource": "arn:aws:states:local:0123456789:execution:simple_state_machine",
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

    # Initialise the boto3 client setting the endpoint_url to our local
    # ASL Workflow Engine
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    sfn = boto3.client("stepfunctions", endpoint_url="http://localhost:4584")
    caller_state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:caller_state_machine"
    state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:simple_state_machine"

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
    except ClientError as e:
        #print(e.response)
        message = e.response["Error"]["Message"]
        #code = e.response["Error"]["Code"]
        if message == "StateMachineAlreadyExists": # Do update instead of create
            response = sfn.update_state_machine(
                stateMachineArn=caller_state_machine_arn,
                definition=caller_ASL
            )

            response = sfn.update_state_machine(
                stateMachineArn=state_machine_arn,
                definition=callee_ASL
            )
        else: raise


    # Loop through items invoking a new state machine execution for each item
    try:
        for item in items:
            response = sfn.start_execution(
            stateMachineArn=caller_state_machine_arn,
            #name=EXECUTION_NAME, # If not specified a UUID is assigned
            input=item
        )
    except ClientError as e:
        self.logger.error(e.response)

