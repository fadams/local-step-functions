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
# PYTHONPATH=.. python3 iterate2.py
#
"""
This test illustrates the case of passing an array of data to a Step Function
and iterating through the array, triggering execution of a child Step Function
for each item in the array. This test is mostly  intended to provide quite a bit
of "stress" to the ASL engine as it can result in rather large JSON objects as
inputs to the state machine, which will tend to make JSONPath processing the
bottleneck, and lots of items in the input JSON array will also result in lots
of state transitions for each execution.


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

iterate2_ASL = """{
    "Comment": "State Machine to illustrate iterating an array and passing each array item to a child Step Function",
    "StartAt": "IsEmptyArray",
    "States": {
        "IsEmptyArray": {
            "Comment": "This Choice state checks for an empty array. Note that it relies on the JSONPath engine explicitly returning False if the path fails to match. As it happens the Python jsonpath library does this but I've not checked the behaviour of AWS StepFunctions for this edge case yet. It is possible that a Task state may be required to return an explicit value for an empty array but it depends on the JSONPath implementation and returning False for ths case is reasonable behaviour though the JSONPath specification is unclear about what should happen in this case.",
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$[0]",
                    "BooleanEquals": false,
                    "Next": "EndState"
                }
            ],
            "Default": "StepFunctionLauncher"
        },
        "StepFunctionLauncher": {
            "Comment": "Launch the child Step Function. In the Parameters section we extract the first item from the array passed as input to the state and use that as the input to the child Step Function. The ResultPath of null causes the input to be passed to the output.",
            "Type": "Task",
            "Resource": "arn:aws:states:local:0123456789:states:startExecution",
            "Parameters": {  
                "Input.$": "$[0]",
                "StateMachineArn": "arn:aws:states:local:0123456789:stateMachine:child_state_machine"
            },
            "ResultPath": null,
            "Next": "RemoveFirstItemFromArray"
        },
        "RemoveFirstItemFromArray": {
            "Type": "Pass",
            "InputPath": "$[1:]",
            "Next": "IsEmptyArray"
        },
        "EndState": {
            "Type": "Succeed"
        }
    }
}"""

child_ASL = """{
    "Comment": "Trivial Child Step Function",
    "StartAt": "StartState",
    "States": {
        "StartState": {
            "Type": "Pass",
            "End": true
        }
    }
}"""

array_length = 1000
item = "[0" + "".join(", " + str(i) for i in range(1, array_length)) + "]"
#items = [item]
items = [item, item, item, item, item, item, item, item, item, item, item, item, item, item, item, item, item, item, item, item]

if __name__ == '__main__':
    # Initialise logger
    logger = init_logging(log_name='iterate2')

    # Initialising OpenTracing. It's important to do this before the boto3.client
    # call as create_tracer "patches" boto3 to add the OpenTracing hooks.
    create_tracer("iterate2", {"implementation": "Jaeger"})

    # Initialise the boto3 client setting the endpoint_url to our local
    # ASL Workflow Engine
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    sfn = boto3.client("stepfunctions", endpoint_url="http://localhost:4584")
    iterate2_state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:iterate2_state_machine"
    child_state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:child_state_machine"

    def create_state_machines():
        # Create state machines using a dummy roleArn. If it already exists an
        # exception will be thrown, we ignore that but raise other exceptions.
        try:
            response = sfn.create_state_machine(
                name="iterate2_state_machine", definition=iterate2_ASL,
                roleArn="arn:aws:iam::0123456789:role/service-role/MyRole"
            )

            response = sfn.create_state_machine(
                name="child_state_machine", definition=child_ASL,
                roleArn="arn:aws:iam::0123456789:role/service-role/MyRole"
            )
        except sfn.exceptions.StateMachineAlreadyExists as e:
            #print(e.response)

            response = sfn.update_state_machine(
                stateMachineArn=iterate2_state_machine_arn,
                definition=iterate2_ASL
            )

            response = sfn.update_state_machine(
                stateMachineArn=child_state_machine_arn,
                definition=child_ASL
            )
        except ClientError as e:
            logger.error(e)


    create_state_machines()

    # Loop through items invoking a new state machine execution for each item
    for item in items:
        try:
            response = sfn.start_execution(
                stateMachineArn=iterate2_state_machine_arn,
                #name=EXECUTION_NAME, # If not specified a UUID is assigned
                input=item
            )
        except sfn.exceptions.StateMachineDoesNotExist as e:
            logger.info(e)

            create_state_machines()
            response = sfn.start_execution(
                stateMachineArn=iterate2_state_machine_arn,
                #name=EXECUTION_NAME, # If not specified a UUID is assigned
                input=item
            )
        except ClientError as e:
            logger.error(e)

    time.sleep(1)  # Give OpenTracing a chance to flush buffer before exiting

