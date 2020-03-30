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
# PYTHONPATH=.. python3 iterate1.py
#
"""
This test illustrates the case of passing an array of data to a Step Function
and iterating through the array, triggering execution of a child Step Function
for each item in the array.

Note that this use case is one that would fit better into the new ASL Map state

The Map state provides most benefit when calling out to Tasks that might
take some time as iterating rather than running in parallel clearly results
in cumulative time. Map states also excel where one wishes to "join" the results
and rejoin the original state machine. When one just wishes to trigger a child
state machine for each item those benefits are marginal as each iteration only
costs the state machine launch cost and those are likely to be cumulative even
in the Map state where the implementation is likely to internally iterate
through the array and launch the execution. So for this particular use case the
main benefit of the Map state is likely to be a slight improvement to the
logic and readability - but that's all TBD.

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

iterate1_ASL = """{
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


items = ['[{"category": "reference", "author": "Nigel Rees", "title": "Sayings of the Century", "price": 8.95}, {"category": "fiction", "author": "Evelyn Waugh", "title": "Sword of Honour", "price": 12.99}, {"category": "fiction", "author": "Herman Melville", "title": "Moby Dick", "isbn": "0-553-21311-3", "price": 8.99}, {"category": "fiction", "author": "J. R. R. Tolkien", "title": "The Lord of the Rings", "isbn": "0-395-19395-8", "price": 22.99}]']

if __name__ == '__main__':
    # Initialise logger
    logger = init_logging(log_name='iterate1')

    # Initialising OpenTracing. It's important to do this before the boto3.client
    # call as create_tracer "patches" boto3 to add the OpenTracing hooks.
    create_tracer("iterate1", {"implementation": "Jaeger"})

    # Initialise the boto3 client setting the endpoint_url to our local
    # ASL Workflow Engine
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    sfn = boto3.client("stepfunctions", endpoint_url="http://localhost:4584")
    iterate1_state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:iterate1_state_machine"
    child_state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:child_state_machine"

    # Create state machines using a dummy roleArn. If it already exists an
    # exception will be thrown, we ignore that but raise other exceptions.
    try:
        response = sfn.create_state_machine(
            name="iterate1_state_machine", definition=iterate1_ASL,
            roleArn="arn:aws:iam::0123456789:role/service-role/MyRole"
        )

        response = sfn.create_state_machine(
            name="child_state_machine", definition=child_ASL,
            roleArn="arn:aws:iam::0123456789:role/service-role/MyRole"
        )
    except ClientError as e:
        #print(e.response)
        message = e.response["Error"]["Message"]
        #code = e.response["Error"]["Code"]
        if message == "StateMachineAlreadyExists": # Do update instead of create
            response = sfn.update_state_machine(
                stateMachineArn=iterate1_state_machine_arn,
                definition=iterate1_ASL
            )

            response = sfn.update_state_machine(
                stateMachineArn=child_state_machine_arn,
                definition=child_ASL
            )
        else: raise


    # Loop through items invoking a new state machine execution for each item
    try:
        for item in items:
            response = sfn.start_execution(
                stateMachineArn=iterate1_state_machine_arn,
                #name=EXECUTION_NAME, # If not specified a UUID is assigned
                input=item
            )

            """
            Get ARN of the execution just invoked on the iterate1_state_machine.
            Note that it's more awkward trying to get the execution ARN of
            the execution that this invokes on the callee because we let it use
            a system assigned ARN, but hey illustrating this stuff is kind of
            the point of this example.
            """
            execution_arn = response["executionArn"]
            history = sfn.get_execution_history(
                executionArn=execution_arn
            )
            #print("Execution history for Launcher state machine execution:")
            #print(execution_arn)
            #print()
            #print(history)
            #print()

            """
            List all executions for the callee state machine. Do NOT
            use the "SUCCEEDED" statusFilter, because this example includes a
            Wait state which waits for around 10s, so the child execution will
            still only be in the RUNNING state by this point.
            """
            executions = sfn.list_executions(
                stateMachineArn=child_state_machine_arn
            )
            #print(executions)

    except ClientError as e:
        logger.error(e.response)

    time.sleep(1)  # Give OpenTracing a chance to flush buffer before exiting

