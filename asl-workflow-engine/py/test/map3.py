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
# PYTHONPATH=.. python3 map3.py
#
"""
This test uses the AWS python SDK boto3 to access our local ASL Workflow Engine
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html
https://docs.aws.amazon.com/code-samples/latest/catalog/code-catalog-python-example_code-stepfunctions.html

This example illustrates how to implement the behaviour of the iterate1.py
example using the ASL Map state.
"""

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import boto3, time
from botocore.exceptions import ClientError

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.open_tracing_factory import create_tracer

ASL = """{
  "Comment": "State Machine to illustrate processing an array using an ASL Map state and passing each array item to a child Step Function. In this example InputPath, ItemsPath, MaxConcurrency and ResultPath are all omitted as we are using their defaults. The OutputPath is null, that means the input and Result are discarded, and the effective output from the state is an empty JSON object, {}",
  "StartAt": "ProcessArray",
  "States": {
    "ProcessArray": {
      "Type": "Map",
      "OutputPath": null,
      "Iterator": {
        "StartAt": "StepFunctionLauncher",
        "States": {
          "StepFunctionLauncher": {
            "Type": "Task",
            "Resource": "arn:aws:states:local:0123456789:states:startExecution",
            "Parameters": {  
                "Input.$": "$",
                "StateMachineArn": "arn:aws:states:local:0123456789:stateMachine:child_state_machine"
            },
            "Comment": "Launch the child Step Function, the input of which should be the *effective* input of the iteration.",
            "End": true
          }
        }
      },
      "End": true
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
    logger = init_logging(log_name="map3")

    # Initialising OpenTracing. It's important to do this before the boto3.client
    # call as create_tracer "patches" boto3 to add the OpenTracing hooks.
    create_tracer("map3", {"implementation": "Jaeger"})

    # Initialise the boto3 client setting the endpoint_url to our local
    # ASL Workflow Engine
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    sfn = boto3.client("stepfunctions", endpoint_url="http://localhost:4584")
    state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:map3"
    child_state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:child_state_machine"

    # Create state machine using a dummy roleArn. If it already exists an
    # exception will be thrown, we ignore that but raise other exceptions.
    try:
        response = sfn.create_state_machine(
            name="map3", definition=ASL,
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
                stateMachineArn=state_machine_arn,
                definition=ASL
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
                stateMachineArn=state_machine_arn,
                #name=EXECUTION_NAME, # If not specified a UUID is assigned
                input=item
            )
    except ClientError as e:
        logger.error(e.response)

    time.sleep(1)  # Give OpenTracing a chance to flush buffer before exiting

