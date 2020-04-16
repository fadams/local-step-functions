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
# PYTHONPATH=.. python3 map2.py
#
"""
This test uses the AWS python SDK boto3 to access our local ASL Workflow Engine
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html
https://docs.aws.amazon.com/code-samples/latest/catalog/code-catalog-python-example_code-stepfunctions.html

This example is a minimal example to illustrate use of the Map State
MaxConcurrency field, which can help limit throughput and somewhat serialise
access to expensive (or otherwise limited) resources to avoid overwhelming them
https://states-language.net/spec.html#map-state
"""

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import boto3, time
from botocore.exceptions import ClientError

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.open_tracing_factory import create_tracer

ASL = """{
  "Comment": "Map Example.",
  "StartAt": "Validate-All",
  "States": {
    "Validate-All": {
      "Type": "Map",
      "InputPath": "$",
      "ItemsPath": "$.items",
      "MaxConcurrency": 3,
      "ResultPath": "$.items",
      "Parameters": {
        "item.$": "$$.Map.Item.Value",
        "index.$": "$$.Map.Item.Index"
      },
      "Iterator": {
        "StartAt": "Validate",
        "States": {
          "Validate": {
            "Type": "Pass",
            "Comment": "The Result from each iteration should be its *effective* input, which is a JSON node that contains both the current item data from the context object, and the courier information from the 'delivery-partner' field from the Map state input.",
            "End": true
          }
        }
      },
      "End": true
    }
  }
}"""

items = ["""
{
    "items": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
}
"""]

if __name__ == '__main__':
    # Initialise logger
    logger = init_logging(log_name="map2")

    # Initialising OpenTracing. It's important to do this before the boto3.client
    # call as create_tracer "patches" boto3 to add the OpenTracing hooks.
    create_tracer("map2", {"implementation": "Jaeger"})

    # Initialise the boto3 client setting the endpoint_url to our local
    # ASL Workflow Engine
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    sfn = boto3.client("stepfunctions", endpoint_url="http://localhost:4584")
    state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:map2"

    def create_state_machines():
        # Create state machine using a dummy roleArn. If it already exists an
        # exception will be thrown, we ignore that but raise other exceptions.
        try:
            response = sfn.create_state_machine(
                name="map2", definition=ASL,
                roleArn="arn:aws:iam::0123456789:role/service-role/MyRole"
            )
        except sfn.exceptions.StateMachineAlreadyExists as e:
            #print(e.response)

            response = sfn.update_state_machine(
                stateMachineArn=state_machine_arn,
                definition=ASL
            )
        except ClientError as e:
            logger.error(e)


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

