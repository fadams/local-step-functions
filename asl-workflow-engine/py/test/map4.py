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
# PYTHONPATH=.. python3 map4.py
#
"""
This test uses the AWS python SDK boto3 to access our local ASL Workflow Engine
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html
https://docs.aws.amazon.com/code-samples/latest/catalog/code-catalog-python-example_code-stepfunctions.html

This example is a minimal example to illustrate use of the Map State
https://states-language.net/spec.html#map-state

It is a modification of the second example (Map State Example With Parameters)
from the AWS Step Functions Developer Guide made even simpler and more
self-contained by using basic Pass States.
https://github.com/awsdocs/aws-step-functions-developer-guide/blob/master/doc_source/amazon-states-language-map-state.md#map-state-example-with-parameters

This test is based on map1.py but adds a Fail State to deliberately fail.
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
      "InputPath": "$.detail",
      "ItemsPath": "$.shipped",
      "MaxConcurrency": 0,
      "ResultPath": "$.detail.shipped",
      "Parameters": {
        "parcel.$": "$$.Map.Item.Value",
        "courier.$": "$.delivery-partner"
      },
      "Iterator": {
        "StartAt": "Validate",
        "States": {
          "Validate": {
            "Type": "Pass",
            "Comment": "The Result from each iteration should be its *effective* input, which is a JSON node that contains both the current item data from the context object, and the courier information from the 'delivery-partner' field from the Map state input.",
            "Next": "FailState"
          },
          "FailState": {
            "Type": "Fail"
          }
        }
      },
      "End": true
    }
  }
}"""

items = ["""
{
  "ship-date": "2016-03-14T01:59:00Z",
  "detail": {
    "delivery-partner": "UQS",
    "shipped": [
      { "prod": "R31", "dest-code": 9511, "quantity": 1344 },
      { "prod": "S39", "dest-code": 9511, "quantity": 40 },
      { "prod": "R31", "dest-code": 9833, "quantity": 12 },
      { "prod": "R40", "dest-code": 9860, "quantity": 887 },
      { "prod": "R40", "dest-code": 9511, "quantity": 1220 }
    ]
  }
}
"""]

if __name__ == '__main__':
    # Initialise logger
    logger = init_logging(log_name="map4")

    # Initialising OpenTracing. It's important to do this before the boto3.client
    # call as create_tracer "patches" boto3 to add the OpenTracing hooks.
    create_tracer("map4", {"implementation": "Jaeger"})

    # Initialise the boto3 client setting the endpoint_url to our local
    # ASL Workflow Engine
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    sfn = boto3.client("stepfunctions", endpoint_url="http://localhost:4584")
    state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:map4"

    def create_state_machines():
        # Create state machine using a dummy roleArn. If it already exists an
        # exception will be thrown, we ignore that but raise other exceptions.
        try:
            response = sfn.create_state_machine(
                name="map4", definition=ASL,
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
                stateMachineArn=state_machine_arn,
                definition=ASL,
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

