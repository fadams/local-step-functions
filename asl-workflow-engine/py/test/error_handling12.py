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
# PYTHONPATH=.. python3 error_handling12.py
#
"""
This test has a non-returning Task state in the Iterator with a configurable
Timeout used to deliberately fail a given branch that will fail the Map State
and should cause the Tasks in the other branches to be terminated.

This test was added to reproduce a bug that turned out to be due to the Catch
in the Task state incorrectly transitioning to CatchAllFallback on
Task.Terminated preventing branch cancellation from behaving properly. The
fix was to make Task.Terminated a non-catchable/non-retriable error like
States.Runtime which forces the Task state to end on Task.Terminated.
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
      "ItemsPath": "$.items",
      "ResultPath": "$.results",
      "Iterator": {
        "StartAt": "Validate",
        "States": {
          "Validate": {
            "Type": "Task",
            "Resource": "arn:aws:rpcmessage:local::function:NonExistentLambda",
            "TimeoutSecondsPath": "$.timeout",
            "Catch": [
              {
                "ErrorEquals": ["States.ALL"],
                "ResultPath": "$.error-info",
                "Next": "CatchAllFallback"
              }
            ],
            "Next": "Success"
          },
          "Success": {
            "Type": "Pass",
            "End": true
          },
          "CatchAllFallback": {
            "Type": "Pass",
            "Next": "FailState"
          },
          "FailState": {
            "Type": "Fail",
            "Error": "CatchAllFail",
            "Cause": "CatchAllFallback was called"
          }
        }
      },
      "End": true
    }
  }
}"""

# This passes a configurable Task timeout where the first and second items
# timeout after the third and should thus be terminated as soon as the third
# item times out rather than waiting for their own timeouts to complete.
#items = ["""
#{
#    "items": [{"value": 0, "timeout": 30}, {"value": 1, "timeout": 20}, {"value": 2, #"timeout": 10}, {"value": 3, "timeout": 10}, {"value": 4, "timeout": 10}, {"value": 5, #"timeout": 10}, {"value": 6, "timeout": 10}, {"value": 7, "timeout": 10}, {"value": 8, #"timeout": 10}, {"value": 9, "timeout": 10}, {"value": 10, "timeout": 10}, {"value": 11, #"timeout": 10}, {"value": 12, "timeout": 10}]
#}
#"""]

#items = ["""
#{
#    "items": [{"value": 0, "timeout": 30}, {"value": 1, "timeout": 30}, {"value": 2, #"timeout": 10}, {"value": 3, "timeout": 30}, {"value": 4, "timeout": 30}]
#}
#"""]

#items = ["""
#{
#    "items": [{"value": 0, "timeout": 30}, {"value": 1, "timeout": 30}, {"value": 2, #"timeout": 10}]
#}
#"""]

items = ["""
{
    "items": [{"value": 0, "timeout": 30}, {"value": 1, "timeout": 10}]
}
"""]



if __name__ == '__main__':
    # Initialise logger
    logger = init_logging(log_name="error_handling12")

    # Initialising OpenTracing. It's important to do this before the boto3.client
    # call as create_tracer "patches" boto3 to add the OpenTracing hooks.
    create_tracer("error_handling12", {"implementation": "Jaeger"})

    # Initialise the boto3 client setting the endpoint_url to our local
    # ASL Workflow Engine
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    sfn = boto3.client("stepfunctions", endpoint_url="http://localhost:4584")
    state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:error_handling12"

    def create_state_machines():
        # Create state machine using a dummy roleArn. If it already exists an
        # exception will be thrown, we ignore that but raise other exceptions.
        try:
            response = sfn.create_state_machine(
                name="error_handling12", definition=ASL,
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

