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
# PYTHONPATH=.. python3 simple_embedded_state_machine1.py
#
"""
This test is the rough equivalent of running the following on the AWS CLI
with item 1 to 4 differing by the --input below. Eventually the aim is to
emulate the Step Function server but for now push all the required info to
the state engine.

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

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.state_engine import StateEngine

ASL = '{"Comment": "Test Step Function","StartAt": "StartState","States": {"StartState": {"Type": "Pass","Next": "ChoiceState"},"ChoiceState": {"Type": "Choice","Choices": [{"Variable": "$.lambda","StringEquals": "InternalErrorNotHandled","Next": "InternalErrorNotHandledLambda"},{"Variable": "$.lambda","StringEquals": "InternalErrorHandled","Next": "InternalErrorHandledLambda"},{"Variable": "$.lambda","StringEquals": "Success","Next": "SuccessLambda"},{"Variable": "$.lambda","StringEquals": "Timeout","Next": "TimeoutLambda"}],"Default": "FailState"},"FailState": {"Type": "Fail","Error": "NoLambdaError","Cause": "No Matches!"},"SuccessLambda": {"Type": "Task","Resource": "$SUCCESS_LAMBDA_ARN","Next": "EndState"},"InternalErrorNotHandledLambda": {"Type": "Task","Resource": "$INTERNAL_ERROR_NOT_HANDLED_LAMBDA_ARN","Next": "EndState"},"InternalErrorHandledLambda": {"Type": "Task","Resource": "$INTERNAL_ERROR_HANDLED_LAMBDA_ARN","Next": "EndState"},"TimeoutLambda": {"Type": "Task","Resource": "$TIMEOUT_LAMBDA_ARN","Next": "EndState"},"EndState": {"Type": "Pass","End": true}}}'

items = ['{"CurrentState": "", "Data": {"lambda":"Success"}, "ASL":' + ASL + ',"ASLRef": "arn:aws:states:local:1234:stateMachine:simple_state_machine1"}', '{"CurrentState": "", "Data": {"lambda":"InternalErrorNotHandled"}, "ASL":' + ASL + ',"ASLRef": "arn:aws:states:local:1234:stateMachine:simple_state_machine1"}', '{"CurrentState": "", "Data": {"lambda":"InternalErrorHandled"}, "ASL":' + ASL + ',"ASLRef": "arn:aws:states:local:1234:stateMachine:simple_state_machine1"}', '{"CurrentState": "", "Data": {"lambda":"Timeout"}, "ASL":' + ASL + ',"ASLRef": "arn:aws:states:local:1234:stateMachine:simple_state_machine1"}']

if __name__ == '__main__':
    # Initialise logger
    logger = init_logging(log_name='simple_embedded_state_machine1')
    config = {"state_engine": {"asl_cache": "ASL.json"}}

    # TODO this no longer works 'cause I've moved to a proper context object
    state_engine = StateEngine(logger, config)
    for item in items:
        state_engine.notify(item)

