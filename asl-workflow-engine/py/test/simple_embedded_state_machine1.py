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

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

from asl_workflow_engine.state_engine import StateEngine


ASL = '{"Comment": "Test Step Function","StartAt": "StartState","States": {"StartState": {"Type": "Pass","Next": "ChoiceState"},"ChoiceState": {"Type": "Choice","Choices": [{"Variable": "$.lambda","StringEquals": "InternalErrorNotHandled","Next": "InternalErrorNotHandledLambda"},{"Variable": "$.lambda","StringEquals": "InternalErrorHandled","Next": "InternalErrorHandledLambda"},{"Variable": "$.lambda","StringEquals": "Success","Next": "SuccessLambda"},{"Variable": "$.lambda","StringEquals": "Timeout","Next": "TimeoutLambda"}],"Default": "FailState"},"FailState": {"Type": "Fail","Error": "NoLambdaError","Cause": "No Matches!"},"SuccessLambda": {"Type": "Task","Resource": "$SUCCESS_LAMBDA_ARN","Next": "EndState"},"InternalErrorNotHandledLambda": {"Type": "Task","Resource": "$INTERNAL_ERROR_NOT_HANDLED_LAMBDA_ARN","Next": "EndState"},"InternalErrorHandledLambda": {"Type": "Task","Resource": "$INTERNAL_ERROR_HANDLED_LAMBDA_ARN","Next": "EndState"},"TimeoutLambda": {"Type": "Task","Resource": "$TIMEOUT_LAMBDA_ARN","Next": "EndState"},"EndState": {"Type": "Pass","End": true}}}'



if __name__ == '__main__':
    state_engine = StateEngine()
#    print(ASL)
    state_engine.notify(ASL)

