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

from threading import Timer
import datetime
import json
from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.state_engine import StateEngine

ASL = '{"Comment": "Test Step Function","StartAt": "StartState","States": {"StartState": {"Type": "Pass","Next": "ChoiceState"},"ChoiceState": {"Type": "Choice","Choices": [{"Variable": "$.lambda","StringEquals": "InternalErrorNotHandled","Next": "InternalErrorNotHandledLambda"},{"Variable": "$.lambda","StringEquals": "InternalErrorHandled","Next": "InternalErrorHandledLambda"},{"Variable": "$.lambda","StringEquals": "Success","Next": "SuccessLambda"},{"Variable": "$.lambda","StringEquals": "Timeout","Next": "TimeoutLambda"}],"Default": "FailState"},"FailState": {"Type": "Fail","Error": "NoLambdaError","Cause": "No Matches!"},"SuccessLambda": {"Type": "Task","Resource": "$SUCCESS_LAMBDA_ARN","Next": "WaitState"},"InternalErrorNotHandledLambda": {"Type": "Task","Resource": "$INTERNAL_ERROR_NOT_HANDLED_LAMBDA_ARN","Next": "EndState"},"InternalErrorHandledLambda": {"Type": "Task","Resource": "$INTERNAL_ERROR_HANDLED_LAMBDA_ARN","Next": "EndState"},"TimeoutLambda": {"Type": "Task","Resource": "$TIMEOUT_LAMBDA_ARN","Next": "EndState"},"EndState": {"Type": "Pass","End": true},"WaitState": {"Type": "Wait","Seconds":10,"Next": "EndState"}}}'

"""
See https://stackoverflow.com/questions/2150739/iso-time-iso-8601-in-python
For info on creating ISO 8601 time format

The application context is described in the AWS documentation:
https://docs.aws.amazon.com/step-functions/latest/dg/input-output-contextobject.html 

{
    "Execution": {
        "Id": <String>,
        "Input": <Object>,
        "StartTime": <String Format: ISO 8601>
    },
    "State": {
        "EnteredTime": <String Format: ISO 8601>,
        "Name": <String>,
        "RetryCount": <Number>
    },
    "StateMachine": {
        "Id": <String>,
        "value": <Object representing ASL state machine>
    },
    "Task": {
        "Token": <String>
    }
}

The most important paths for state traversal are:
$$.State.Name = the current state
$$.StateMachine.value = (optional) contains the complete ASL state machine
$$.StateMachine.Id = a unique reference to an ASL state machine
"""
context = '{"State": {"EnteredTime": "' + datetime.datetime.now().isoformat() + '", "Name": ""}, "StateMachine": {"Id": "arn:aws:states:local:1234:stateMachine:simple_state_machine1", "value": ' + ASL + '}}'

items = ['{"data": {"lambda":"Success", "result":"Woo Hoo!"}, "context": ' + context + '}',
         '{"data": {"lambda":"InternalErrorNotHandled"}, "context": ' + context + '}',
         '{"data": {"lambda":"InternalErrorHandled"}, "context": ' + context + '}',
         '{"data": {"lambda":"Timeout"}, "context": ' + context + '}']

#items = ['{"data": {"lambda":"Success", "result":"Woo Hoo!"}, "context": ' + context + '}']
#items = ['{"data": {"lambda":"InternalErrorNotHandled"}, "context": ' + context + '}']
#items = ['{"data": {"lambda":"InternalErrorHandled"}, "context": ' + context + '}']
#items = ['{"data": {"lambda":"Timeout"}, "context": ' + context + '}']


"""
Create a simple EventDispatcher stub so that we can test the State Engine
without requiring the messaging fabric. Rather than publishing each new state
to a queue the stub simply calls notify on the StateEngine with the new state
information. This simplistic approach should be OK for most tests, but we should
be careful of issues due to recursion so we may need to revisit this IDC.
"""
class EventDispatcherStub(object):

    def __init__(self, logger, state_engine, config):

        """
        Create an association with the state engine and give that a reference
        back to this event dispatcher so that it can publish events and make
        use of the set_timeout time scheduler.
        """
        self.state_engine = state_engine
        self.state_engine.event_dispatcher = self
        self.message_count = -1

    """
    This simple threaded timeout should work OK, the real timeout is actually
    implemented using Pika's connection.call_later() which is single threaded
    and handled within Pika's event loop. That approach plays much better with
    Pika's event loop.
    """
    def set_timeout(self, callback, delay):
        t = Timer(delay/1000, callback)
        t.start()
        return t

    def dispatch(self, message):
        """
        Start at -1 and increment before call to notify as this stub, unlike
        the real EventDispatcher will recursively call dispatch as the State
        Engine calls publish, this means code after the call to notify won't
        be reached when one might expect it to.
        """
        self.message_count += 1
        # The notify method expects a JSON object not a string.
        state_engine.notify(json.loads(message), self.message_count)

    def acknowledge(self, id):
        pass

    def publish(self, item):
        # Convert event back to JSON string for dispatching.
        self.dispatch(json.dumps(item))

if __name__ == '__main__':
    # Initialise logger
    logger = init_logging(log_name='simple_embedded_state_machine1')
    config = {"state_engine": {"asl_cache": "ASL.json"}}

    state_engine = StateEngine(logger, config)
    event_dispatcher = EventDispatcherStub(logger, state_engine, config)
    for item in items:
        event_dispatcher.dispatch(item)

