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
# PYTHONPATH=.. python3 simple_state_machine1.py
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

import datetime
from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.amqp_0_9_1_messaging import Connection, Message
from asl_workflow_engine.exceptions import *

ASL = '{"Comment": "Test Step Function","StartAt": "StartState","States": {"StartState": {"Type": "Pass","Next": "ChoiceState"},"ChoiceState": {"Type": "Choice","Choices": [{"Variable": "$.lambda","StringEquals": "InternalErrorNotHandled","Next": "InternalErrorNotHandledLambda"},{"Variable": "$.lambda","StringEquals": "InternalErrorHandled","Next": "InternalErrorHandledLambda"},{"Variable": "$.lambda","StringEquals": "Success","Next": "SuccessLambda"},{"Variable": "$.lambda","StringEquals": "Timeout","Next": "TimeoutLambda"}],"Default": "FailState"},"FailState": {"Type": "Fail","Error": "NoLambdaError","Cause": "No Matches!"},"SuccessLambda": {"Type": "Task","Resource": "arn:aws:rpcmessage:local::function:SuccessLambda","Next": "WaitState"},"InternalErrorNotHandledLambda": {"Type": "Task","Resource": "arn:aws:rpcmessage:local::function:InternalErrorNotHandledLambda","Next": "EndState"},"InternalErrorHandledLambda": {"Type": "Task","Resource": "arn:aws:rpcmessage:local::function:InternalErrorHandledLambda","Next": "EndState"},"TimeoutLambda": {"Type": "Task","Resource": "arn:aws:rpcmessage:local::function:TimeoutLambda","Next": "EndState"},"EndState": {"Type": "Pass","End": true},"WaitState": {"Type": "Wait","Seconds":10,"Next": "EndState"}}}'


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
        "Value": <Object representing ASL state machine>
    },
    "Task": {
        "Token": <String>
    }
}

The most important paths for state traversal are:
$$.State.Name = the current state
$$.StateMachine.Value = (optional) contains the complete ASL state machine
$$.StateMachine.Id = a unique reference to an ASL state machine
"""
context = '{"State": {"EnteredTime": "' + datetime.datetime.now().isoformat() + '", "Name": ""}, "StateMachine": {"Id": "arn:aws:states:local:1234:stateMachine:simple_state_machine1", "Value": ' + ASL + '}}'

#print("----------------------")
#print(context)
#print("----------------------")

items = ['{"data": {"lambda":"Success"}, "context": ' + context + '}',
         '{"data": {"lambda":"InternalErrorNotHandled"}, "context": ' + context + '}',
         '{"data": {"lambda":"InternalErrorHandled"}, "context": ' + context + '}',
         '{"data": {"lambda":"Timeout"}, "context": ' + context + '}']

#items = ['{"data": {"lambda":"Success"}, "context": ' + context + '}']
#items = ['{"data": {"lambda":"InternalErrorNotHandled"}, "context": ' + context + '}']
#items = ['{"data": {"lambda":"InternalErrorHandled"}, "context": ' + context + '}']
#items = ['{"data": {"lambda":"Timeout"}, "context": ' + context + '}']

if __name__ == '__main__':
    # Initialise logger
    logger = init_logging(log_name='simple_state_machine1')

    # Connect to event queue and send items.
    connection = Connection("amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=0")
    try:
        connection.open()
        session = connection.session()
        producer = session.producer("asl_workflow_events") # event queue
        for item in items:
            """
            Setting content_type isn't necessary for correct operation,
            however it is the correct thing to do:
            https://www.ietf.org/rfc/rfc4627.txt.
            """
            producer.send(Message(item, content_type="application/json"))
        connection.close();
    # Could catch MessagingError if we don't want to handle these separately
    except ConnectionError as e:
        self.logger.error(e)
    except SessionError as e:
        self.logger.error(e)
    except ConsumerError as e:
        self.logger.error(e)
    except ProducerError as e:
        self.logger.error(e)

