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
# PYTHONPATH=.. python3 id_then_unzip.py
#
"""
"""

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import datetime
from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.amqp_0_9_1_messaging import Connection, Message
from asl_workflow_engine.exceptions import *

ASL = """{
  "Comment": "A Simple step function to perform MIME ID followed by untar",
  "StartAt": "MimeID",
  "States": {
    "MimeID": {
      "Type": "Task",
      "Resource": "arn:aws:rpcmessage:local::function:mime-id",
      "ResultPath": null,
      "Next": "BsdtarUnzip"
    },
    "BsdtarUnzip": {
      "Type": "Task",
      "Resource": "arn:aws:rpcmessage:local::function:bsdtar-unzip",
      "End": true
    }
  }
}"""


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
context = '{"State": {"EnteredTime": "' + datetime.datetime.now().isoformat() + '", "Name": ""}, "StateMachine": {"Id": "arn:aws:states:local:1234:stateMachine:id_then_unzip", "Value": ' + ASL + '}}'

#print("----------------------")
#print(context)
#print("----------------------")

items = ['{"data": {"zipfile": "s3://37199-dev/CFX/input-data/akismet.2.5.3.zip", "destination": "s3://37199-dev/CFX/processed-data"}, "context": ' + context + '}']

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

