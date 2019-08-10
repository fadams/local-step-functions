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
# PYTHONPATH=.. python3 test_wait_state.py
#

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import unittest
import json
from datetime import datetime, timezone, timedelta
from threading import Timer
from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.state_engine import StateEngine

ASL = """{
    "StartAt": "ChoiceState",
    "States": {
       "ChoiceState": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.test",
                    "NumericEquals": 1,
                    "Next": "TestSeconds"
                },
                {
                    "Variable": "$.test",
                    "NumericEquals": 2,
                    "Next": "TestSecondsPath"
                },
                {
                    "Variable": "$.test",
                    "NumericEquals": 3,
                    "Next": "TestTimestamp"
                },
                {
                    "Variable": "$.test",
                    "NumericEquals": 4,
                    "Next": "TestTimestampPath"
                }
            ]
        },
        "TestSeconds": {
            "Type": "Wait",
            "Seconds":10,
            "End": true
        },
        "TestSecondsPath": {
            "Type": "Wait",
            "SecondsPath":"$.delay",
            "End": true
        },
        "TestTimestamp": {
            "Type": "Wait",
            "Timestamp":"2019-08-08T10:55:25.325038+01:00",
            "End": true
        },
        "TestTimestampPath": {
            "Type": "Wait",
            "TimestampPath":"$.expiry",
            "End": true
        }
    }
}"""

# Replace the absolute time Timestamp stored in the TestTimestamp state with
# the current time plus ten seconds. This will make the test more sensible.
delta = timedelta(seconds=10)
time_now_plus_10s = (datetime.now(timezone.utc) + delta).astimezone().isoformat()
ASL = ASL.replace("2019-08-08T10:55:25.325038+01:00", time_now_plus_10s)


context = '{"StateMachine": {"Id": "arn:aws:states:local:0123456789:stateMachine:simple_state_machine", "Definition": ' + ASL + '}}'

"""
Create a simple EventDispatcher stub so that we can test the State Engine
without requiring the messaging fabric. Rather than publishing each new state
to a queue the stub simply calls notify on the StateEngine with the new state
information. This simplistic approach should be OK for most tests, but we should
be careful of issues due to recursion so we may need to revisit this IDC.
"""
class EventDispatcherStub(object):

    def __init__(self, state_engine, config):

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
        self.state_engine.notify(json.loads(message), self.message_count)

    def acknowledge(self, id):
        pass

    def publish(self, item):
        # Convert event back to JSON string for dispatching.
        self.dispatch(json.dumps(item))

"""
This stubs out the real TaskDispatcher execute_task method which requires
messaging infrastructure to run whereas this test is just a state machine test.
"""
def execute_task_stub(resource_arn, parameters, callback):
    name = resource_arn.split(":")[-1]
    result = {"reply": name + " reply"}
    callback(result)


class TestWaitState(unittest.TestCase):

    def setUp(self):
        # Initialise logger
        logger = init_logging(log_name="test_wait_state")
        config = {"state_engine": {"asl_cache": "ASL.json"}}

        state_engine = StateEngine(config)
        # Stub out the real TaskDispatcher execute_task
        state_engine.task_dispatcher.execute_task = execute_task_stub
        self.event_dispatcher = EventDispatcherStub(state_engine, config)
    
    def test_seconds(self):
        self.event_dispatcher.dispatch('{"data": {"test": 1}, "context": ' + context + '}')

    def test_seconds_path(self):
        self.event_dispatcher.dispatch('{"data": {"test": 2, "delay":15}, "context": ' + context + '}')
    
    def test_timeout(self):
        self.event_dispatcher.dispatch('{"data": {"test": 3}, "context": ' + context + '}')
    
    def test_timeout_path(self):
        delta = timedelta(seconds=10)
        time_now_plus_10s = (datetime.now(timezone.utc) + delta).astimezone().isoformat()
        self.event_dispatcher.dispatch('{"data": {"test": 4, "expiry":"' + time_now_plus_10s + '"}, "context": ' + context + '}')
    

if __name__ == '__main__':
    unittest.main()

