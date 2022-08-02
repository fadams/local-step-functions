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
# PYTHONPATH=.. python3 test_empty_array.py
# PYTHONPATH=.. LOG_LEVEL=DEBUG python3 test_empty_array.py
#
"""
This test tests the case where we might have an empty array as input but
an OutputPath that attempts to match $.inputs[0]
"""

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import unittest
import json
from datetime import datetime, timezone, timedelta
from threading import Timer
from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.state_engine import StateEngine

ASL = """{
    "Comment": "This is your state machine",
    "StartAt": "Pass",
    "States": {
       "Pass": {
            "Type": "Pass",
            "End": true,
            "OutputPath": "$.inputs[0]"
        }
    }
}"""

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
        self.output_event = None # Retain last event so test can check its value

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
    
    def broadcast(self, subject, message, carrier_properties=None):
        self.output_event = message

"""
This stubs out the real TaskDispatcher execute_task method which requires
messaging infrastructure to run whereas this test is just a state machine test.
"""
def execute_task_stub(resource_arn, parameters, callback, timeout, context, id):
    name = resource_arn.split(":")[-1]
    result = {"reply": name + " reply"}
    callback(result)


class TestEmptyArray(unittest.TestCase):

    def setUp(self):
        # Initialise logger
        logger = init_logging(log_name="test_payload_template")
        config = {
            "state_engine": {
                "store_url": "ASL_store.json", 
                "execution_ttl": 500
            }
        }

        state_engine = StateEngine(config)
        # Stub out the real TaskDispatcher execute_task
        state_engine.task_dispatcher.execute_task = execute_task_stub
        self.event_dispatcher = EventDispatcherStub(state_engine, config)
    
    def test_empty_array(self):
        self.event_dispatcher.dispatch('{"data": {"inputs": []}, "context": ' + context + '}')

        #print(self.event_dispatcher.output_event)

        output_event_detail = self.event_dispatcher.output_event["detail"]
        status = output_event_detail["status"]
        print(status)

        self.assertEqual(status, "FAILED")
        print("FAILED is the expected result")

if __name__ == '__main__':
    unittest.main()

