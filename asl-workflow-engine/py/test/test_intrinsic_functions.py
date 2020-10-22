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
# PYTHONPATH=.. python3 test_intrinsic_functions.py
# PYTHONPATH=.. LOG_LEVEL=DEBUG python3 test_intrinsic_functions.py
#
"""
This test tests the ASL update that added Intrinsic Function support.
https://states-language.net/#intrinsic-functions

The State Machine in this test is rather contrived with a Parameters field in
the Map state intended to exercise all of the available Intrinsic Functions
including nesting of Intrinsic Functions.
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
    "StartAt": "ChoiceState",
    "States": {
       "ChoiceState": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.items[0]",
                    "NumericEquals": 0,
                    "Next": "Validate-All"
                }
            ]
        },
        "Validate-All": {
          "Comment": "The slightly ludicrous format.$ field in Parameters deliberately has weird whitespace to test that the Intrinsic Function argument parser can deal with such things.",
          "Type": "Map",
          "InputPath": "$",
          "ItemsPath": "$.items",
          "MaxConcurrency": 3,
          "ResultPath": "$",
          "Parameters": {
            "item.$": "$$.Map.Item.Value",
            "index.$": "$$.Map.Item.Index",
            "format.$": "States.Format  (  'Execution started at {}, {}. Extra args are {}, {}, {}, {}'  ,    $$.Execution.StartTime   , States.Format   ( 'OK {}'  , 'y\\\\'all' ), -1.0, 1234, $.items[0]  ,  null  )",
            "array.$": "States.Array('Foo', 2020, $.someJson, null)",
            "s2j1.$": "States.StringToJson(  '   {\\"number1\\": 20}  '  )",
            "s2j2.$": "States.StringToJson(States.Format('{}{}', '{\\"numb', 'er2\\": 20.5}'))",
            "s2j3.$": "States.StringToJson($.someString)",
            "j2s.$": "States.JsonToString($.someJson)"
          },
          "ResultSelector": {
            "state.$": "$$.State.Name",
            "items.$": "$[:].item",
            "item0.$": "$[0]"
          },
          "Iterator": {
            "StartAt": "Validate",
            "States": {
              "Validate": {
                "Type": "Pass",
                "Comment": "The Result from each iteration should be its *effective* input, which is a JSON node that contains the current item data and its index from the context object.",
                "End": true
              }
            }
          },
          "End": true
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
        # Save Execution StartTime to use when validating output
        if self.message_count == 0:
            self.execution_start_time = item["context"]["Execution"]["StartTime"]
        # Convert event back to JSON string for dispatching.
        self.dispatch(json.dumps(item))
    
    def broadcast(self, subject, message, carrier_properties=None):
        self.output_event = message

"""
This stubs out the real TaskDispatcher execute_task method which requires
messaging infrastructure to run whereas this test is just a state machine test.
"""
def execute_task_stub(resource_arn, parameters, callback):
    name = resource_arn.split(":")[-1]
    result = {"reply": name + " reply"}
    callback(result)


class TestIntrinsicFunctions(unittest.TestCase):

    def setUp(self):
        # Initialise logger
        logger = init_logging(log_name="test_intrinsic_functions")
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
    
    def test_intrinsic_functions(self):
        self.event_dispatcher.dispatch('{"data": {"someJson": {"random": "abcdefg"}, "someString": "{\\"number3\\": 25}", "items": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]}, "context": ' + context + '}')

        #print(self.event_dispatcher.output_event)
        execution_start_time = self.event_dispatcher.execution_start_time
        output_event_detail = self.event_dispatcher.output_event["detail"]
        status = output_event_detail["status"]
        output_str = output_event_detail["output"]

        output = json.loads(output_str)
        print(output)

        check_items = output["items"] == [0,1,2,3,4,5,6,7,8,9,10,11,12]
        check_state = output["state"] == "Validate-All"
        check_item0_s2j1_number1 = output["item0"]["s2j1"]["number1"] == 20
        check_item0_s2j2_number2 = output["item0"]["s2j2"]["number2"] == 20.5
        check_item0_s2j3_number3 = output["item0"]["s2j3"]["number3"] == 25
        check_item0_j2s = output["item0"]["j2s"] == '{"random":"abcdefg"}'
        check_item0_array = output["item0"]["array"] == ["Foo",2020,{"random":"abcdefg"},None]

        format = "Execution started at " + execution_start_time + ", OK y\\'all. Extra args are -1.0, 1234, 0, None"
        check_item0_format = output["item0"]["format"] == format

        self.assertEqual(status, "SUCCEEDED")
        self.assertEqual(check_items, True)
        self.assertEqual(check_state, True)
        self.assertEqual(check_item0_s2j1_number1, True)
        self.assertEqual(check_item0_s2j2_number2, True)
        self.assertEqual(check_item0_s2j3_number3, True)
        self.assertEqual(check_item0_j2s, True)
        self.assertEqual(check_item0_array, True)
        self.assertEqual(check_item0_format, True)

if __name__ == '__main__':
    unittest.main()

