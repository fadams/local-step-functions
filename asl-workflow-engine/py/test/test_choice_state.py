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
# PYTHONPATH=.. python3 test_choice_state.py
#
"""
This test tests the ASL choice state implementation
"""

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import unittest
import json
from threading import Timer
from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.state_engine import StateEngine

ASL = """{
    "Comment": "Test Choice State",
    "StartAt": "ChoiceState",
    "States": {
        "ChoiceState": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.string_equals",
                    "StringEquals": "hello world",
                    "Next": "StringEqualsSuccess"
                },
                {
                    "Variable": "$.string_equals_path",
                    "StringEqualsPath": "$.test_value",
                    "Next": "StringEqualsSuccess"
                },
                {
                    "Variable": "$.case_insensitive_string_equals",
                    "CaseInsensitiveStringEquals": "hello world",
                    "Next": "StringEqualsSuccess"
                },
                {
                    "Variable": "$.case_insensitive_string_equals_path",
                    "CaseInsensitiveStringEqualsPath": "$.test_value",
                    "Next": "StringEqualsSuccess"
                },
                {
                    "Variable": "$.string_less_than",
                    "StringLessThan": "apple",
                    "Next": "StringLessThanSuccess"
                },
                {
                    "Variable": "$.string_less_than_path",
                    "StringLessThanPath": "$.test_value",
                    "Next": "StringLessThanSuccess"
                },
                {
                    "Variable": "$.string_greater_than",
                    "StringGreaterThan": "airbnb",
                    "Next": "StringGreaterThanSuccess"
                },
                {
                    "Variable": "$.string_greater_than_path",
                    "StringGreaterThanPath": "$.test_value",
                    "Next": "StringGreaterThanSuccess"
                },
                {
                    "Variable": "$.string_less_than_equals",
                    "StringLessThanEquals": "apple",
                    "Next": "StringLessThanEqualsSuccess"
                },
                {
                    "Variable": "$.string_less_than_equals_path",
                    "StringLessThanEqualsPath": "$.test_value",
                    "Next": "StringLessThanEqualsSuccess"
                },
                {
                    "Variable": "$.string_greater_than_equals",
                    "StringGreaterThanEquals": "airbnb",
                    "Next": "StringGreaterThanEqualsSuccess"
                },
                {
                    "Variable": "$.string_greater_than_equals_path",
                    "StringGreaterThanEqualsPath": "$.test_value",
                    "Next": "StringGreaterThanEqualsSuccess"
                },
                {
                    "Variable": "$.string_matches1",
                    "StringMatches": "foo*.log",
                    "Next": "StringMatchesSuccess"
                },
                {
                    "Variable": "$.string_matches2",
                    "StringMatches": "*.log",
                    "Next": "StringMatchesSuccess"
                },
                {
                    "Variable": "$.string_matches3",
                    "StringMatches": "foo*.*",
                    "Next": "StringMatchesSuccess"
                },
                {
                    "Variable": "$.string_matches4",
                    "StringMatches": "foo\\\\*[hello]\\\\test\\\\.log",
                    "Next": "StringMatchesSuccess"
                },
                {
                    "Variable": "$.numeric_equals",
                    "NumericEquals": 1234,
                    "Next": "NumericEqualsSuccess"
                },
                {
                    "Variable": "$.numeric_equals_path",
                    "NumericEqualsPath": "$.test_value",
                    "Next": "NumericEqualsSuccess"
                },
                {
                    "Variable": "$.numeric_less_than",
                    "NumericLessThan": 1234,
                    "Next": "NumericLessThanSuccess"
                },
                {
                    "Variable": "$.numeric_less_than_path",
                    "NumericLessThanPath": "$.test_value",
                    "Next": "NumericLessThanSuccess"
                },
                {
                    "Variable": "$.numeric_greater_than",
                    "NumericGreaterThan": 1234,
                    "Next": "NumericGreaterThanSuccess"
                },
                {
                    "Variable": "$.numeric_greater_than_path",
                    "NumericGreaterThanPath": "$.test_value",
                    "Next": "NumericGreaterThanSuccess"
                },
                {
                    "Variable": "$.numeric_less_than_equals",
                    "NumericLessThanEquals": 1234,
                    "Next": "NumericLessThanEqualsSuccess"
                },
                {
                    "Variable": "$.numeric_less_than_equals_path",
                    "NumericLessThanEqualsPath": "$.test_value",
                    "Next": "NumericLessThanEqualsSuccess"
                },
                {
                    "Variable": "$.numeric_greater_than_equals",
                    "NumericGreaterThanEquals": 1234,
                    "Next": "NumericGreaterThanEqualsSuccess"
                },
                {
                    "Variable": "$.numeric_greater_than_equals_path",
                    "NumericGreaterThanEqualsPath": "$.test_value",
                    "Next": "NumericGreaterThanEqualsSuccess"
                },
                {
                    "Variable": "$.boolean_equals",
                    "BooleanEquals": true,
                    "Next": "BooleanEqualsSuccess"
                },
                {
                    "Variable": "$.boolean_equals_path",
                    "BooleanEqualsPath": "$.test_value",
                    "Next": "BooleanEqualsSuccess"
                },
                {
                    "And": [
                        {
                            "Variable": "$.and_test_value",
                            "NumericGreaterThanEquals": 20
                        },
                        {
                            "Variable": "$.and_test_value",
                            "NumericLessThan": 30
                        }
                    ],
                    "Next": "ValueInTwenties"
                },
                {
                    "Or": [
                        {
                            "Variable": "$.animal",
                            "StringEquals": "cat"
                        },
                        {
                            "Variable": "$.animal",
                            "StringEquals": "dog"
                        }
                    ],
                    "Next": "IsPet"
                },
                {
                    "And": [
                        {
                            "Or": [
                                {
                                    "Variable": "$.name",
                                    "StringEquals": "shrek"
                                },
                                {
                                    "Variable": "$.name",
                                    "StringEquals": "donkey"
                                }
                            ]
                        },
                        {
                            "And": [
                                {
                                    "Variable": "$.age",
                                    "NumericGreaterThanEquals": 40
                                },
                                {
                                    "Variable": "$.age",
                                    "NumericLessThan": 60
                                }
                            ]

                        },
                        {
                            "Not": {
                                "Variable": "$.colour",
                                "StringEquals": "pink"
                            }
                        }
                    ],
                    "Next": "IsCharacter"
                },
                {
                    "Variable": "$.timestamp_equals",
                    "TimestampEquals": "2019-08-08T10:55:25.325038+01:00",
                    "Next": "TimestampEqualsSuccess"
                },
                {
                    "Variable": "$.timestamp_equals_path",
                    "TimestampEqualsPath": "$.test_value",
                    "Next": "TimestampEqualsSuccess"
                },
                {
                    "Variable": "$.timestamp_less_than",
                    "TimestampLessThan": "2019-08-08T10:55:25.325038+01:00",
                    "Next": "TimestampLessThanSuccess"
                },
                {
                    "Variable": "$.timestamp_less_than_path",
                    "TimestampLessThanPath": "$.test_value",
                    "Next": "TimestampLessThanSuccess"
                },
                {
                    "Variable": "$.timestamp_greater_than",
                    "TimestampGreaterThan": "2019-08-08T10:55:25.325038+01:00",
                    "Next": "TimestampGreaterThanSuccess"
                },
                {
                    "Variable": "$.timestamp_greater_than_path",
                    "TimestampGreaterThanPath": "$.test_value",
                    "Next": "TimestampGreaterThanSuccess"
                },
                {
                    "Variable": "$.timestamp_less_than_equals",
                    "TimestampLessThanEquals": "2019-08-08T10:55:25.325038+01:00",
                    "Next": "TimestampLessThanEqualsSuccess"
                },
                {
                    "Variable": "$.timestamp_less_than_equals_path",
                    "TimestampLessThanEqualsPath": "$.test_value",
                    "Next": "TimestampLessThanEqualsSuccess"
                },
                {
                    "Variable": "$.timestamp_greater_than_equals",
                    "TimestampGreaterThanEquals": "2019-08-08T10:55:25.325038+01:00",
                    "Next": "TimestampGreaterThanEqualsSuccess"
                },
                {
                    "Variable": "$.timestamp_greater_than_equals_path",
                    "TimestampGreaterThanEqualsPath": "$.test_value",
                    "Next": "TimestampGreaterThanEqualsSuccess"
                },
                {
                    "Variable": "$.is_boolean",
                    "IsBoolean": true,
                    "Next": "TypeTestSuccess"
                },
                {
                    "Variable": "$.is_null",
                    "IsNull": true,
                    "Next": "TypeTestSuccess"
                },
                {
                    "Variable": "$.is_numeric",
                    "IsNumeric": true,
                    "Next": "TypeTestSuccess"
                },
                {
                    "Variable": "$.is_string",
                    "IsString": true,
                    "Next": "TypeTestSuccess"
                },
                {
                    "Variable": "$.is_present",
                    "IsPresent": true,
                    "Next": "TypeTestSuccess"
                },
                {
                    "Variable": "$.is_timestamp",
                    "IsTimestamp": true,
                    "Next": "TypeTestSuccess"
                },
                {
                    "Not": {
                        "Variable": "$.not_string_equals",
                        "StringEquals": "hello world"
                    },
                    "Next": "NotStringEqualsSuccess"
                }
            ],
            "Default": "FailState"
        },
        "FailState": {
            "Type": "Fail",
            "Error": "NoMatchError",
            "Cause": "No Matches!"
        },
        "StringEqualsSuccess": {
            "Type": "Pass",
            "End": true
        },
        "StringLessThanSuccess": {
            "Type": "Pass",
            "End": true
        },
        "StringGreaterThanSuccess": {
            "Type": "Pass",
            "End": true
        },
        "StringLessThanEqualsSuccess": {
            "Type": "Pass",
            "End": true
        },
        "StringGreaterThanEqualsSuccess": {
            "Type": "Pass",
            "End": true
        },
        "StringMatchesSuccess": {
            "Type": "Pass",
            "End": true
        },
        "NumericEqualsSuccess": {
            "Type": "Pass",
            "End": true
        },
        "NumericLessThanSuccess": {
            "Type": "Pass",
            "End": true
        },
        "NumericGreaterThanSuccess": {
            "Type": "Pass",
            "End": true
        },
        "NumericLessThanEqualsSuccess": {
            "Type": "Pass",
            "End": true
        },
        "NumericGreaterThanEqualsSuccess": {
            "Type": "Pass",
            "End": true
        },
        "BooleanEqualsSuccess": {
            "Type": "Pass",
            "End": true
        },
        "ValueInTwenties": {
            "Type": "Pass",
            "End": true
        },
        "IsPet": {
            "Type": "Pass",
            "End": true
        },
        "IsCharacter": {
            "Type": "Pass",
            "End": true
        },
        "TimestampEqualsSuccess": {
            "Type": "Pass",
            "End": true
        },
        "TimestampLessThanSuccess": {
            "Type": "Pass",
            "End": true
        },
        "TimestampGreaterThanSuccess": {
            "Type": "Pass",
            "End": true
        },
        "TimestampLessThanEqualsSuccess": {
            "Type": "Pass",
            "End": true
        },
        "TimestampGreaterThanEqualsSuccess": {
            "Type": "Pass",
            "End": true
        },
        "NotStringEqualsSuccess": {
            "Type": "Pass",
            "End": true
        },
        "TypeTestSuccess": {
            "Type": "Pass",
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
        self.last_event = None # Retain last event so test can check its value

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
        self.last_event = item
        self.dispatch(json.dumps(item))
    
    def broadcast(self, subject, message, carrier_properties=None):
        pass

"""
This stubs out the real TaskDispatcher execute_task method which requires
messaging infrastructure to run whereas this test is just a state machine test.
"""
def execute_task_stub(resource_arn, parameters, callback, timeout, context, event_id, redelivered):
    name = resource_arn.split(":")[-1]
    result = {"reply": name + " reply"}
    callback(result)


class TestChoiceState(unittest.TestCase):

    def setUp(self):
        # Initialise logger
        logger = init_logging(log_name="test_choice_state")
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

    #---------- String ---------------------------------------------------------
    def test_string_equals(self):
        print("---------- test_string_equals ----------")
        item = '{"data": {"string_equals":"hello world"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_equals"], "hello world")
        self.assertEqual(state, "StringEqualsSuccess")

    def test_string_equals_path(self):
        print("---------- test_string_equals_path ----------")
        item = '{"data": {"string_equals_path":"hello world", "test_value":"hello world"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_equals_path"], "hello world")
        self.assertEqual(state, "StringEqualsSuccess")

    def test_case_insensitive_string_equals(self):
        # Case insensitive string comparison is not covered in the ASL spec.
        # but as it is useful and trivial to implement we may as well add it.
        print("---------- test_case_insensitive_string_equals ----------")
        item = '{"data": {"case_insensitive_string_equals":"Hello world"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["case_insensitive_string_equals"], "Hello world")
        self.assertEqual(state, "StringEqualsSuccess")

    def test_case_insensitive_string_equals_path(self):
        # Case insensitive string comparison is not covered in the ASL spec.
        # but as it is useful and trivial to implement we may as well add it.
        print("---------- test_case_insensitive_string_equals_path ----------")
        item = '{"data": {"case_insensitive_string_equals_path":"Hello world", "test_value":"hello world"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["case_insensitive_string_equals_path"], "Hello world")
        self.assertEqual(state, "StringEqualsSuccess")

    def test_string_less_than(self):
        print("---------- test_string_less_than ----------")
        item = '{"data": {"string_less_than":"amazon"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_less_than"], "amazon")
        self.assertEqual(state, "StringLessThanSuccess")

    def test_string_less_than_path(self):
        print("---------- test_string_less_than_path ----------")
        item = '{"data": {"string_less_than_path":"amazon", "test_value":"apple"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_less_than_path"], "amazon")
        self.assertEqual(state, "StringLessThanSuccess")

    def test_string_greater_than(self):
        print("---------- test_string_greater_than ----------")
        item = '{"data": {"string_greater_than":"amazon"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_greater_than"], "amazon")
        self.assertEqual(state, "StringGreaterThanSuccess")

    def test_string_greater_than_path(self):
        print("---------- test_string_greater_than_path ----------")
        item = '{"data": {"string_greater_than_path":"amazon", "test_value":"airbnb"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_greater_than_path"], "amazon")
        self.assertEqual(state, "StringGreaterThanSuccess")

    def test_string_less_than_equals(self):
        print("---------- test_string_less_than_equals ----------")
        item = '{"data": {"string_less_than_equals":"amazon"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_less_than_equals"], "amazon")
        self.assertEqual(state, "StringLessThanEqualsSuccess")

        item = '{"data": {"string_less_than_equals":"apple"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_less_than_equals"], "apple")
        self.assertEqual(state, "StringLessThanEqualsSuccess")

    def test_string_less_than_equals_path(self):
        print("---------- test_string_less_than_equals_path ----------")
        item = '{"data": {"string_less_than_equals_path":"amazon", "test_value":"apple"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_less_than_equals_path"], "amazon")
        self.assertEqual(state, "StringLessThanEqualsSuccess")

        item = '{"data": {"string_less_than_equals_path":"apple", "test_value":"apple"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_less_than_equals_path"], "apple")
        self.assertEqual(state, "StringLessThanEqualsSuccess")
    
    def test_string_greater_than_equals(self):
        print("---------- test_string_greater_than_equals ----------")
        item = '{"data": {"string_greater_than_equals":"amazon"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_greater_than_equals"], "amazon")
        self.assertEqual(state, "StringGreaterThanEqualsSuccess")

        item = '{"data": {"string_greater_than_equals":"airbnb"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_greater_than_equals"], "airbnb")
        self.assertEqual(state, "StringGreaterThanEqualsSuccess")

    def test_string_greater_than_equals_path(self):
        print("---------- test_string_greater_than_equals_path ----------")
        item = '{"data": {"string_greater_than_equals_path":"amazon", "test_value":"airbnb"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_greater_than_equals_path"], "amazon")
        self.assertEqual(state, "StringGreaterThanEqualsSuccess")

        item = '{"data": {"string_greater_than_equals_path":"airbnb", "test_value":"airbnb"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_greater_than_equals_path"], "airbnb")
        self.assertEqual(state, "StringGreaterThanEqualsSuccess")

    def test_string_matches(self):
        print("---------- test_string_matches ----------")
        item = '{"data": {"string_matches1":"foo23.log"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_matches1"], "foo23.log")
        self.assertEqual(state, "StringMatchesSuccess")

        item = '{"data": {"string_matches1":"fool.log"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_matches1"], "fool.log")
        self.assertEqual(state, "StringMatchesSuccess")

        item = '{"data": {"string_matches2":"foo23.log"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_matches2"], "foo23.log")
        self.assertEqual(state, "StringMatchesSuccess")

        item = '{"data": {"string_matches2":"zebra.log"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_matches2"], "zebra.log")
        self.assertEqual(state, "StringMatchesSuccess")

        item = '{"data": {"string_matches3":"foo23.log"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_matches3"], "foo23.log")
        self.assertEqual(state, "StringMatchesSuccess")

        item = '{"data": {"string_matches3":"foobar.zebra"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_matches3"], "foobar.zebra")
        self.assertEqual(state, "StringMatchesSuccess")

        # Test escape characters in value to match literal chars present in variable
        # In JSON the escaped string \\\\ represents \
        item = '{"data": {"string_matches4":"foo*[hello]\\\\test\\\\.log"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["string_matches4"], "foo*[hello]\\test\\.log")
        self.assertEqual(state, "StringMatchesSuccess")

    #---------- Numeric --------------------------------------------------------

    def test_numeric_equals(self):
        print("---------- test_numeric_equals ----------")
        item = '{"data": {"numeric_equals":1234}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["numeric_equals"], 1234)
        self.assertEqual(state, "NumericEqualsSuccess")

    def test_numeric_equals_path(self):
        print("---------- test_numeric_equals_path ----------")
        item = '{"data": {"numeric_equals_path":1234, "test_value":1234}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["numeric_equals_path"], 1234)
        self.assertEqual(state, "NumericEqualsSuccess")
    
    def test_numeric_less_than(self):
        print("---------- test_numeric_less_than ----------")
        item = '{"data": {"numeric_less_than":123}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["numeric_less_than"], 123)
        self.assertEqual(state, "NumericLessThanSuccess")

    def test_numeric_less_than_path(self):
        print("---------- test_numeric_less_than_path ----------")
        item = '{"data": {"numeric_less_than_path":123, "test_value":1234}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["numeric_less_than_path"], 123)
        self.assertEqual(state, "NumericLessThanSuccess")
    
    def test_numeric_greater_than(self):
        print("---------- test_numeric_greater_than ----------")
        item = '{"data": {"numeric_greater_than":12345}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["numeric_greater_than"], 12345)
        self.assertEqual(state, "NumericGreaterThanSuccess")

    def test_numeric_greater_than_path(self):
        print("---------- test_numeric_greater_than_path ----------")
        item = '{"data": {"numeric_greater_than_path":12345, "test_value":1234}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["numeric_greater_than_path"], 12345)
        self.assertEqual(state, "NumericGreaterThanSuccess")
    
    def test_numeric_less_than_equals(self):
        print("---------- test_numeric_less_than_equals ----------")
        item = '{"data": {"numeric_less_than_equals":1234}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["numeric_less_than_equals"], 1234)
        self.assertEqual(state, "NumericLessThanEqualsSuccess")

        item = '{"data": {"numeric_less_than_equals":123}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["numeric_less_than_equals"], 123)
        self.assertEqual(state, "NumericLessThanEqualsSuccess")

    def test_numeric_less_than_equals_path(self):
        print("---------- test_numeric_less_than_equals_path ----------")
        item = '{"data": {"numeric_less_than_equals_path":1234, "test_value":1234}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["numeric_less_than_equals_path"], 1234)
        self.assertEqual(state, "NumericLessThanEqualsSuccess")

        item = '{"data": {"numeric_less_than_equals_path":123, "test_value":1234}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["numeric_less_than_equals_path"], 123)
        self.assertEqual(state, "NumericLessThanEqualsSuccess")
    
    def test_numeric_greater_than_equals(self):
        print("---------- test_numeric_greater_than_equals ----------")
        item = '{"data": {"numeric_greater_than_equals":1234}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["numeric_greater_than_equals"], 1234)
        self.assertEqual(state, "NumericGreaterThanEqualsSuccess")

        item = '{"data": {"numeric_greater_than_equals":12345}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["numeric_greater_than_equals"], 12345)
        self.assertEqual(state, "NumericGreaterThanEqualsSuccess")

    def test_numeric_greater_than_equals_path(self):
        print("---------- test_numeric_greater_than_equals_path ----------")
        item = '{"data": {"numeric_greater_than_equals_path":1234, "test_value":1234}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["numeric_greater_than_equals_path"], 1234)
        self.assertEqual(state, "NumericGreaterThanEqualsSuccess")

        item = '{"data": {"numeric_greater_than_equals_path":12345, "test_value":1234}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["numeric_greater_than_equals_path"], 12345)
        self.assertEqual(state, "NumericGreaterThanEqualsSuccess")

    #---------- Boolean --------------------------------------------------------
    
    def test_boolean_equals(self):
        print("---------- test_boolean_equals ----------")
        # Note difference between JSON true and Python True
        item = '{"data": {"boolean_equals":true}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["boolean_equals"], True)
        self.assertEqual(state, "BooleanEqualsSuccess")

    def test_boolean_equals_path(self):
        print("---------- test_boolean_equals_path ----------")
        # Note difference between JSON true and Python True
        item = '{"data": {"boolean_equals_path":true, "test_value":true}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["boolean_equals_path"], True)
        self.assertEqual(state, "BooleanEqualsSuccess")

    def test_and(self):
        print("---------- test_and ----------")
        item = '{"data": {"and_test_value":22}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["and_test_value"], 22)
        self.assertEqual(state, "ValueInTwenties")

    def test_or(self):
        print("---------- test_or ----------")
        item = '{"data": {"animal":"cat"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["animal"], "cat")
        self.assertEqual(state, "IsPet")

        item = '{"data": {"animal":"dog"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["animal"], "dog")
        self.assertEqual(state, "IsPet")

    def test_complex_logic(self):
        print("---------- test_complex_logic ----------")
        item = '{"data": {"name":"shrek", "age":55, "colour":"green"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["name"], "shrek")
        self.assertEqual(state, "IsCharacter")

        item = '{"data": {"name":"donkey", "age":50, "colour":"grey"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["name"], "donkey")
        self.assertEqual(state, "IsCharacter")

    def test_not_string_equals(self):
        # In other words this is the != operator
        print("---------- test_not_string_equals ----------")
        item = '{"data": {"not_string_equals":"not hello world"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["not_string_equals"], "not hello world")
        self.assertEqual(state, "NotStringEqualsSuccess")

    def test_timestamp_equals(self):
        print("---------- test_timestamp_equals ----------")
        item = '{"data": {"timestamp_equals":"2019-08-08T10:55:25.325038+01:00"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["timestamp_equals"], "2019-08-08T10:55:25.325038+01:00")
        self.assertEqual(state, "TimestampEqualsSuccess")

        # This is the same time but represented in Zulu so test that matches too.
        item = '{"data": {"timestamp_equals":"2019-08-08T09:55:25.325038Z"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["timestamp_equals"], "2019-08-08T09:55:25.325038Z")
        self.assertEqual(state, "TimestampEqualsSuccess")

    def test_timestamp_equals_path(self):
        print("---------- test_timestamp_equals_path ----------")
        item = '{"data": {"timestamp_equals_path":"2019-08-08T10:55:25.325038+01:00", "test_value":"2019-08-08T10:55:25.325038+01:00"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["timestamp_equals_path"], "2019-08-08T10:55:25.325038+01:00")
        self.assertEqual(state, "TimestampEqualsSuccess")

        # This is the same time but represented in Zulu so test that matches too.
        item = '{"data": {"timestamp_equals_path":"2019-08-08T09:55:25.325038Z", "test_value":"2019-08-08T10:55:25.325038+01:00"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["timestamp_equals_path"], "2019-08-08T09:55:25.325038Z")
        self.assertEqual(state, "TimestampEqualsSuccess")
    
    def test_timestamp_less_than(self):
        print("---------- test_timestamp_less_than ----------")
        item = '{"data": {"timestamp_less_than":"2019-08-08T09:55:25.325038+01:00"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["timestamp_less_than"], "2019-08-08T09:55:25.325038+01:00")
        self.assertEqual(state, "TimestampLessThanSuccess")
    
    def test_timestamp_less_than_path(self):
        print("---------- test_timestamp_less_than_path ----------")
        item = '{"data": {"timestamp_less_than_path":"2019-08-08T09:55:25.325038+01:00", "test_value":"2019-08-08T10:55:25.325038+01:00"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["timestamp_less_than_path"], "2019-08-08T09:55:25.325038+01:00")
        self.assertEqual(state, "TimestampLessThanSuccess")

    def test_timestamp_greater_than(self):
        print("---------- test_timestamp_greater_than ----------")
        item = '{"data": {"timestamp_greater_than":"2019-08-08T11:55:25.325038+01:00"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["timestamp_greater_than"], "2019-08-08T11:55:25.325038+01:00")
        self.assertEqual(state, "TimestampGreaterThanSuccess")

    def test_timestamp_greater_than_path(self):
        print("---------- test_timestamp_greater_than_path ----------")
        item = '{"data": {"timestamp_greater_than_path":"2019-08-08T11:55:25.325038+01:00", "test_value":"2019-08-08T10:55:25.325038+01:00"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["timestamp_greater_than_path"], "2019-08-08T11:55:25.325038+01:00")
        self.assertEqual(state, "TimestampGreaterThanSuccess")
    
    def test_timestamp_less_than_equals(self):
        print("---------- test_timestamp_less_than_equals ----------")
        item = '{"data": {"timestamp_less_than_equals":"2019-08-08T10:55:25.325038+01:00"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["timestamp_less_than_equals"], "2019-08-08T10:55:25.325038+01:00")
        self.assertEqual(state, "TimestampLessThanEqualsSuccess")

        item = '{"data": {"timestamp_less_than_equals":"2019-08-08T09:55:25.325038+01:00"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["timestamp_less_than_equals"], "2019-08-08T09:55:25.325038+01:00")
        self.assertEqual(state, "TimestampLessThanEqualsSuccess")

    def test_timestamp_less_than_equals_path(self):
        print("---------- test_timestamp_less_than_equals_path ----------")
        item = '{"data": {"timestamp_less_than_equals_path":"2019-08-08T10:55:25.325038+01:00", "test_value":"2019-08-08T10:55:25.325038+01:00"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["timestamp_less_than_equals_path"], "2019-08-08T10:55:25.325038+01:00")
        self.assertEqual(state, "TimestampLessThanEqualsSuccess")

        item = '{"data": {"timestamp_less_than_equals_path":"2019-08-08T09:55:25.325038+01:00", "test_value":"2019-08-08T10:55:25.325038+01:00"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["timestamp_less_than_equals_path"], "2019-08-08T09:55:25.325038+01:00")
        self.assertEqual(state, "TimestampLessThanEqualsSuccess")

    def test_timestamp_greater_than_equals(self):
        print("---------- test_timestamp_greater_than_equals ----------")
        item = '{"data": {"timestamp_greater_than_equals":"2019-08-08T10:55:25.325038+01:00"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["timestamp_greater_than_equals"], "2019-08-08T10:55:25.325038+01:00")
        self.assertEqual(state, "TimestampGreaterThanEqualsSuccess")

        item = '{"data": {"timestamp_greater_than_equals":"2019-08-08T11:55:25.325038+01:00"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["timestamp_greater_than_equals"], "2019-08-08T11:55:25.325038+01:00")
        self.assertEqual(state, "TimestampGreaterThanEqualsSuccess")

    def test_timestamp_greater_than_equals_path(self):
        print("---------- test_timestamp_greater_than_equals_path ----------")
        item = '{"data": {"timestamp_greater_than_equals_path":"2019-08-08T10:55:25.325038+01:00", "test_value":"2019-08-08T10:55:25.325038+01:00"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["timestamp_greater_than_equals_path"], "2019-08-08T10:55:25.325038+01:00")
        self.assertEqual(state, "TimestampGreaterThanEqualsSuccess")

        item = '{"data": {"timestamp_greater_than_equals_path":"2019-08-08T11:55:25.325038+01:00", "test_value":"2019-08-08T10:55:25.325038+01:00"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["timestamp_greater_than_equals_path"], "2019-08-08T11:55:25.325038+01:00")
        self.assertEqual(state, "TimestampGreaterThanEqualsSuccess")

    #---------- Type Test ------------------------------------------------------

    def test_is_boolean(self):
        print("---------- test_is_boolean ----------")
        item = '{"data": {"is_boolean":false}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["is_boolean"], False)
        self.assertEqual(state, "TypeTestSuccess")

    def test_is_null(self):
        print("---------- test_is_null ----------")
        item = '{"data": {"is_null":null}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["is_null"], None)
        self.assertEqual(state, "TypeTestSuccess")

    def test_is_numeric(self):
        print("---------- test_is_numeric ----------")
        item = '{"data": {"is_numeric":1234}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["is_numeric"], 1234)
        self.assertEqual(state, "TypeTestSuccess")

        item = '{"data": {"is_numeric":1234.5}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["is_numeric"], 1234.5)
        self.assertEqual(state, "TypeTestSuccess")

    def test_is_string(self):
        print("---------- test_is_string ----------")
        item = '{"data": {"is_string":"cat"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["is_string"], "cat")
        self.assertEqual(state, "TypeTestSuccess")

    def test_is_present(self):
        print("---------- test_is_present ----------")
        item = '{"data": {"is_present":{}}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["is_present"], {})
        self.assertEqual(state, "TypeTestSuccess")

        item = '{"data": {"is_present":false}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["is_present"], False)
        self.assertEqual(state, "TypeTestSuccess")

    def test_is_timestamp(self):
        print("---------- test_is_timestamp ----------")
        item = '{"data": {"is_timestamp":"2019-08-08T10:55:25.325038+01:00"}, "context": ' + context + '}'
        self.event_dispatcher.dispatch(item)
        data = self.event_dispatcher.last_event["data"]
        state = self.event_dispatcher.last_event["context"]["State"]["Name"]
        self.assertEqual(data["is_timestamp"], "2019-08-08T10:55:25.325038+01:00")
        self.assertEqual(state, "TypeTestSuccess")

if __name__ == '__main__':
    unittest.main()

