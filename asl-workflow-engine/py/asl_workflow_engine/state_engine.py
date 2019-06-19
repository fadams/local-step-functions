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

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import json

class StateEngine(object):

    def __init__(self, logger, config):
        """
        """
        self.logger = logger
        self.logger.info("Creating StateEngine")
        self.config = config["state_engine"] # TODO Handle missing config

        """
        Holds a cache of ASL objects keyed by ASLRef which represents the ARN
        of the ASL State Machine. TODO eventually the ASL should be stored in
        a key/value database of some sort, but for now we'll use a dictionary
        as a cache and store in a JSON file (TODO as well, but less TODO...)
        """
        self.asl_cache_file = self.config["asl_cache"]
        print("self.asl_cache_file = " + self.asl_cache_file)
        try:
            with open(self.asl_cache_file, 'r') as fp:
                self.asl_cache = json.load(fp)
            self.logger.info("StateEngine loading ASL Cache")
        except IOError as e:
            self.asl_cache = {}
        except ValueError as e:
            self.logger.error("StateEngine ASL Cache does not contain valid JSON")
            raise

    def notify(self, item, id):
        """
        :item item: Describes the Data, CurrentState and ASL State Machine
        :type item: dict as described below.

        {
	        "CurrentState": <String representing current state ID>,
	        "Data": <Object representing application data>,
	        "ASL": <Object representing ASL state machine>,
	        "ASLRef": <String representing unique ref to ASL>
        }
        """
        ASLRef = item["ASLRef"]
        print(ASLRef)

        """
        TODO abstract this into a separate class for handling caching/databasing
        of the ASL. The code below is overly simplistic and will likely go
        awry if multiple instances of the ASL Workflow Engine get started, as
        there is no file locking or any other concurrency protection.
        """
        if ASLRef in self.asl_cache:
            print("Using cached ASL")
            ASL = self.asl_cache[ASLRef]
            item["ASL"] = ASL
        else:
            ASL = item["ASL"]
            self.asl_cache[ASLRef] = ASL
            try:
                with open(self.asl_cache_file, 'w') as fp:
                    json.dump(self.asl_cache, fp)
                self.logger.info("Creating ASL Cache: {}".format(self.asl_cache_file))
            except IOError as e:
                raise

        # Determine the current state.
        current_state = item["CurrentState"]
        current_state = ASL["StartAt"] if current_state == "" else current_state
        
        print("current_state = " + current_state)

        """
        Determine the ASL state type of the current state and use that to
        dynamically invoke the appropriate ASL State Handler given state type.
        The lambda provides a default handler in case of malformed ASL. Note
        that the methods are prefixed with "asl_state_" as a mitigation against
        accidentally or deliberately placing an invalid State type in the ASL.
        """
        state_type = ASL["States"][current_state]["Type"]
        getattr(self, "asl_state_" + state_type, lambda item, id, current_state: self.logger.error("StateEngine illegal state transition: {}".format(state_type)))(item, id, current_state)

#        self.event_dispatcher.set_timeout(self.timeout, 500)
#        self.event_dispatcher.set_timeout(self.timeout1, 1000)


    def asl_state_Pass(self, item, id, current_state):
        """
        The Pass State (identified by "Type":"Pass") simply passes its input to
        its output, performing no work. Pass States are useful when constructing
        and debugging state machines.

        A Pass State MAY have a field named “Result”. If present, its value is
        treated as the output of a virtual task, and placed as prescribed by the
        “ResultPath” field, if any, to be passed on to the next state. If
        “Result” is not provided, the output is the input. Thus if neither
        “Result” nor “ResultPath” are provided, the Pass state copies its input
        through to its output.
        """
        print("PASS")

        """
        Retrieve the ASL from the item then delete as after the first state
        in the state machine has been processed the ASL should be cached so
        subsequent events only need to contain the ASLRef, which should help
        keep the message size relatively modest.
        """
        ASL = item["ASL"]
        del item["ASL"]

        state = ASL["States"][current_state]
        data = item["Data"]

        print(state)
        print(data)
        print(id)

        print(item)

        """
        When processing has completed set the item's new CurrentState to the
        next state in the state machine then publish the event and acknowledge
        the current event.
        """
        next_state = state["Next"]
        item["CurrentState"] = next_state
        self.event_dispatcher.publish(item)

        self.event_dispatcher.acknowledge(id)

    def asl_state_Task(self, item, id, current_state):
        """
        The Task State (identified by "Type":"Task") causes the interpreter to
        execute the work identified by the state’s “Resource” field.

        A Task State MUST include a “Resource” field, whose value MUST be a URI
        that uniquely identifies the specific task to execute. The States
        language does not constrain the URI scheme nor any other part of the URI.

        Tasks can optionally specify timeouts. Timeouts (the “TimeoutSeconds”
        and “HeartbeatSeconds” fields) are specified in seconds and MUST be
        positive integers. If provided, the “HeartbeatSeconds” interval MUST be
        smaller than the “TimeoutSeconds” value.

        If not provided, the default value of “TimeoutSeconds” is 60.

        If the state runs longer than the specified timeout, or if more time
        than the specified heartbeat elapses between heartbeats from the task,
        then the interpreter fails the state with a States.Timeout Error Name.
        """
        print("TASK")
        print(item)


        self.event_dispatcher.acknowledge(id)

    def asl_state_Choice(self, item, id, current_state):
        """
        A Choice state (identified by "Type":"Choice") adds branching logic to a
        state machine.

        A Choice state state MUST have a “Choices” field whose value is a non-
        empty array. Each element of the array is called a Choice Rule - an
        object containing a comparison operation and a “Next” field, whose value
        MUST match a state name.

        The interpreter attempts pattern-matches against the Choice Rules in
        array order and transitions to the state specified in the “Next” field
        on the first Choice Rule where there is an exact match between the input
        value and a member of the comparison-operator array.
        """
        print("CHOICE")
        print(item)


        self.event_dispatcher.acknowledge(id)

    def asl_state_Wait(self, item, id, current_state):
        """
        A Wait state (identified by "Type":"Wait") causes the interpreter to
        delay the machine from continuing for a specified time. The time can be
        specified as a wait duration, specified in seconds, or an absolute
        expiry time, specified as an ISO-8601 extended offset date-time format
        string.

        The wait duration does not need to be hardcoded and may also be a
        Reference Path to the data such as "TimestampPath": "$.expirydate"

        A Wait state MUST contain exactly one of ”Seconds”, “SecondsPath”,
        “Timestamp”, or “TimestampPath”.
        """
        print("WAIT")
        print(item)


        self.event_dispatcher.acknowledge(id)

    def asl_state_Succeed(self, item, id, current_state):
        """
        The Succeed State (identified by "Type":"Succeed") terminates a state
        machine successfully. The Succeed State is a useful target for Choice-
        state branches that don't do anything but terminate the machine.

        Because Succeed States are terminal states, they have no “Next” field.
        """
        print("SUCCEED")
        print(item)


        self.event_dispatcher.acknowledge(id)

    def asl_state_Fail(self, item, id, current_state):
        """
        The Fail State (identified by "Type":"Fail") terminates the machine and
        marks it as a failure.

        A Fail State MUST have a string field named “Error”, used to provide an
        error name that can be used for error handling (Retry/Catch),
        operational, or diagnostic purposes. A Fail State MUST have a string
        field named “Cause”, used to provide a human-readable message.

        Because Fail States are terminal states, they have no “Next” field.
        """
        print("FAIL")
        print(item)


        self.event_dispatcher.acknowledge(id)

    def asl_state_Parallel(self, item, id, current_state):
        """
        """
        print("PARALLEL")
        print(item)


        self.event_dispatcher.acknowledge(id)




    def timeout(self):
        print("----- TIMEOUT -----")
    def timeout1(self):
        print("----- TIMEOUT1 -----")

