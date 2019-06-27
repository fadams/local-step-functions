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

# https://github.com/kennknowles/python-jsonpath-rw
# https://stackoverflow.com/questions/48629461/python-jsonpath-how-do-i-parse-with-jsonpath-correctly
# Tested using jsonpath_rw 1.4.0
from jsonpath_rw import parse # sudo pip3 install jsonpath_rw

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

    def notify(self, event, id):
        """
        :item event: Describes the data, current state and the ASL State Machine
        :type event: dict as described below.
        :item id: The ID of the event as given by the EventDispatcher, it is
         primarily used for acknowledging the event.
        :type id: A string representing the event ID, it may just be a number.

        {
	        "data": <Object representing application data>,
	        "context": <Object representing application context>,
        }

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

        #print(event)

        context = event["context"]
        state_machine = context["StateMachine"]

        # Get ASL from event/cache/storage
        """
        TODO abstract this into a separate class for handling caching/databasing
        of the ASL. The code below is overly simplistic and will likely go
        awry if multiple instances of the ASL Workflow Engine get started, as
        there is no file locking or any other concurrency protection.
        """
        state_machine_id = state_machine["Id"]
        print(state_machine_id)

        if state_machine_id in self.asl_cache:
            print("Using cached ASL")
            ASL = self.asl_cache[state_machine_id]
            state_machine["value"] = ASL
        else:
            ASL = state_machine["value"]
            self.asl_cache[state_machine_id] = ASL
            try:
                with open(self.asl_cache_file, 'w') as fp:
                    json.dump(self.asl_cache, fp)
                self.logger.info("Creating ASL Cache: {}".format(self.asl_cache_file))
            except IOError as e:
                raise

        """
        After the first state in the state machine has been processed the ASL
        should be cached, so if it was passed in the context it may be deleted
        as subsequent events only need to contain the ASL id, which should help
        keep the message size relatively modest.
        """
        del state_machine["value"]

        # Determine the current state from $$.State.Name.
        # TODO also set to ASL["StartAt"] if $$.State.Name is None or unset.
        current_state = context["State"]["Name"]
        current_state = ASL["StartAt"] if current_state == "" else current_state
        
        print("current_state = " + current_state)

        state = ASL["States"][current_state]
        data = event["data"]

        print("state = " + str(state))
        print("data = " + str(data))
        print(id)

        #-----------------------------------------------------------------------
        """
        Define nested functions as handlers for each supported ASL state type.
        Using nested functions so we can use the context extracted in notify.

        That the methods are prefixed with "asl_state_" as a mitigation against
        accidentally or deliberately placing an invalid State type in the ASL.
        """
        def asl_state_Pass():
            """
            The Pass State (identified by "Type":"Pass") simply passes its input
            to its output, performing no work. Pass States are useful when
            constructing and debugging state machines.
    
            A Pass State MAY have a field named “Result”. If present, its value
            is treated as the output of a virtual task, and placed as prescribed
            by the “ResultPath” field, if any, to be passed on to the next state.
            If “Result” is not provided, the output is the input. Thus if neither
            neither “Result” nor “ResultPath” are provided, the Pass state
            copies its input through to its output.
            """
            print("PASS")
            #print(event)
            # TODO process Result/ResultPath field

            """
            When processing has completed set the event's new current state in
            $$.State.Name to the next state in the state machine then publish
            the event and acknowledge the current event.
            """
            if (state.get("End")):
                print("** END OF STATE MACHINE**")
                # TODO output results
            else:
                context["State"]["Name"] = state.get("Next")
                self.event_dispatcher.publish(event)

            self.event_dispatcher.acknowledge(id)

        def asl_state_Task():
            """
            The Task State (identified by "Type":"Task") causes the interpreter
            to execute the work identified by the state’s “Resource” field.

            A Task State MUST include a “Resource” field, whose value MUST be a
            URI that uniquely identifies the specific task to execute. The
            States language does not constrain the URI scheme nor any other part
            of the URI.

            Tasks can optionally specify timeouts. Timeouts (the “TimeoutSeconds”
            and “HeartbeatSeconds” fields) are specified in seconds and MUST be
            positive integers. If provided, the “HeartbeatSeconds” interval MUST
            be smaller than the “TimeoutSeconds” value.

            If not provided, the default value of “TimeoutSeconds” is 60.

            If the state runs longer than the specified timeout, or if more time
            than the specified heartbeat elapses between heartbeats from the task,
            then the interpreter fails the state with a States.Timeout Error Name.
            """
            print("TASK")
            print(event)

            # TODO




            """
            When processing has completed set the event's new current state in
            $$.State.Name to the next state in the state machine then publish
            the event and acknowledge the current event.
            """
            if (state.get("End")):
                print("** END OF STATE MACHINE**")
                # TODO output results
            else:
                context["State"]["Name"] = state.get("Next")
                self.event_dispatcher.publish(event)

            self.event_dispatcher.acknowledge(id)

        def asl_state_Choice():
            """
            A Choice state (identified by "Type":"Choice") adds branching logic
            to a state machine.
            """

            """
            The choose function implements the actual choice logic. We must
            first extract the Variable field and use its value as JSONPath to
            scan the input data for the actual value the we wish to match.
            """
            def choose(choice):
                variable_field = choice.get("Variable")
                print("Variable field = " + str(variable_field))

                json_tag_val = parse(variable_field).find(data)          
                #print(json_tag_val)
                variable =  json_tag_val[0].value

                #const variable = choice.Variable && jp.value(this.input, choice.Variable);

                print("Variable value = " + variable)

                next = choice.get("Next", True)


                def asl_choice_And():
                    # TODO Test me
                    # Javascript if (choice.And.every(ch => this.process(ch)))
                    if all(choose(ch) for ch in choice): return next
                def asl_choice_Or():
                    # TODO Test me
                    # Javascript if (choice.Or.some(ch => this.process(ch)))
                    if any(choose(ch) for ch in choice): return next
                def asl_choice_Not():
                    # TODO Test me
                    # if (!this.process(choice.Not))
                    if (not this.choose(choice["Not"])): return next

                def asl_choice_BooleanEquals():
                    if (variable == choice["BooleanEquals"]): return next
                def asl_choice_NumericEquals():
                    if (variable == choice["NumericEquals"]): return next
                def asl_choice_NumericGreaterThan():
                    if (variable > choice["NumericGreaterThan"]): return next
                def asl_choice_NumericGreaterThanEquals():
                    if (variable >= choice["NumericGreaterThanEquals"]): return next
                def asl_choice_NumericLessThan():
                    if (variable < choice["NumericLessThan"]): return next
                def asl_choice_NumericLessThanEquals():
                    if (variable <= choice["NumericLessThanEquals"]): return next
                def asl_choice_StringEquals():
                    if (variable == choice["StringEquals"]): return next
                def asl_choice_StringGreaterThan():
                    if (variable > choice["StringGreaterThan"]): return next
                def asl_choice_StringGreaterThanEquals():
                    if (variable >= choice["StringGreaterThanEquals"]): return next
                def asl_choice_StringLessThan():
                    if (variable < choice["StringLessThan"]): return next
                def asl_choice_StringLessThanEquals():
                    if (variable <= choice["StringLessThanEquals"]): return next
                def asl_choice_TimestampEquals():
                    if (variable == choice["TimestampEquals"]): return next
                def asl_choice_TimestampGreaterThan():
                    if (variable > choice["TimestampGreaterThan"]): return next
                def asl_choice_TimestampGreaterThanEquals():
                    if (variable >= choice["TimestampGreaterThanEquals"]): return next
                def asl_choice_TimestampLessThan():
                    if (variable < choice["TimestampLessThan"]): return next
                def asl_choice_TimestampLessThanEquals():
                    if (variable <= choice["TimestampLessThanEquals"]): return next

                for key in choice:
                    """
                    Determine the ASL choice operator of the current choice and
                    use that to dynamically invoke the appropriate choice handler.
                    """
                    next_state = locals().get("asl_choice_" + key, lambda: None)()
                    print("Key: " + key + " ------------------------- next_state = " + str(next_state))
                    if next_state: return next_state

            """
            A Choice state state MUST have a “Choices” field whose value is a
            non-empty array. Each element of the array is called a Choice Rule -
            an object containing a comparison operation and a “Next” field,
            whose value MUST match a state name.
            """
            choices = state.get("Choices", []) # Sets to [] if key not present
            
            """
            The interpreter attempts pattern-matches against the Choice Rules in
            array order and transitions to the state specified in the “Next”
            field on the first Choice Rule where there is an exact match between
            the input value and a member of the comparison-operator array.
            """
            for choice in choices:
                next_state = choose(choice)
                if next_state: break

            """
            Choice states MAY have a “Default” field, which will execute if none
            of the Choice Rules match. Using state.get("Default") will set the
            value to None if the "Default" field is not present.
            """
            next_state = next_state if next_state else state.get("Default") 

            print("-------- next_state = " + str(next_state))

            """
            The interpreter will raise a run-time States.NoChoiceMatched error
            if a “Choice” state fails to match a Choice Rule and no “Default”
            transition was specified. 
            """
            if next_state:
                context["State"]["Name"] = next_state
                self.event_dispatcher.publish(event)
            else:
                self.logger.error("States.NoChoiceMatched: {}".format(json.dumps(context)))
                # TODO actually emit/publish an error to the caller when the
                # mechanism for returning data/errors has been determined.

            self.event_dispatcher.acknowledge(id)

        def asl_state_Wait():
            """
            A Wait state (identified by "Type":"Wait") causes the interpreter
            to delay the machine from continuing for a specified time.
            """
            print("WAIT")
            print(event)

            """
            It's important for this function to be nested as we want the event,
            state and id to be wrapped in its closure to be used when the
            timeout actually fires.
            """
            def on_timeout():
                print("----- TIMEOUT ----- id = " + str(id))
                """
                When processing has completed set the event's new current state
                in $$.State.Name to the next state in the state machine then
                publish the event and acknowledge the current event.
                """
                if (state.get("End")):
                    print("** END OF STATE MACHINE**")
                    # TODO output results
                else:
                    context["State"]["Name"] = state.get("Next")
                    self.event_dispatcher.publish(event)

                self.event_dispatcher.acknowledge(id)

            """
            The time can be specified as a wait duration, specified in seconds,
            or an absolute expiry time, specified as an ISO-8601 extended offset
            date-time format string.

            The wait duration does not need to be hardcoded and may also be a
            Reference Path to the data such as "TimestampPath": "$.expirydate"
    
            A Wait state MUST contain exactly one of ”Seconds”, “SecondsPath”,
            “Timestamp”, or “TimestampPath”.
            """
            seconds = state.get("Seconds")
            seconds_path = state.get("SecondsPath")
            timestamp = state.get("Timestamp")
            timestamp_path = state.get("TimestampPath")
            if seconds:
                timeout = seconds * 1000
            elif seconds_path:
                # TODO - should just be a JSONPath parse
                timeout = 1 # seconds_path * 1000
            elif timestamp:
                # TODO - Need to subtract current time from timestamp to
                # find timeout duration
                timeout = 1 # timestamp * 1000
            elif timestamp_path:
                # TODO - should just be a JSONPath parse to get Timestamp
                # TODO - Need to subtract current time from timestamp to
                # find timeout duration
                timeout = 1 # timestamp_path * 1000

            """
            Schedule the timeout. This is slightly subtle, the idea is that
            the event instance, state and id for this call are wrapped in the
            on_timeout function's closure, so when the timeout fires the correct
            event should be published and the correct id acknowledged.
            """
            self.event_dispatcher.set_timeout(on_timeout, timeout)
     
        def asl_state_Succeed():
            """
            The Succeed State (identified by "Type":"Succeed") terminates a state
            machine successfully. The Succeed State is a useful target for Choice-
            state branches that don't do anything but terminate the machine.
    
            Because Succeed States are terminal states, they have no “Next” field.
            """
            print("SUCCEED")
            print(event)


            self.event_dispatcher.acknowledge(id)

        def asl_state_Fail():
            """
            The Fail State (identified by "Type":"Fail") terminates the machine
            and marks it as a failure.
    
            A Fail State MUST have a string field named “Error”, used to provide
            an error name that can be used for error handling (Retry/Catch),
            operational, or diagnostic purposes. A Fail State MUST have a string
            field named “Cause”, used to provide a human-readable message.
    
            Because Fail States are terminal states, they have no “Next” field.
            """
            print("FAIL")
            print(event)


            self.event_dispatcher.acknowledge(id)

        def asl_state_Parallel():
            """
            """
            # TODO

            print("PARALLEL")
            print(event)


            self.event_dispatcher.acknowledge(id)

        """
        End of nested state handler functions.
        """
        #-----------------------------------------------------------------------

        """
        Determine the ASL state type of the current state and use that to
        dynamically invoke the appropriate ASL state handler given state type.
        The lambda provides a default handler in case of malformed ASL. 
        """
        state_type = ASL["States"][current_state]["Type"]
        locals().get("asl_state_" + state_type,
                     lambda:
                        self.logger.error("StateEngine illegal state transition: {}".
                        format(state_type)))()

