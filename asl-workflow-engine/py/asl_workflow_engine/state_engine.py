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

import json, time, uuid

from datetime import datetime, timezone, timedelta
from asl_workflow_engine.state_engine_paths import apply_jsonpath, \
                                                   apply_resultpath, \
                                                   evaluate_parameters

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.task_dispatcher import TaskDispatcher

from asl_workflow_engine.asl_exceptions import *
from asl_workflow_engine.arn import *


def parse_rfc3339_datetime(rfc3339):
    """
    Parse an RFC3339 (https://www.ietf.org/rfc/rfc3339.txt) format string into
    a datetime object which is essentially the inverse operation to
    datetime.now(timezone.utc).astimezone().isoformat()
    We primarily need this in the Wait state so we can compute timeouts etc.
    """
    #rfc3339 = rfc3339.strip() # Remove any leading/trailing whitespace
    if rfc3339[-1] == "Z":
        date = rfc3339[:-1]
        offset = "+00:00"
    else:
        date = rfc3339[:-6]
        offset = rfc3339[-6:]

    if "." not in date: date = date + ".0"
    raw_datetime = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f')
    delta = timedelta(hours=int(offset[-5:-3]), minutes=int(offset[-2]))
    if offset[0] == "-": delta = -delta
    return raw_datetime.replace(tzinfo=timezone(delta))


"""
TODO move ReplicatedDict to its own file and start to look at actual
replication. Current train of thought is to use messaging fabric such that
updates are published as messages and all servers in the cluster subscribe
to updates. The exact mechanism needs some thought. Also some use cases
such as state machines write infrequently but read often so replication on
update is likely best in that case, however other use cases such as getting
execution history might read infrequently, but execution updates happen every
state change so in those cases it may be better to have each node in the
cluster maintain their own "chunk" and only achieve consistency on read.
Lost to think about to do a nice implementation, but not urgent yet - just
things to bear in mind so other decisions don't make clustering harder.
"""
import collections

"""
This class implements dict semantics, but uses a messaging fabric to replicate
CRUD operations across instances and stores to a "transaction log" on the
master instance in order to persist any required changes.

https://stackoverflow.com/questions/3387691/how-to-perfectly-override-a-dict

TODO focussing on storage ATM need to look at replication stuff soon.
"""
class ReplicatedDict(collections.MutableMapping):
    def __init__(self, config, *args, **kwargs):
        self.logger = init_logging(log_name='asl_workflow_engine')
        self.logger.info("Creating ReplicatedDict")

        self.asl_cache_file = config["asl_cache"]
        #print("self.asl_cache_file = " + self.asl_cache_file)

        try:
            with open(self.asl_cache_file, 'r') as fp:
                self.store = json.load(fp)
            self.logger.info("ReplicatedDict loading: {}".format(self.asl_cache_file))
        except IOError as e:
            self.store = {}
        except ValueError as e:
            self.logger.warning("ReplicatedDict {} does not contain valid JSON".format(self.asl_cache_file))
            self.store = {}

        #self.store = dict()
        #self.update(dict(*args, **kwargs))  # use the free update to set keys

    def __str__(self):
        return str(self.store)

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        self.store[key] = value
        try:
            with open(self.asl_cache_file, 'w') as fp:
                json.dump(self.store, fp)
            self.logger.info("Updating ReplicatedDict: {}".format(self.asl_cache_file))
        except IOError as e:
            raise

    def __delitem__(self, key):
        del self.store[key]
        try:
            with open(self.asl_cache_file, 'w') as fp:
                json.dump(self.store, fp)
            self.logger.info("Updating ReplicatedDict: {}".format(self.asl_cache_file))
        except IOError as e:
            raise

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)






class StateEngine(object):

    def __init__(self, config):
        """
        """
        self.logger = init_logging(log_name='asl_workflow_engine')
        self.logger.info("Creating StateEngine")

        """
        Holds a cache of ASL objects keyed by the ARN of the ASL State Machine.
        This behaves like a dict semantically, but replicates the contents
        across all instances in the cluster so that we can horizontally scale.
        """
        self.asl_cache = ReplicatedDict(config["state_engine"])
        self.task_dispatcher = TaskDispatcher(self, config)
        # self.event_dispatcher set by EventDispatcher

    def change_state(self, state_type, next_state, event):
        """
        Set event's new current state in $$.State.Name to Next state.
        """
        data = event["data"]
        context = event["context"]
        state = context["State"]
        self.update_execution_history(context["Execution"]["Id"],
                                      state_type + "StateExited",
                                      {"output": data, "name": state["Name"]})
        # Update the state
        state["Name"] = next_state
        # https://stackoverflow.com/questions/8556398/generate-rfc-3339-timestamp-in-python
        state["EnteredTime"] = datetime.now(timezone.utc).astimezone().isoformat()
        self.event_dispatcher.publish(event)

    def end_state_machine(self, state_type, event):
        """
        End the state machine execution and update execution metadata.
        """
        data = event["data"]
        context = event["context"]
        state = context["State"]
        self.update_execution_history(context["Execution"]["Id"],
                                      state_type + "StateExited",
                                      {"output": data, "name": state["Name"]})
        update_type = "ExecutionSucceeded"
        if data.get("Error"): update_type = "ExecutionFailed"
        self.update_execution_history(context["Execution"]["Id"],
                                      update_type,
                                      {"output": data})

    def update_execution_history(self, execution_id, update_type, details):
        """
        This will be eventually used to store the execution information in a
        way that will be accessible to the REST API, but initially just log it.
        """
        # TODO logging the details object is likely to be expensive and in any
        # case the idea is to get execution history via the REST API
        # https://docs.aws.amazon.com/step-functions/latest/apireference/API_GetExecutionHistory.html
        # Probably need to be more selective about what is logged here.
        self.logger.info("{} {} {}".format(execution_id, update_type, details))

    def heartbeat(self):
        print("StateEngine heartbeat")
        self.task_dispatcher.heartbeat()

    def notify(self, event, id):
        """
        :item event: Describes the data, current state and the ASL State Machine
        :type event: dict as described below. N.B. note that the expected type
         is a dictionary i.e. a JSON object and NOT a JSON string!
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
		        "Definition": <Object representing ASL state machine>
	        },
	        "Task": {
		        "Token": <String>
	        }
        }

        The most important paths for state traversal are:
        $$.State.Name = the current state
        $$.StateMachine.Definition = (optional) contains the complete ASL state machine
        $$.StateMachine.Id = a unique reference to an ASL state machine
        """
        context = event.get("context")
        if context == None:
            self.logger.error("StateEngine: event {} has no $.context field, dropping the message!".format(event))
            self.event_dispatcher.acknowledge(id)
            return

        state_machine = context.get("StateMachine")
        if state_machine == None:
            self.logger.error("StateEngine: event {} has no $.context.StateMachine field, dropping the message!".format(event))
            self.event_dispatcher.acknowledge(id)
            return

        state_machine_id = state_machine.get("Id")
        if not state_machine_id:
            self.logger.error("StateEngine: event {} has no $.context.StateMachine.Id field, dropping the message!".format(event))
            self.event_dispatcher.acknowledge(id)
            return

        """
        If ASL is present in optional state_machine["Definition"] store that as
        if it were a CreateStateMachine API call. TODO the roleArn added here is
        not a valid IAM role ARN. This way of adding State Nachines "by value"
        embedded in the context was mainly added to enable development of some
        features prior to the addition of the REST API, so it might eventually
        be deprecated, alternatively the approach could be enhanced to allow
        a valid roleArn to be added from config or indeed be embedded in the
        context in a similar way to how state_machine["Definition"] is.
        """
        if "Definition" in state_machine:
            arn = parse_arn(state_machine_id)
            self.asl_cache[state_machine_id] = {
                "name": arn["resource"],
                "definition": state_machine["Definition"],
                "creationDate": time.time(),
                "roleArn": "arn:aws:iam:::role/dummy-role/dummy"
            }
            del state_machine["Definition"]

        asl_item = self.asl_cache.get(state_machine_id, {})
        ASL = asl_item.get("definition")
        if not ASL:
            """
            Dropping the message is possibly not the right thing to do in this
            scenario as we could simply add the State Machine and retry. OTOH
            if a State Machine is deleted executions should also be deleted.
            """
            self.logger.error("StateEngine: State Machine {} does not exist, dropping the message!".format(state_machine_id))
            self.event_dispatcher.acknowledge(id)
            return

        """
        https://states-language.net/spec.html#data
        https://docs.aws.amazon.com/step-functions/latest/dg/concepts-state-machine-data.html

        The interpreter passes data between states to perform calculations or to
        dynamically control the state machine’s flow. All such data MUST be
        expressed in JSON.

        When a state machine is started, the caller can provide an initial JSON
        text as input, which is passed to the machine's start state as input. If
        no input is provided, the default is an empty JSON object, {}. As each
        state is executed, it receives a JSON text as input and can produce
        arbitrary output, which MUST be a JSON text. When two states are linked
        by a transition, the output from the first state is passed as input to
        the second state. The output from the machine's terminal state is
        treated as its output. 
        """
        data = event.get("data", {}) # Note default to empty JSON object


        # Determine the current state from $$.State.Name.
        # Use get() defaults to cater for State or Name being initially unset.
        if not context.get("State"): context["State"] = {"Name": None}
        current_state = context["State"].get("Name")

        if not current_state: # Start state. Initialise unset context fields.
            current_state = ASL["StartAt"]
            if context.get("Execution") == None: context["Execution"] = {}
            execution = context["Execution"]

            if not execution.get("Name"): execution["Name"] = str(uuid.uuid4())

            if not execution.get("Id"):
                # Create Id
                # Form executionArn from stateMachineArn and uuid
                arn = parse_arn(state_machine_id)
                execution_arn = create_arn(service="states",
                                           region=arn.get("region", "local"),
                                           account=arn["account"], 
                                           resource_type="execution",
                                           resource=arn["resource"] + ":" +     
                                                    execution["Name"])
                execution["Id"] = execution_arn
            # execution["Input"] holds the initial Step Function input
            if execution.get("Input") == None: execution["Input"] = data

            if not execution.get("RoleArn"):
                execution["RoleArn"] = asl_item["roleArn"]

            # https://stackoverflow.com/questions/8556398/generate-rfc-3339-timestamp-in-python
            start_time = datetime.now(timezone.utc).astimezone().isoformat()
            if execution.get("StartTime") == None:
                execution["StartTime"] = start_time

            """
            For the start state set EnteredTime here, which will be reset if
            the message gets redelivered, for other state transitions we will
            set EnteredTime when the transition occurs so if redeliveries
            occur the EnteredTime will reflect the time the state was entered
            not the time the redelivery occurred.
            """
            if context["State"].get("EnteredTime") == None:
                context["State"]["EnteredTime"] = start_time

            self.update_execution_history(execution["Id"],
                                          "ExecutionStarted",
                                          {"input": data, "roleArn": execution["RoleArn"]})


        state = ASL["States"].get(current_state)
        if state == None:
            self.logger.error("StateEngine: State {} does not exist, dropping the message!".format(current_state))
            self.event_dispatcher.acknowledge(id)
            return

        # Determine the ASL state type of the current state.
        state_type = state["Type"]
    
        """
        print("state_machine_id = " + state_machine_id)
        print("current_state = " + current_state)
        print("state = " + str(state))
        print("data = " + str(data))
        print("event id = " + str(id))
        """

        #-----------------------------------------------------------------------
        """
        Define nested functions as handlers for each supported ASL state type.
        Using nested functions so we can use the context extracted in notify.

        That the methods are prefixed with "asl_state_" is a mitigation against
        accidentally or deliberately placing an invalid State type in the ASL.
        """
        def asl_state_Pass():
            """
            https://states-language.net/spec.html#pass-state
            https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-pass-state.html

            The Pass State (identified by "Type":"Pass") simply passes its input
            to its output, performing no work. Pass States are useful when
            constructing and debugging state machines.
            """
            input = apply_jsonpath(data, state.get("InputPath", "$"))

            """
            https://states-language.net/spec.html#parameters

            If the “Parameters” field is provided, its value, after the
            extraction and embedding, becomes the effective input.
            """
            parameters = evaluate_parameters(input, context, state.get("Parameters"))
            #print(parameters)

            """
            A Pass State MAY have a field named “Result”. If present, its value
            is treated as the output of a virtual task, and placed as prescribed
            by the “ResultPath” field, if any, to be passed on to the next state.

            If “Result” is not provided, the output is the input. Thus if neither
            neither “Result” nor “ResultPath” are provided, the Pass state
            copies its input through to its output.
            """
            result = state.get("Result", parameters) # Default is effective input as per spec.

            # Pass state applies ResultPath to "raw input"
            output = apply_resultpath(data, result, state.get("ResultPath", "$"))
            event["data"] = apply_jsonpath(output, state.get("OutputPath", "$"))

            if (state.get("End")):
                self.end_state_machine(state_type, event)
            else:
                self.change_state(state_type, state.get("Next"), event)

            self.event_dispatcher.acknowledge(id)

        def asl_state_Task():
            """
            https://states-language.net/spec.html#task-state
            https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-task-state.html

            TODO Timeouts & Hearbeats:
            Tasks can optionally specify timeouts. Timeouts (the “TimeoutSeconds”
            and “HeartbeatSeconds” fields) are specified in seconds and MUST be
            positive integers. If provided, the “HeartbeatSeconds” interval MUST
            be smaller than the “TimeoutSeconds” value.

            If not provided, the default value of “TimeoutSeconds” is 60.

            If the state runs longer than the specified timeout, or if more time
            than the specified heartbeat elapses between heartbeats from the task,
            then the interpreter fails the state with a States.Timeout Error Name.
            """
            #print("TASK")
            #print(state)
            #print(event)

            """
            It's important for this function to be nested as we want the event,
            state and id to be wrapped in its closure, to be used when the
            service integrated to the Task *actually* returns its result.
            """
            def on_response(result):
                # Task state applies ResultPath to "raw input"
                output = apply_resultpath(data, result, state.get("ResultPath", "$"))
                event["data"] = apply_jsonpath(output, state.get("OutputPath", "$"))

                if (state.get("End")):
                    self.end_state_machine(state_type, event)
                else:
                    self.change_state(state_type, state.get("Next"), event)

                self.event_dispatcher.acknowledge(id)


            input = apply_jsonpath(data, state.get("InputPath", "$"))

            """
            The Task State (identified by "Type":"Task") causes the interpreter
            to execute the work identified by the state’s “Resource” field.

            A Task State MUST include a “Resource” field, whose value MUST be a
            URI that uniquely identifies the specific task to execute. The
            States language does not constrain the URI scheme nor any other part
            of the URI.
            """
            resource = state.get("Resource")

            """
            https://states-language.net/spec.html#parameters

            If the “Parameters” field is provided, its value, after the
            extraction and embedding, becomes the effective input.
            """
            parameters = evaluate_parameters(input, context, state.get("Parameters"))
            #print(parameters)

            self.task_dispatcher.execute_task(resource, parameters, on_response)

        def asl_state_Choice():
            """
            https://states-language.net/spec.html#choice-state
            https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-choice-state.html

            A Choice state (identified by "Type":"Choice") adds branching logic
            to a state machine.

            TODO - Question, adding the option to perform a regex test would
            seem to be a useful thing to be able to do. This is not defined in
            the ASL specification but is useful so what should we do?
            

            InputPath & OutputPath are allowed (but unusual) in Choice states.
            https://states-language.net/spec.html#statetypes
            """
            input = apply_jsonpath(data, state.get("InputPath", "$"))

            """
            The choose function implements the actual choice logic. We must
            first extract the Variable field and use its value as JSONPath to
            scan the effective input for the actual value the we wish to match.
            """
            def choose(choice):
                # variable_field may be None for And, Or, Not choice rules
                variable_field = choice.get("Variable")
                variable = apply_jsonpath(input, variable_field)
                next = choice.get("Next", True)

                def isnumber(x):
                    # General test for numeric - any number x zero is zero
                    try:
                        return not isinstance(x, bool) and 0 == x*0
                    except:
                        return False
                    
                def asl_choice_And():
                    if all(choose(ch) for ch in choice["And"]): return next
                def asl_choice_Or():
                    if any(choose(ch) for ch in choice["Or"]): return next
                def asl_choice_Not():
                    if (not choose(choice["Not"])): return next

                def asl_choice_BooleanEquals():
                    value = choice["BooleanEquals"]
                    if isinstance(variable, bool) and isinstance(value, bool) and \
                    variable == value: return next

                def asl_choice_NumericEquals():
                    value = choice["NumericEquals"]
                    if isnumber(variable) and isnumber(value) and \
                    variable == value: return next
                def asl_choice_NumericGreaterThan():
                    value = choice["NumericGreaterThan"]
                    if isnumber(variable) and isnumber(value) and \
                    variable > value: return next
                def asl_choice_NumericGreaterThanEquals():
                    value = choice["NumericGreaterThanEquals"]
                    if isnumber(variable) and isnumber(value) and \
                    variable >= value: return next
                def asl_choice_NumericLessThan():
                    value = choice["NumericLessThan"]
                    if isnumber(variable) and isnumber(value) and \
                    variable < value: return next
                def asl_choice_NumericLessThanEquals():
                    value = choice["NumericLessThanEquals"]
                    if isnumber(variable) and isnumber(value) and \
                    variable <= value: return next

                def asl_choice_StringEquals():
                    value = choice["StringEquals"]
                    if isinstance(variable, str) and isinstance(value, str) and \
                    variable == value: return next
                def asl_choice_CaseInsensitiveStringEquals():
                    # Not covered in ASL spec. but useful and trivial to handle.
                    value = choice["CaseInsensitiveStringEquals"]
                    if isinstance(variable, str) and isinstance(value, str) and \
                    variable.lower() == value.lower(): return next
                def asl_choice_StringGreaterThan():
                    value = choice["StringGreaterThan"]
                    if isinstance(variable, str) and isinstance(value, str) and \
                    variable > value: return next
                def asl_choice_StringGreaterThanEquals():
                    value = choice["StringGreaterThanEquals"]
                    if isinstance(variable, str) and isinstance(value, str) and \
                    variable >= value: return next
                def asl_choice_StringLessThan():
                    value = choice["StringLessThan"]
                    if isinstance(variable, str) and isinstance(value, str) and \
                    variable < value: return next
                def asl_choice_StringLessThanEquals():
                    value = choice["StringLessThanEquals"]
                    if isinstance(variable, str) and isinstance(value, str) and \
                    variable <= value: return next

                """
                The ASL spec. is a little vague on timestamps. The approach we
                take here is to parse the rfc3339 string into an epoch timestamp
                and perform the comparisons on those. The thinking with that
                is because different rfc3339 representations can resolve to the
                same actual time e.g. a representation in Zulu or local time
                plus offset can both refer to the same time.
                """
                def asl_choice_TimestampEquals():
                    timestamp = parse_rfc3339_datetime(variable).timestamp()
                    value = parse_rfc3339_datetime(choice["TimestampEquals"]).timestamp()
                    if timestamp == value: return next
                def asl_choice_TimestampGreaterThan():
                    timestamp = parse_rfc3339_datetime(variable).timestamp()
                    value = parse_rfc3339_datetime(choice["TimestampGreaterThan"]).timestamp()
                    if timestamp > value: return next
                def asl_choice_TimestampGreaterThanEquals():
                    timestamp = parse_rfc3339_datetime(variable).timestamp()
                    value = parse_rfc3339_datetime(choice["TimestampGreaterThanEquals"]).timestamp()
                    if timestamp >= value: return next
                def asl_choice_TimestampLessThan():
                    timestamp = parse_rfc3339_datetime(variable).timestamp()
                    value = parse_rfc3339_datetime(choice["TimestampLessThan"]).timestamp()
                    if timestamp < value: return next
                def asl_choice_TimestampLessThanEquals():
                    timestamp = parse_rfc3339_datetime(variable).timestamp()
                    value = parse_rfc3339_datetime(choice["TimestampLessThanEquals"]).timestamp()
                    if timestamp <= value: return next

                for key in choice:
                    """
                    Determine the ASL choice operator of the current choice and
                    use that to dynamically invoke the appropriate choice handler.
                    """
                    try:
                        next_state = locals().get("asl_choice_" + key, lambda: None)()
                    except Exception:
                        next_state = None
                    
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

            event["data"] = apply_jsonpath(input, state.get("OutputPath", "$"))
            """
            The interpreter will raise a run-time States.NoChoiceMatched error
            if a “Choice” state fails to match a Choice Rule and no “Default”
            transition was specified. 
            """
            if next_state:
                self.change_state(state_type, next_state, event)
            else:
                self.logger.error("States.NoChoiceMatched: {}".format(json.dumps(context)))
                # TODO actually emit/publish an error to the caller when the
                # mechanism for returning data/errors has been determined.

            self.event_dispatcher.acknowledge(id)

        def asl_state_Wait():
            """
            https://states-language.net/spec.html#wait-state
            https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-wait-state.html

            A Wait state (identified by "Type":"Wait") causes the interpreter
            to delay the machine from continuing for a specified time.

            InputPath & OutputPath are allowed (but unusual) in Wait states.
            https://states-language.net/spec.html#statetypes. This is defined
            before on_timeout() so that input is captured in its closure.
            """
            input = apply_jsonpath(data, state.get("InputPath", "$"))

            """
            It's important for this function to be nested as we want the event,
            state and id to be wrapped in its closure, to be used when the
            timeout actually fires.
            """
            def on_timeout():
                event["data"] = apply_jsonpath(input, state.get("OutputPath", "$"))

                if (state.get("End")):
                    self.end_state_machine(state_type, event)
                else:
                    self.change_state(state_type, state.get("Next"), event)

                self.event_dispatcher.acknowledge(id)

            """
            The time can be specified as a wait duration, specified in seconds,
            or an absolute expiry time, specified as an ISO-8601 extended offset
            date-time format string.

            The wait duration does not need to be hardcoded and may also be a
            Reference Path to the effective input such as
            "TimestampPath": "$.expirydate"
    
            A Wait state MUST contain exactly one of ”Seconds”, “SecondsPath”,
            “Timestamp”, or “TimestampPath”.
            """
            seconds = state.get("Seconds")
            seconds_path = state.get("SecondsPath")
            timestamp = state.get("Timestamp")
            timestamp_path = state.get("TimestampPath")

            """
            For relative timeouts (seconds and seconds_path) we assume that the
            required delay is relative to the time the state was entered, so we
            add to the entry timestamp converted to epoch time and subtract
            current epoch time. For normal operation this should be extremely
            close to just using the seconds value, but for case where there is
            a backlog on the event queue or the state engine has died and the
            event has been redelivered to another instance then the actual delay
            could be somewhat less. The ASL specification says nothing about
            such a scenario, but it kind of seems logical for the wait time of
            a Wait state to be relative to when the state was actually entered.
            """
            entry_time = context["State"].get("EnteredTime")
            entry_timestamp = parse_rfc3339_datetime(entry_time).timestamp()
            if seconds:
                current_timestamp = time.time()
                timeout = (entry_timestamp + seconds - current_timestamp) * 1000
            elif seconds_path:
                seconds = apply_jsonpath(input, seconds_path)
                current_timestamp = time.time()
                timeout = (entry_timestamp + seconds - current_timestamp) * 1000
            elif timestamp:
                try:
                    target_timestamp = parse_rfc3339_datetime(timestamp).timestamp()
                    current_timestamp = time.time()
                    timeout = (target_timestamp - current_timestamp) * 1000
                except ValueError as e:
                    self.logger.warning("timestamp {} failed to parse correctly, defaulting to zero delay".format(timestamp))
                    timeout = 0
            elif timestamp_path:
                timestamp = apply_jsonpath(input, timestamp_path)
                try:
                    target_timestamp = parse_rfc3339_datetime(timestamp).timestamp()
                    current_timestamp = time.time()
                    timeout = (target_timestamp - current_timestamp) * 1000
                except ValueError as e:
                    self.logger.warning("timestamp {} failed to parse correctly, defaulting to zero delay".format(timestamp))
                    timeout = 0

            """
            Schedule the timeout. This is slightly subtle, the idea is that
            the event instance, state and id for this call are wrapped in the
            on_timeout function's closure, so when the timeout fires the correct
            event should be published and the correct id acknowledged.
            """
            self.event_dispatcher.set_timeout(on_timeout, timeout)
     
        def asl_state_Succeed():
            """
            https://states-language.net/spec.html#succeed-state
            https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-succeed-state.html

            The Succeed State (identified by "Type":"Succeed") terminates a state
            machine successfully. The Succeed State is a useful target for Choice-
            state branches that don't do anything but terminate the machine.
    
            Because Succeed States are terminal states, they have no “Next” field.
            
            InputPath & OutputPath are allowed (but unusual) in Succeed states.
            https://states-language.net/spec.html#statetypes
            """
            input = apply_jsonpath(data, state.get("InputPath", "$"))
            event["data"] = apply_jsonpath(input, state.get("OutputPath", "$"))

            self.end_state_machine(state_type, event)
            self.event_dispatcher.acknowledge(id)

        def asl_state_Fail():
            """
            https://states-language.net/spec.html#fail-state
            https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-fail-state.html

            The Fail State (identified by "Type":"Fail") terminates the machine
            and marks it as a failure.
        
            Because Fail States are terminal states, they have no “Next” field.

            A Fail State MUST have a string field named “Error”, used to provide
            an error name that can be used for error handling (Retry/Catch),
            operational, or diagnostic purposes. A Fail State MUST have a string
            field named “Cause”, used to provide a human-readable message.

            The spec. doesn't specify what happens if those fields are not set.
            """
            error = {
                "Error": state.get("Error", "Unspecified"),
                "Cause": state.get("Cause", "Unspecified")
            }

            # Fail states don't allow InputPath, OutputPath or ResultPath
            event["data"] = error
            self.end_state_machine(state_type, event)
            self.event_dispatcher.acknowledge(id)

        def asl_state_Parallel():
            """
            https://states-language.net/spec.html#parallel-state
            https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-parallel-state.html

            """
            # TODO

            print("PARALLEL - Not Yet Implemented")
            print(event)

            self.event_dispatcher.acknowledge(id)

        """
        End of nested state handler functions.
        """
        #-----------------------------------------------------------------------

        """
        Use the ASL state type of the current state to dynamically invoke the
        appropriate ASL state handler given state type. The (Python) lambda
        provides a default handler in case of malformed ASL. 
        """
        self.update_execution_history(context["Execution"]["Id"],
                                      state_type + "StateEntered",
                                      {"input": data, "name": current_state})
        locals().get("asl_state_" + state_type,
                     lambda:
                        self.logger.error("StateEngine illegal state transition: {}".
                        format(state_type)))()

