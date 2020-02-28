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

"""
-------------------------------- READ ME FIRST ---------------------------------
Note that in JSON and dict manipulation herein there may be a mix of camel case
and fields starting with capitals. This is unfortunate, but somewhat deliberate
as we are trying to follow the patterns used in real AWS Step Functions, which
seems to use camel case in the REST API calls but in the Context object and
indeed in the ASL specification the fields start with capitals. Be aware of this
if suddenly overcome by the urge to "make everything consistent"
--------------------------------------------------------------------------------
"""

import sys
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3


import json, time, uuid, opentracing

from datetime import datetime, timezone, timedelta
from asl_workflow_engine.state_engine_paths import (
    apply_jsonpath,
    get_full_jsonpath,
    apply_resultpath,
    evaluate_parameters,
)

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.open_tracing_factory import span_context, inject_span
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
    rfc3339 = rfc3339.strip()  # Remove any leading/trailing whitespace
    if rfc3339[-1] == "Z":
        date = rfc3339[:-1]
        offset = "+00:00"
    else:
        date = rfc3339[:-6]
        offset = rfc3339[-6:]

    if "." not in date:
        date = date + ".0"
    raw_datetime = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f")
    delta = timedelta(hours=int(offset[-5:-3]), minutes=int(offset[-2]))
    if offset[0] == "-":
        delta = -delta
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
    def __init__(self, transaction_log_name, *args, **kwargs):
        self.logger = init_logging(log_name="asl_workflow_engine")
        self.logger.info("Creating ReplicatedDict")

        self.transaction_log = transaction_log_name
        # print("self.transaction_log = " + self.transaction_log)

        try:
            with open(self.transaction_log, "r") as fp:
                self.store = json.load(fp)
            self.logger.info(
                "ReplicatedDict loading: {}".format(self.transaction_log)
            )
        except IOError as e:
            self.store = {}
        except ValueError as e:
            self.logger.warning(
                "ReplicatedDict {} does not contain valid JSON".format(
                    self.transaction_log
                )
            )
            self.store = {}

        # self.store = dict()
        # self.update(dict(*args, **kwargs))  # use the free update to set keys

    def __str__(self):
        return str(self.store)

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        self.store[key] = value
        try:
            with open(self.transaction_log, "w") as fp:
                json.dump(self.store, fp)
            self.logger.info("Updating ReplicatedDict: {}".format(self.transaction_log))
        except IOError as e:
            raise

    def __delitem__(self, key):
        del self.store[key]
        try:
            with open(self.transaction_log, "w") as fp:
                json.dump(self.store, fp)
            self.logger.info("Updating ReplicatedDict: {}".format(self.transaction_log))
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
        self.logger = init_logging(log_name="asl_workflow_engine")
        self.logger.info("Creating StateEngine")

        """
        Holds a cache of ASL objects keyed by the ARN of the ASL State Machine.
        This behaves like a dict semantically, but replicates the contents
        across all instances in the cluster so that we can horizontally scale.
        """
        self.asl_cache = ReplicatedDict(config["state_engine"]["asl_cache"])

        """
        Holds information about the State Machine executions, that is to say
        each Step Function, so that we can do things like get execution history.
        """
        self.executions = {}

        """
        Holds the results of Parallel state Branches or Map state Iterations.
        It holds objects keyed by the execution ARN and each object is another
        dict which contains arrays keyed by the Parallel or Map state name.
        """
        self.branch_results = {}

        self.task_dispatcher = TaskDispatcher(self, config)
        # self.event_dispatcher set by EventDispatcher


    def broadcast_notification(self, execution_arn):
        """
        Broadcasts notification of execution status changes to a topic.

        The intention is to act rather like AWS CloudWatch events:
        https://docs.aws.amazon.com/step-functions/latest/dg/cw-events.html
        https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/CloudWatchEventsandEventPatterns.html
        The events are published to the topic/subject:
        asl_workflow_engine/<state_machine_arn>.<status>

        That is to say the asl_workflow_engine topic exchange with a subject
        comprising the state_machine_arn and the status separated by a dot. e.g. 
        asl_workflow_engine/arn:aws:states:local:0123456789:stateMachine:simple_state_machine.SUCCEEDED

        Consumers may subscribe to specific events using the full subject or
        groups of event using wildcards, for example the subscription:
        asl_workflow_engine/arn:aws:states:local:0123456789:stateMachine:simple_state_machine.*
        Subscribes to all notification events published for a given state machine.

        The format of the message body is as described in
        https://docs.aws.amazon.com/step-functions/latest/dg/cw-events.html
        the "detail" field gives access to the "executionArn", "stateMachineArn",
        "input", "output" and other relevant information and provides the same
        information as the DescribeExecution API.
        """
        
        # Look up executionArn
        match = self.executions.get(execution_arn)
        if not match:
            self.logger.info(
                "StateEngine: broadcast_notification: Execution {} does not exist".format(
                    execution_arn
                )
            )
            return

        detail = {
            "executionArn": execution_arn,
            "input": match["input"],
            "name": match["name"],
            "output": match["output"],
            "startDate": match["startDate"],
            "stateMachineArn": match["stateMachineArn"],
            "status": match["status"],
            "stopDate": match["stopDate"],
        }

        arn = parse_arn(execution_arn)
        region=arn.get("region", "local")
        account=arn["account"]

        # https://docs.aws.amazon.com/step-functions/latest/dg/cw-events.html
        # https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/CloudWatchEventsandEventPatterns.html

        # https://stackoverflow.com/questions/8556398/generate-rfc-3339-timestamp-in-python
        event_time = datetime.now(timezone.utc).astimezone().isoformat()
        cw_event = {
            "version": "0",  # By default, this is set to 0 (zero) in all events.
            "id": str(uuid.uuid4()),  # A unique value is generated for every event.
            "detail-type": "Step Functions Execution Status Change",
            "source": "aws.states",  # Identifies the service that sourced the event.
            "account": account,  # The 12-digit number identifying an AWS account.
            "time": event_time,  # Seems to be in rfc-3339 format in examples.
            "region": region,  # Identifies the AWS region where the event originated.
            "resources": [execution_arn],
            "detail": detail,
        }

        subject = detail["stateMachineArn"] + "." + detail["status"]
        self.event_dispatcher.broadcast(subject, cw_event)

    def start_state_machine(self, start_state, event):
        """
        Initialise the state machine execution's state, in particular this will
        set any context metadata that hasn't previously been set elsewhere.
        This is mostly only necessary if the execution was started in a
        "low-level" e.g. way by publishing directly to the event queue whereas
        if the execution was started via the REST API the context metadata should
        already be set and this method's purpose is then simply to update the
        execution history metadata.
        """
        data = event["data"]
        context = event["context"]
        state_machine_id = context["StateMachine"]["Id"]

        context["State"]["Name"] = start_state

        if context.get("Execution") == None:
            context["Execution"] = {}
        execution = context["Execution"]

        if not execution.get("Name"):
            execution["Name"] = str(uuid.uuid4())

        if not execution.get("Id"):
            # Create Id
            # Form executionArn from stateMachineArn and uuid
            arn = parse_arn(state_machine_id)
            execution_arn = create_arn(
                service="states",
                region=arn.get("region", "local"),
                account=arn["account"],
                resource_type="execution",
                resource=arn["resource"] + ":" + execution["Name"],
            )
            execution["Id"] = execution_arn
        # execution["Input"] holds the initial Step Function input
        if execution.get("Input") == None:
            execution["Input"] = data

        if not execution.get("RoleArn"):
            """
            The default dummy ARN case shouldn't generally occur unless
            the asl_cache "database" has been manually edited because when
            it is updated via the API the roleArn field gets populated.
            """
            asl_item = self.asl_cache.get(state_machine_id, {})
            execution["RoleArn"] = asl_item.get(
                "roleArn", "arn:aws:iam:::role/dummy-role/dummy"
            )

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
            
        self.executions[execution["Id"]] = {
            "input": data,
            "output": None,
            "name": execution["Name"],
            "startDate": time.time(),
            "stopDate": None,
            "stateMachineArn": state_machine_id,
            "status": "RUNNING",
            "history": [],
        }
        self.update_execution_history(
            execution["Id"],
            "ExecutionStarted",
            {"input": data, "roleArn": execution["RoleArn"]},
        )
        self.broadcast_notification(execution["Id"])

    def change_state(self, state_type, next_state, event):
        """
        Set event's new current state in $$.State.Name to Next state.
        """
        data = event["data"]
        context = event["context"]
        state = context["State"]
        self.update_execution_history(
            context["Execution"]["Id"],
            state_type + "StateExited",
            {"output": data, "name": state["Name"]},
        )
        # Update the state with the next state's name and erase any retry info.
        state["Name"] = next_state
        if "RetryCount" in state:
            del state["RetryCount"]
        if "RetryTimeout" in state:
            del state["RetryTimeout"]

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
        execution_arn = context["Execution"]["Id"]
        self.update_execution_history(
            execution_arn,
            state_type + "StateExited",
            {"output": data, "name": state["Name"]},
        )
        update_type = "ExecutionSucceeded"
        self.executions[execution_arn]["stopDate"] = time.time()

        with opentracing.tracer.start_active_span(
            operation_name="StartExecution:ExecutionEnding",
            child_of=span_context("text_map", context.get("Tracer", {}), self.logger),
            tags={
                "component": "state_engine",
                "execution_arn": context["Execution"]["Id"]
            }
        ) as scope:
            if isinstance(data, dict) and data.get("Error"):
                opentracing.tracer.active_span.set_tag("error", True)
                opentracing.tracer.active_span.log_kv(
                    {
                        "message": "ExecutionFailed",
                    }
                )
                update_type = "ExecutionFailed"
                opentracing.tracer.active_span.set_tag("status", "FAILED")
                self.executions[execution_arn]["status"] = "FAILED"
                self.executions[execution_arn]["output"] = None
            else:
                opentracing.tracer.active_span.set_tag("status", "SUCCEEDED")
                self.executions[execution_arn]["status"] = "SUCCEEDED"
                self.executions[execution_arn]["output"] = data
        
        self.update_execution_history(execution_arn, update_type, {"output": data})
        self.broadcast_notification(execution_arn)

    def update_execution_history(self, execution_arn, update_type, details):
        """
        Store the execution information in a way that will be accessible to the
        REST API.
        """
        # TODO logging the details object is likely to be expensive and in any
        # case the idea is to get execution history via the REST API
        # https://docs.aws.amazon.com/step-functions/latest/apireference/API_GetExecutionHistory.html
        # Probably need to be more selective about what is logged here.
        self.logger.info("{} {} {}".format(execution_arn, update_type, details))

        """
        self.executions.get(execution_arn) == None should only really happen if
        the StateEngine has failed and been restarted and we are handling a
        redelivered message. When we add code IDC to persist execution metadata
        state we should hopefully be able to avoid the following condition upon
        StateEngine restart.
        """
        if self.executions.get(execution_arn) == None:
            self.logger.warning(
                "StateEngine: update_execution_history: Execution {} does not "
                "exist, probably due to StateEngine restart. Some history "
                "metadata has been lost!".format(execution_arn))

            # Derive missing fields from execution_arn
            split = execution_arn.rpartition(':')
            state_machine_arn = split[0]
            name = split[2]

            self.executions[execution_arn] = {
                "input": None,
                "output": None,
                "name": name,
                "startDate": time.time(),
                "stopDate": None,
                "stateMachineArn": state_machine_arn,
                "status": "RUNNING",
                "history": []
            }

        history = self.executions[execution_arn]["history"]
        id = len(history) + 1
        if "StateEntered" in update_type:
            details_type = "stateEnteredEventDetails"
        elif "StateExited" in update_type:
            details_type = "stateExitedEventDetails"
        else:
            details_type = update_type[0].lower() + update_type[1:] + "EventDetails"

        history.append(
            {
                "timestamp": time.time(),
                details_type: details,
                "type": update_type,
                "id": id,
                "previousEventId": id - 1,
            }
        )

    def notify(self, event, id):
        """
        ------------------------------------------------------------------------
        This method is the main entry point to the StateEngine. It is called by
        the EventDispatcher when State events are available on the event queue.
        ------------------------------------------------------------------------

        :item event: Describes the data, current state and the ASL State Machine
        :type event: dict as described below. N.B. note that the expected type
         is a dictionary i.e. a JSON object and NOT a JSON string!
        :item id: The ID of the event as given by the EventDispatcher, it is
         primarily used for acknowledging the event.
        :type id: A string representing the event ID, it may just be a number.

        The event dict passed to the notify method has the following format:

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
            self.logger.error(
                "StateEngine: event {} has no $.context field, "
                "dropping the message!".format(
                    event
                )
            )
            self.event_dispatcher.acknowledge(id)
            return

        state_machine = context.get("StateMachine")
        if state_machine == None:
            self.logger.error(
                "StateEngine: event {} has no $.context.StateMachine field, "
                "dropping the message!".format(
                    event
                )
            )
            self.event_dispatcher.acknowledge(id)
            return

        state_machine_id = state_machine.get("Id")
        if not state_machine_id:
            self.logger.error(
                "StateEngine: event {} has no $.context.StateMachine.Id field, "
                "dropping the message!".format(
                    event
                )
            )
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
                "roleArn": "arn:aws:iam:::role/dummy-role/dummy",
            }
            del state_machine["Definition"]


        asl_item = self.asl_cache.get(state_machine_id, {})
        ASL = asl_item.get("definition")
        if not ASL:
            """
            Dropping the message is possibly not the right thing to do in this
            scenario as we *could* simply add the State Machine and retry. OTOH
            if a State Machine is deleted executions should also be deleted.
            """
            self.logger.error(
                "StateEngine: State Machine {} does not exist, "
                "dropping the message!".format(
                    state_machine_id
                )
            )
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
        data = event.get("data", {})  # Note default to empty JSON object


        # Determine the current state from $$.State.Name.
        # Use get() defaults to cater for State or Name being initially unset.
        if not context.get("State"):
            context["State"] = {"Name": None}
        current_state = context["State"].get("Name")

        if not current_state:
            """
            If current_state is uninitialised it means we are the Start State.
            If so initialise unset context fields and start OpenTracing span.
            """
            current_state = ASL["StartAt"]
            self.start_state_machine(current_state, event)

            with opentracing.tracer.start_active_span(
                operation_name="StartExecution:ExecutionStarting",
                child_of=span_context("text_map", context.get("Tracer", {}), self.logger),
                tags={
                    "component": "state_engine",
                    "execution_arn": context["Execution"]["Id"]
                }
            ) as scope:
                context["Tracer"] = inject_span("text_map", scope.span, self.logger)

        """
        States in any given “States” field can transition only to each other,
        and no state outside of that “States” field can transition into it.
        That is to say a parent state machine cannot transition directly to
        a state within a Parallel branch or Map Iterator state machine nor
        can states within those state machines directly transition to states
        outside of their own “States” field. We use current_state_machine to
        check that state transitions only occur within the correct "States".

        The parent is None if state is from the main parent state machine or
        is the Map or Parallel state that contains the nested state machine. 
        The parent_state is the name of the parent state described above.
        """
        parent_state = ""
        parent = None
        current_state_machine = ASL["States"]
        state = current_state_machine.get(current_state)
        if state == None:
            """
            If the state can't be found in the parent state machine search for
            it more deeply using recursive descent as the specified state might
            actually be in a Parallel branch or Map Iterator state machine.
            Because JSONPath doesn't have a parent operator and we want to get
            the parent States object and also the Map or Parallel state which
            contains that States object we get the full JSONPath string for the
            query then use simple string splits to find the paths of those.
            """
            path = get_full_jsonpath(current_state_machine, "$.." + current_state)
            states_path = path.rpartition("['States']")[0]
            parent_path = path.rpartition("['Iterator']")[0]
            if not parent_path:
                parent_path = path.rpartition("['Branches']")[0]

            if path and states_path and parent_path:
                parent = apply_jsonpath(current_state_machine, parent_path)
                parent_state = parent_path.rpartition("['")[2].partition("']")[0]
                branch = apply_jsonpath(current_state_machine, states_path)
                current_state_machine = branch["States"]
                state = current_state_machine.get(current_state)
            else:
                self.logger.error(
                    "StateEngine: State {} does not exist, dropping the message!".format(
                        current_state
                    )
                )
                self.event_dispatcher.acknowledge(id)
                return

        # Determine the ASL state type of the current state.
        state_type = state["Type"]

        """
        print("state_machine_id = " + state_machine_id)
        print("parent_state = " + parent_state)
        print("parent = " + str(parent))
        print("current_state = " + current_state)
        print("state = " + str(state))
        print("data = " + str(data))
        print("event id = " + str(id))
        """

        # ----------------------------------------------------------------------
        def handle_error(error_type, error_message):
            """
            https://states-language.net/spec.html#retrying-after-error Task
            States, Parallel States, and Map States MAY have a field named
            “Retry”, whose value MUST be an array of objects, called Retriers.
            """
            retry_matched = False
            catch_matched = False

            retry = state.get("Retry")
            if retry and isinstance(retry, list):
                for retrier in retry:
                    """
                    Each Retrier MUST contain a field named “ErrorEquals”
                    whose value MUST be a non-empty array of Strings,
                    which match Error Names.

                    When a state reports an error, the interpreter scans
                    through the Retriers and, when the Error Name appears
                    in the value of a Retrier’s “ErrorEquals” field,
                    implements the retry policy described in that Retrier.

                    How to correctly handle States.TaskFailed is a little
                    unclear, but the AWS Error Handling document here:
                    https://docs.aws.amazon.com/step-functions/latest/dg/concepts-error-handling.html
                    says "States.TaskFailed, which matches any error that
                    a Lambda function outputs." which suggests that if
                    error_type has *any* value and States.TaskFailed is
                    present in the ErrorEquals array then we should match.
                    """
                    error_equals = retrier.get("ErrorEquals")
                    if (
                        error_type in error_equals
                        or "States.TaskFailed" in error_equals
                        or (len(error_equals) == 1
                            and error_equals[0] == "States.ALL")
                    ):
                        # Default values taken from ASL specification.
                        interval_seconds = retrier.get("IntervalSeconds", 1)
                        max_attempts = retrier.get("MaxAttempts", 3)
                        backoff_rate = retrier.get("BackoffRate", 2.0)
                        if backoff_rate < 1.0:
                            backoff_rate = 1.0

                        retries = context["State"].get("RetryCount", 0)
                        if retries < max_attempts:
                            retry_matched = True
                            timeout = interval_seconds * (backoff_rate ** retries)
                            retries += 1
                            context["State"]["RetryCount"] = retries
                            context["State"]["RetryTimeout"] = timeout * 1000
                            context["State"]["EnteredTime"] = (
                                datetime.fromtimestamp(
                                    time.time() + timeout, timezone.utc
                                ).astimezone().isoformat()
                            )
                            """
                            Republish the Task state event with the new
                            RetryCount and RetryTimeout set. We also adjust
                            EnteredTime above. The ASL spec is unclear on
                            this, but if the error is due to States.Timeout
                            and we retry it would seem logical to reset,
                            otherwise when we retry the Task will timeout
                            immediately, similarly we add the RetryTimeout
                            to the EnteredTime as we don't actually start
                            retrying any Task resource until after
                            RetryTimeout has expired.
                            TODO - check the behaviour of AWS Step Functions
                            to see what they actually do in this scenario.
                            """
                            self.event_dispatcher.publish(event)

                        break

            """
            https://states-language.net/spec.html#fallback-states
            When a state reports an error and either there is no Retrier, or
            retries have failed to resolve the error, the interpreter scans
            through the Catchers in array order, and when the Error Name appears
            in the value of a Catcher’s “ErrorEquals” field, transitions the
            machine to the state named in the value of the “Next” field.

            When a state has both “Retry” and “Catch” fields, the interpreter
            uses any appropriate Retriers first and only applies the a matching
            Catcher transition if the retry policy fails to resolve the error.
            """
            catch = state.get("Catch")
            if not retry_matched and catch and isinstance(catch, list):
                for catcher in catch:
                    """
                    Each Catcher MUST contain a field named “ErrorEquals”,
                    specified exactly as with the Retrier “ErrorEquals” field,
                    and a field named “Next” whose value MUST be a string
                    exactly matching a State Name.

                    When a state reports an error and either there is no
                    Retrier, or retries have failed to resolve the error,
                    the interpreter scans through the Catchers in array order,
                    and when the Error Name appears in the value of a Catcher’s 
                    “ErrorEquals” field, transitions the machine to the state
                    named in the value of the “Next” field.
                    """
                    error_equals = catcher.get("ErrorEquals")
                    if (
                        error_type in error_equals
                        or "States.TaskFailed" in error_equals
                        or (len(error_equals) == 1
                            and error_equals[0] == "States.ALL")
                    ):
                        catch_matched = True
                        """
                        When a state reports an error and it matches a
                        Catcher, causing a transfer to another state, the
                        state’s Result (and thus the input to the state
                        identified in the Catcher’s “Next” field) is a JSON
                        object, called the Error Output. The Error Output
                        MUST have a string-valued field named “Error”,
                        containing the Error Name. It SHOULD have a string
                        valued field named “Cause”, containing human-readable
                        text about the error.
                        """
                        result = {"Error": error_type}
                        if error_message:
                            result["Cause"] = error_message

                        """
                        A Catcher MAY have an “ResultPath” field, which works
                        exactly like a state’s top-level “ResultPath”, and
                        may be used to inject the Error Output into the
                        state’s original input to create the input for the
                        Catcher’s “Next” state. The default value, if the
                        “ResultPath” field is not provided, is “$”, meaning
                        that the output consists entirely of the Error Output. 
                        """
                        # Catcher applies ResultPath to "raw input"
                        output = apply_resultpath(
                            data, result, catcher.get("ResultPath", "$")
                        )
                        event["data"] = apply_jsonpath(output, "$")

                        self.change_state(state_type, catcher.get("Next"), event)
                        break

            """
            When a state reports an error, the default course of action for the
            interpreter is to fail the whole state machine, so if no retriers or
            catchers match then fail the execution.
            """
            if not retry_matched and not catch_matched:
                result = {"Error": error_type}
                if error_message:
                    result["Cause"] = error_message

                event["data"] = result
                self.end_state_machine(state_type, event)

        # ----------------------------------------------------------------------


        def handle_terminal_state():
            """
            This function handles the boilerplate needed for terminal states.
            It should have the same state (id, state_type, event, etc.) available
            to it as the calling function as it is wrapped in the same closure.
            """
            if context.get("Parallel"):
                execution_arn = context["Execution"]["Id"]
                self.update_execution_history(
                    execution_arn,
                    state_type + "StateExited",
                    {"output": event["data"], "name": current_state},
                )
                asl_state_Parallel_collect_results()
            elif context.get("Map"):
                asl_state_Map_collect_results()
            else:
                self.end_state_machine(state_type, event)
                self.event_dispatcher.acknowledge(id)

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
            # print(parameters)

            """
            A Pass State MAY have a field named “Result”. If present, its value
            is treated as the output of a virtual task, and placed as prescribed
            by the “ResultPath” field, if any, to be passed on to the next state.

            If “Result” is not provided, the output is the input. Thus if neither
            neither “Result” nor “ResultPath” are provided, the Pass state
            copies its input through to its output.
            """
            result = state.get(
                "Result", parameters
            ) # Default is the "effective input" as per ASL spec.

            try:
                # Pass state applies ResultPath to "raw input"
                output = apply_resultpath(
                    data, result, state.get("ResultPath", "$")
                )
                event["data"] = apply_jsonpath(
                    output, state.get("OutputPath", "$")
                )

                if state.get("End"):
                    handle_terminal_state()
                else:
                    self.change_state(state_type, state.get("Next"), event)
                    self.event_dispatcher.acknowledge(id)
            except ResultPathMatchFailure as e:
                handle_error("States.ResultPathMatchFailure", str(e))
                self.event_dispatcher.acknowledge(id)

        def asl_state_Task_delegate():
            """
            This function represents the real Task state, it is delegated to by
            the asl_state_Task function when any retry delay interval has expired.

            https://states-language.net/spec.html#task-state
            https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-task-state.html
            """

            """
            It's important for the on_response function to be nested as we want
            the event, state and id to be wrapped in its closure, to be used when
            the service integrated to the Task *actually* returns its result.
            """
            def on_response(result):
                """
                The use of the "errorType" field to report an error invoking a
                Task isn't actually mentioned in the ASL specification, but this
                is the pattern adopted for error messages returned by AWS Lambda
                so is almost certainly a fair starting point.
                https://docs.aws.amazon.com/lambda/latest/dg/python-exceptions.html
                https://docs.aws.amazon.com/lambda/latest/dg/nodejs-prog-mode-exceptions.html
                https://docs.aws.amazon.com/lambda/latest/dg/java-exceptions.html
                """
                error_type = result.get("errorType")
                if error_type:
                    error_message = result.get("errorMessage", "")
                    self.logger.warning("{}: {}".format(error_type, error_message))
                    handle_error(error_type, error_message)
                    self.event_dispatcher.acknowledge(id)
                else:  # No error
                    try:
                        # Task state applies ResultPath to "raw input"
                        output = apply_resultpath(
                            data, result, state.get("ResultPath", "$")
                        )
                        event["data"] = apply_jsonpath(
                            output, state.get("OutputPath", "$")
                        )

                        if state.get("End"):
                            handle_terminal_state()
                        else:
                            self.change_state(state_type, state.get("Next"), event)
                            self.event_dispatcher.acknowledge(id)
                    except ResultPathMatchFailure as e:
                        handle_error("States.ResultPathMatchFailure", str(e))
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
            resource_arn = state.get("Resource")

            """
            https://states-language.net/spec.html#parameters

            If the “Parameters” field is provided, its value, after the
            extraction and embedding, becomes the effective input.
            """
            parameters = evaluate_parameters(input, context, state.get("Parameters"))
            # print(parameters)

            """
            Tasks can optionally specify timeouts. Timeouts (the “TimeoutSeconds”
            and “HeartbeatSeconds” fields) are specified in seconds and MUST be
            positive integers. If provided, the “HeartbeatSeconds” interval MUST
            be smaller than the “TimeoutSeconds” value.

            If the state runs longer than the specified timeout, or if more time
            than the specified heartbeat elapses between heartbeats from the task,
            then the interpreter fails the state with a States.Timeout Error Name.

            Get the execution and state entry timestamps and the execution and
            state timeout values (if present) and use those to compute the
            Task expiry time that will be checked against the current time each
            heartbeat and will eventually result in the Task returning a
            States.Timeout Error if it expires before it completes. The default
            State Machine execution timeout is 1 year 60*60*24*365 = 31536000
            https://docs.aws.amazon.com/step-functions/latest/dg/limits.html
            The default Task state timeout is more confusing as this document
            https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-task-state.html says: "If not provided, the default value is 99999999" whereas
            https://states-language.net/spec.html#task-state says "If not
            provided, the default value of “TimeoutSeconds” is 60."
            """
            current_timestamp = time.time()

            start_time = context["Execution"].get("StartTime")
            execution_timestamp = parse_rfc3339_datetime(start_time).timestamp()
            t1 = (execution_timestamp + ASL.get("TimeoutSeconds", 31536000) - current_timestamp) * 1000

            entered_time = context["State"].get("EnteredTime")
            state_timestamp = parse_rfc3339_datetime(entered_time).timestamp()
            t2 = (state_timestamp + state.get("TimeoutSeconds", 99999999) - current_timestamp) * 1000
            #t2 = (state_timestamp + state.get("TimeoutSeconds", 60) - current_timestamp) * 1000
            #t2 = (state_timestamp + state.get("TimeoutSeconds", 10) - current_timestamp) * 1000

            timeout = t1 if t1 < t2 else t2

            self.task_dispatcher.execute_task(
                resource_arn, parameters, on_response, timeout, context
            )

        def asl_state_Task():
            """
            This function delegates to the real Task state implementation
            asl_state_Task_delegate when any retry timeout has expired.
            """
            retry_timeout = context["State"].get("RetryTimeout", 0)
            self.event_dispatcher.set_timeout(asl_state_Task_delegate, retry_timeout)

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
                variable = apply_jsonpath(
                    input, variable_field, return_false_on_failed_match=True
                )
                next = choice.get("Next", True)

                def isnumber(x):
                    # General test for numeric - any number x zero is zero
                    try:
                        return not isinstance(x, bool) and 0 == x * 0
                    except:
                        return False
    
                def asl_choice_And():
                    if all(choose(ch) for ch in choice["And"]):
                        return next

                def asl_choice_Or():
                    if any(choose(ch) for ch in choice["Or"]):
                        return next

                def asl_choice_Not():
                    if (not choose(choice["Not"])):
                        return next

                def asl_choice_BooleanEquals():
                    value = choice["BooleanEquals"]
                    if (
                        isinstance(variable, bool)
                        and isinstance(value, bool)
                        and variable == value
                    ):
                        return next

                def asl_choice_NumericEquals():
                    value = choice["NumericEquals"]
                    if isnumber(variable) and isnumber(value) and variable == value:
                        return next

                def asl_choice_NumericGreaterThan():
                    value = choice["NumericGreaterThan"]
                    if isnumber(variable) and isnumber(value) and variable > value:
                        return next

                def asl_choice_NumericGreaterThanEquals():
                    value = choice["NumericGreaterThanEquals"]
                    if isnumber(variable) and isnumber(value) and variable >= value:
                        return next

                def asl_choice_NumericLessThan():
                    value = choice["NumericLessThan"]
                    if isnumber(variable) and isnumber(value) and variable < value:
                        return next

                def asl_choice_NumericLessThanEquals():
                    value = choice["NumericLessThanEquals"]
                    if isnumber(variable) and isnumber(value) and variable <= value:
                        return next

                def asl_choice_StringEquals():
                    value = choice["StringEquals"]
                    if (
                        isinstance(variable, str)
                        and isinstance(value, str)
                        and variable == value
                    ):
                        return next

                def asl_choice_CaseInsensitiveStringEquals():
                    # Not covered in ASL spec. but useful and trivial to handle.
                    value = choice["CaseInsensitiveStringEquals"]
                    if (
                        isinstance(variable, str)
                        and isinstance(value, str)
                        and variable.lower() == value.lower()
                    ):
                        return next

                def asl_choice_StringGreaterThan():
                    value = choice["StringGreaterThan"]
                    if (
                        isinstance(variable, str)
                        and isinstance(value, str)
                        and variable > value
                    ):
                        return next

                def asl_choice_StringGreaterThanEquals():
                    value = choice["StringGreaterThanEquals"]
                    if (
                        isinstance(variable, str)
                        and isinstance(value, str)
                        and variable >= value
                    ):
                        return next

                def asl_choice_StringLessThan():
                    value = choice["StringLessThan"]
                    if (
                        isinstance(variable, str)
                        and isinstance(value, str)
                        and variable < value
                    ):
                        return next

                def asl_choice_StringLessThanEquals():
                    value = choice["StringLessThanEquals"]
                    if (
                        isinstance(variable, str)
                        and isinstance(value, str)
                        and variable <= value
                    ):
                        return next

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
                    value = parse_rfc3339_datetime(
                        choice["TimestampEquals"]
                    ).timestamp()
                    if timestamp == value:
                        return next

                def asl_choice_TimestampGreaterThan():
                    timestamp = parse_rfc3339_datetime(variable).timestamp()
                    value = parse_rfc3339_datetime(
                        choice["TimestampGreaterThan"]
                    ).timestamp()
                    if timestamp > value:
                        return next

                def asl_choice_TimestampGreaterThanEquals():
                    timestamp = parse_rfc3339_datetime(variable).timestamp()
                    value = parse_rfc3339_datetime(
                        choice["TimestampGreaterThanEquals"]
                    ).timestamp()
                    if timestamp >= value:
                        return next

                def asl_choice_TimestampLessThan():
                    timestamp = parse_rfc3339_datetime(variable).timestamp()
                    value = parse_rfc3339_datetime(
                        choice["TimestampLessThan"]
                    ).timestamp()
                    if timestamp < value:
                        return next

                def asl_choice_TimestampLessThanEquals():
                    timestamp = parse_rfc3339_datetime(variable).timestamp()
                    value = parse_rfc3339_datetime(
                        choice["TimestampLessThanEquals"]
                    ).timestamp()
                    if timestamp <= value:
                        return next

                for key in choice:
                    """
                    Determine the ASL choice operator of the current choice and
                    use that to dynamically invoke the appropriate choice handler.
                    """
                    try:
                        next_state = locals().get("asl_choice_" + key, lambda: None)()
                    except Exception:
                        next_state = None

                    if next_state:
                        return next_state

            """
            A Choice state state MUST have a “Choices” field whose value is a
            non-empty array. Each element of the array is called a Choice Rule -
            an object containing a comparison operation and a “Next” field,
            whose value MUST match a state name.
            """
            choices = state.get("Choices", [])  # Sets to [] if key not present
            
            """
            The interpreter attempts pattern-matches against the Choice Rules in
            array order and transitions to the state specified in the “Next”
            field on the first Choice Rule where there is an exact match between
            the input value and a member of the comparison-operator array.
            """
            for choice in choices:
                next_state = choose(choice)
                if next_state:
                    break

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
                message = "Choice state {} failed to find a match for the condition field extracted from its input".format(current_state)
                self.logger.warning("States.NoChoiceMatched {}".format(message))
                handle_error("States.NoChoiceMatched", message)

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
                if state.get("End"):
                    handle_terminal_state()
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
            entered_time = context["State"].get("EnteredTime")
            state_timestamp = parse_rfc3339_datetime(entered_time).timestamp()

            if seconds:
                current_timestamp = time.time()
                timeout = (state_timestamp + seconds - current_timestamp) * 1000
            elif seconds_path:
                seconds = apply_jsonpath(input, seconds_path)
                current_timestamp = time.time()
                timeout = (state_timestamp + seconds - current_timestamp) * 1000
            elif timestamp:
                try:
                    target_timestamp = parse_rfc3339_datetime(timestamp).timestamp()
                    current_timestamp = time.time()
                    timeout = (target_timestamp - current_timestamp) * 1000
                except ValueError as e:
                    self.logger.warning(
                        "timestamp {} failed to parse correctly, defaulting to zero delay".format(
                            timestamp
                        )
                    )
                    timeout = 0
            elif timestamp_path:
                timestamp = apply_jsonpath(input, timestamp_path)
                try:
                    target_timestamp = parse_rfc3339_datetime(timestamp).timestamp()
                    current_timestamp = time.time()
                    timeout = (target_timestamp - current_timestamp) * 1000
                except ValueError as e:
                    self.logger.warning(
                        "timestamp {} failed to parse correctly, defaulting to zero delay".format(
                            timestamp
                        )
                    )
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
            handle_terminal_state()

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
                "Cause": state.get("Cause", "Unspecified"),
            }

            # Fail states don't allow InputPath, OutputPath or ResultPath
            event["data"] = error
            handle_terminal_state()

        def asl_state_Parallel():
            """
            https://states-language.net/spec.html#parallel-state
            https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-parallel-state.html

            A Parallel state causes the interpreter to execute each branch
            starting with the state named in its “StartAt” field, as concurrently
            as possible.

            The "wait until each branch terminates (reaches a terminal state)
            before processing the Parallel state's “Next” field." part of the 
            Parallel state is implemented in asl_state_Parallel_collect_results()

            The Parallel state passes its input (potentially as filtered by the
            “InputPath” field) as the input to each branch’s “StartAt” state.
            """
            input = apply_jsonpath(data, state.get("InputPath", "$"))

            """
            https://states-language.net/spec.html#parameters

            If the “Parameters” field is provided, its value, after the
            extraction and embedding, becomes the effective input.
            """
            parameters = evaluate_parameters(input, context, state.get("Parameters"))
            # print(parameters)

            """
            A Parallel State MUST contain a field named “Branches” which is an
            array whose elements MUST be objects. Each object MUST contain fields
            named “States” and “StartAt” whose meanings are exactly like those
            in the top level of a State Machine.

            Iterate through the branches and launch each branch State Machine.
            The index of each branch is needed to create the final result, so
            we store it using the same "pattern" that is used for the Map state
            index as described in the AWS documentation for the context object.
            https://docs.aws.amazon.com/step-functions/latest/dg/input-output-contextobject.html
            """
            branches = state.get("Branches", [])
            for index, branch in enumerate(branches):
                context["State"]["Name"] = branch.get("StartAt")
                context["State"]["EnteredTime"] = (
                    datetime.now(timezone.utc).astimezone().isoformat()
                )
                # Store the index in the context as described above.
                context["Parallel"] = {
                    "Item": {
                        "Input": data,  # Save the raw input to the Parallel state
                        "Index": index,
                    },
                }
                event["data"] = parameters
                self.event_dispatcher.publish(event)

            self.event_dispatcher.acknowledge(id)

        def asl_state_Parallel_collect_results():
            """
            Wait until each Parallel branch terminates (reaches a terminal
            state) before processing the Parallel state's “Next” field.
            """
            # Retrieve data from event again to ensure we have result not input.
            data = event["data"]

            execution_arn = context["Execution"]["Id"]
            index = context["Parallel"]["Item"]["Index"]

            # Initialise branch_results if necessary
            if not execution_arn in self.branch_results:
                self.branch_results[execution_arn] = {}
            if not parent_state in self.branch_results[execution_arn]:
                length = len(parent["Branches"])
                self.branch_results[execution_arn][parent_state] = {
                    "results": [None]*length,
                    "ids": [None]*length,
                }

            """
            Record the event id so we can acknowledge the event when the
            results from all branches have been returned.
            """
            branch_results = self.branch_results[execution_arn][parent_state]
            result = branch_results["results"]
            event_ids = branch_results["ids"]
            result[index] = data
            event_ids[index] = id

            # print(result)

            # Return if we haven't yet received the results from all branches
            if None in result:
                return

            # Parallel state applies ResultPath to "raw input"
            data = context["Parallel"]["Item"]["Input"]  # Get saved raw input
            output = apply_resultpath(
                data, result, parent.get("ResultPath", "$")
            )
            event["data"] = apply_jsonpath(
                output, parent.get("OutputPath", "$")
            )

            del context["Parallel"]
            context["State"]["Name"] = parent_state

            """
            Publish any new state change before acknowledging the events.
            """
            if not parent.get("End"):
                self.change_state("Parallel", parent.get("Next"), event)

            # Acknowledge the events for each branch's terminal state
            for event_id in event_ids:
                self.event_dispatcher.acknowledge(event_id)

            """
            Need to do this *after* acknowledging the events as it deletes the
            Parallel branch results for the current execution.
            """
            if parent.get("End"):
                self.end_state_machine("Parallel", event)

        def asl_state_Map():
            """
            https://states-language.net/spec.html#map-state
            https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-map-state.html

            """
            # TODO

            print("MAP - Not Yet Implemented")
            print(event)

            self.event_dispatcher.acknowledge(id)

        def asl_state_Map_collect_results():
            print()
            print("asl_state_Map_collect_results")
            """
            #print()
            #print(event)
            #print()
            #print(state)
            print(data)
            print()
            print(parent_state)
            print(parent)
            print("index = " + str(index))
            print("id = " + str(id))
            print(execution_arn)
            print()
            """

        """
        End of nested state handler functions.
        """
        # ----------------------------------------------------------------------


        """
        Update the execution history with StateEntered information. The test for
        RetryCount is because when we trigger a retry we do so by publishing
        an event to the message queue that triggers a transition back to the
        state being retried, but as this represents a retry and not an actual
        entry to the state we want to suppress the history update.
        """
        if not context["State"].get("RetryCount"):
            self.update_execution_history(
                context["Execution"]["Id"],
                state_type + "StateEntered",
                {"input": data, "name": current_state},
            )

        """
        Use the ASL state type of the current state to dynamically invoke the
        appropriate ASL state handler given state type. The (Python) lambda
        provides a default handler in case of malformed ASL. 
        """
        locals().get(
            "asl_state_" + state_type,
            lambda: self.logger.error(
                "StateEngine illegal state transition: {}".format(state_type)
            ),
        )()

