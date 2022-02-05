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


import operator, time, uuid, fnmatch, opentracing

from datetime import datetime, timezone, timedelta
from aioprometheus import Counter, Histogram

#from asl_workflow_engine.metrics_summary import TimeWindowSummary as Summary
from asl_workflow_engine.metrics_summary import BasicSummary as Summary

from asl_workflow_engine.state_engine_paths import (
    apply_path,
    apply_jsonpath,
    get_full_jsonpath,
    apply_resultpath,
    evaluate_payload_template,
)

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.open_tracing_factory import span_context, inject_span
from asl_workflow_engine.store import (
    create_ASL_store,
    create_executions_store,
    create_history_store,
)
from asl_workflow_engine.task_dispatcher import TaskDispatcher

from asl_workflow_engine.asl_exceptions import *
from asl_workflow_engine.arn import *

try:  # Attempt to use ujson if available https://pypi.org/project/ujson/
    import ujson as json
except:  # Fall back to standard library json
    import json

"""
Stepfunction quota/size limits
https://docs.aws.amazon.com/step-functions/latest/dg/limits.html
https://docs.aws.amazon.com/step-functions/latest/apireference/API_StartExecution.html
https://docs.aws.amazon.com/step-functions/latest/apireference/API_CreateStateMachine.html
"""
MAX_EXECUTION_HISTORY_LENGTH = 25000
MAX_DATA_LENGTH = 262144  # Max length of the input or output JSON string.
MAX_STATE_MACHINE_LENGTH = 1048576  # Max length of the State Machine definition.

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

def find_state(current_state_machine, current_state):
    """
    Look-up the specified JSON state machine to find the state object with the
    given name. If it can't be found by a simple look-up try to find it in
    a nested (e.g. Parallel or Map) state machine as described below.
    """
    state = current_state_machine.get(current_state)
    if state == None:
        """
        If the state can't be found in the parent state machine search for
        it more deeply using recursive descent, as the specified state might
        actually be in a Parallel branch or Map Iterator state machine.
        Because JSONPath doesn't have a parent operator and we want to get
        the parent States object too we get the full JSONPath string for
        the query then use simple string splits to find the path of that.
        """
        path = get_full_jsonpath(current_state_machine, "$.." + current_state)
        states_path = path.rpartition("['States']")[0]
        if path and states_path:
            branch = apply_jsonpath(current_state_machine, states_path)
            current_state_machine = branch["States"]
            state = current_state_machine.get(current_state)

    return state, current_state_machine

def merge_result(data, context, result, state, output_path=None):
    """
    Boiler plate to apply both ResultPath and OutputPath (or supplied output_path)
    """
    output = apply_resultpath(
        data, result, state.get("ResultPath", "$")
    )

    output_path = output_path if output_path else state.get("OutputPath", "$")
    return apply_path(output, context, output_path)


class StateEngine(object):
    def __init__(self, config):
        """
        """
        self.logger = init_logging(log_name="asl_workflow_engine")
        self.logger.info("Creating StateEngine, using {} JSON parser".format(json.__name__))

        store_url = config["state_engine"]["store_url"]
        
        """
        The ASL store is semantically a dict of dict. The outer dict holds
        ASL objects (also dicts) keyed by the ARN of the ASL State Machine.
        The format of the ASL objects is the same as DescribeStateMachine.
        https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeStateMachine.html
        """
        self.asl_store = create_ASL_store(store_url)

        """
        Holds information required by the DescribeExecution API call.
        The executions store is semantically a dict of dict.
        https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeExecution.html
        """
        self.executions = create_executions_store(store_url)

        """
        Holds information required by the GetExecutionHistory API call.
        The executions store is semantically a dict of list of immutable dicts.
        https://docs.aws.amazon.com/step-functions/latest/apireference/API_GetExecutionHistory.html
        """
        self.execution_history = create_history_store(store_url)

        """
        The execution_ttl is the time to live, in seconds, for the execution
        metadata used by DescribeExecution (executions) and GetExecutionHistory
        (execution_history). The initial default is one day 60x60x24 = 86400.
        Whether to raise or lower this value will depend on the available memory
        of the Redis server. The execution_ttl is only honoured by the Redis
        store and is ignored by the simple store.
        """
        self.execution_ttl = config["state_engine"]["execution_ttl"]

        """
        Holds the results of Parallel state Branches or Map state Iterations.
        It holds objects keyed by the execution ARN and each object is another
        dict which contains arrays keyed by the Parallel or Map state name.
        """
        self.branch_results = {}

        self.task_dispatcher = TaskDispatcher(self, config)
        # self.event_dispatcher set by EventDispatcher

        """
        Prometheus metrics intended to emulate Stepfunction CloudWatch metrics.
        https://docs.aws.amazon.com/step-functions/latest/dg/procedure-cw-metrics.html
        """
        self.execution_metrics = {}
        metrics_config = config.get("metrics", {})
        if metrics_config.get("implementation", "") == "Prometheus":
            ns = metrics_config.get("namespace", "")
            if ns:
                self.logger.info("Enabling Prometheus Metrics in namespace: " + ns)
                ns += "_"
            else:
                self.logger.info("Enabling Prometheus Metrics")

            self.execution_metrics = {
                "ExecutionTime": Summary(
                    ns + "ExecutionTime",
                    "The interval, in milliseconds, between the time the " +
                    "execution starts and the time it closes."
                ),
                "ExecutionsFailed": Counter(
                    ns + "ExecutionsFailed",
                    "The number of failed executions."
                ),
                "ExecutionsStarted": Counter(
                    ns + "ExecutionsStarted",
                    "The number of started executions."
                ),
                "ExecutionsSucceeded": Counter(
                    ns + "ExecutionsSucceeded",
                    "The number of successfully completed executions."
                ),
                "ExecutionsTimedOut": Counter(
                    ns + "ExecutionsTimedOut",
                    "The number of executions that time out for any reason."
                )
            }

    def log_and_drop(self, message, obj, id):
        """
        Boiler plate to deal with unrecoverable errors that should rarely occur.
        """
        self.logger.error(
            ("StateEngine: " + message + ", dropping the message!").format(obj)
        )
        self.event_dispatcher.acknowledge(id)

    def broadcast_notification(self, execution_arn, execution_detail, context):
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
        groups of events using wildcards, for example the subscription:
        asl_workflow_engine/arn:aws:states:local:0123456789:stateMachine:simple_state_machine.*
        Subscribes to all notification events published for a given state machine.

        The format of the message body is as described in
        https://docs.aws.amazon.com/step-functions/latest/dg/cw-events.html
        the "detail" field gives access to the "executionArn", "stateMachineArn",
        "input", "output" and other relevant information and provides the same
        information as the DescribeExecution API.
        """

        arn = parse_arn(execution_arn)
        region=arn.get("region", "local")
        account=arn["account"]

        """
        https://docs.aws.amazon.com/step-functions/latest/dg/cw-events.html
        https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/CloudWatchEventsandEventPatterns.html
        https://stackoverflow.com/questions/8556398/generate-rfc-3339-timestamp-in-python
        """
        if not isinstance(execution_detail , dict):  # May be (non JSON) RedisDict
            execution_detail = dict(execution_detail)

        """
        There is an inconsistency in AWS. The DescribeExecution documentation
        has startDate and stopDate described as number and Type: Timestamp, but
        doesn't mention anything about Timestamp units (seconds or millis),
        however boto3 barfs on start_execution, list_execution and
        describe_execution unless timestamp is in epoch seconds e.g. time.time()
        https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeExecution.html
        However the EventBridge (CloudWatch Events) example here:
        https://docs.aws.amazon.com/step-functions/latest/dg/cw-events.html#cw-events-execution-succeeded
        illustrates startDate and stopDate as millisecond Timestamps and that
        seems to be the case for real with CW Events too, so we need to
        convert to millis here. Rather than clone the whole dict just for
        those two fields we save their original values and copy them back after
        we broadcast the message.
        """
        saved_startDate = execution_detail["startDate"]
        saved_stopDate = execution_detail["stopDate"]

        if saved_startDate:
            execution_detail["startDate"] = int(saved_startDate * 1000)
        if execution_detail["stopDate"]:
            execution_detail["stopDate"] = int(saved_stopDate * 1000)

        """
        Start an OpenTracing trace for the notification.
        https://opentracing.io/guides/python/tracers/ standard tags are from
        https://opentracing.io/specification/conventions/
        """
        with opentracing.tracer.start_active_span(
            operation_name="StatusChange",
            child_of=span_context("text_map", context.get("Tracer", {}), self.logger),
            tags={
                "component": "state_engine",
                "execution_arn": execution_arn,
            }
        ) as scope:
            carrier = inject_span("text_map", scope.span, self.logger)
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
                "detail": execution_detail,
            }

            subject = execution_detail["stateMachineArn"] + "." + execution_detail["status"]
            self.event_dispatcher.broadcast(subject, cw_event, carrier_properties=carrier)

        # Copy the original seconds since epoch timestamps back.
        execution_detail["startDate"] = saved_startDate
        execution_detail["stopDate"] = saved_stopDate

    def start_execution(self, state_machine_type, start_state, event):
        """
        Initialise the state machine execution's state, in particular this will
        set any context metadata that hasn't previously been set elsewhere.
        This is mostly only necessary if the execution was started in a
        "low-level" e.g. way by publishing directly to the event queue, whereas
        if the execution was started via the REST API the context metadata should
        already be set and this method's purpose is then simply to update the
        execution history metadata.
        """
        data = event["data"]
        context = event["context"]
        state_machine_arn = context["StateMachine"]["Id"]

        context["State"]["Name"] = start_state

        if "Execution" not in context:
            context["Execution"] = {}
        execution = context["Execution"]

        if "Name" not in execution:
            execution["Name"] = str(uuid.uuid4())

        if "Id" not in execution:
            # Create Id
            # Form executionArn from stateMachineArn and uuid
            arn = parse_arn(state_machine_arn)
            execution_arn = create_arn(
                service="states",
                region=arn.get("region", "local"),
                account=arn["account"],
                resource_type="execution",
                resource=arn["resource"] + ":" + execution["Name"],
            )
            execution["Id"] = execution_arn

        execution_arn = execution["Id"]  # The previous block ensures this is set.

        """
        execution["Input"] holds the initial Step Function input. As this is
        part of the Context Object the "Input" field here is a JSON object.
        https://docs.aws.amazon.com/step-functions/latest/dg/input-output-contextobject.html
        """
        if "Input" not in execution:
            execution["Input"] = data

        if "RoleArn" not in execution:
            """
            The default dummy ARN case shouldn't generally occur unless
            the asl_store "database" has been manually edited because when
            it is updated via the API the roleArn field gets populated.
            """
            asl_item = self.asl_store.get_cached_view(state_machine_arn, {})
            execution["RoleArn"] = asl_item.get(
                "roleArn", "arn:aws:iam:::role/dummy-role/dummy"
            )

        # https://stackoverflow.com/questions/8556398/generate-rfc-3339-timestamp-in-python
        start_time = datetime.now(timezone.utc).astimezone().isoformat()
        if "StartTime" not in execution:
            execution["StartTime"] = start_time

        """
        For the start state set EnteredTime here, which will be reset if
        the message gets redelivered, for other state transitions we will
        set EnteredTime when the transition occurs so if redeliveries
        occur the EnteredTime will reflect the time the state was entered
        not the time the redelivery occurred.
        """
        if "EnteredTime" not in context["State"]:
            context["State"]["EnteredTime"] = start_time
            
        """
        Populate the metadata required by the DescribeExecution API call.
        https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeExecution.html
        Note that the "input" and "output" fields of the execution detail and
        history are actually JSON strings not JSON objects.
        It actually took a while to note that subtlety, especially as the AWS
        CLI describe-execution call doesn't actually complain and displayed the
        JSON object that was originally being sent before json.dumps() was added.
        """
        input_as_string = json.dumps(data)
        execution_detail = {
            "executionArn": execution_arn,
            "input": input_as_string,
            "name": execution["Name"],
            "output": None,
            "startDate": time.time(),
            "stateMachineArn": state_machine_arn,
            "status": "RUNNING",
            "stopDate": None,
        }

        """
        Only store execution metadata/history for "STANDARD" not "EXPRESS"
        State Machine workflows.
        """
        if state_machine_type == "STANDARD":
            self.executions[execution_arn] = execution_detail
            self.executions.set_ttl(execution_arn, self.execution_ttl)

            # Don't set the execution_history ttl here, wait until first append.
            self.execution_history[execution_arn] = []

        """
        Still call this for "EXPRESS" State Machine workflows. Initially it will
        return immediately for "EXPRESS" workflows, but we may do extra logging.
        """
        self.update_execution_history(
            state_machine_type,
            execution_arn,
            "ExecutionStarted",
            {"input": input_as_string, "roleArn": execution["RoleArn"]},
        )

        if self.execution_metrics:
            self.execution_metrics["ExecutionsStarted"].inc(
                {"StateMachineArn": state_machine_arn}
            )

        self.broadcast_notification(execution_arn, execution_detail, context)

    def change_state(self, state_machine_type, state_type, next_state, event):
        """
        Set event's new current state in $$.State.Name to Next state.
        """
        data = event["data"]
        """
        Note that the "output" field of the execution detail and history is a
        JSON string not a JSON object, hence the json.dumps().
        https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeExecution.html
        https://docs.aws.amazon.com/step-functions/latest/apireference/API_GetExecutionHistory.html
        """
        output_as_string = json.dumps(data)
        context = event["context"]
        state = context["State"]

        """
        First check if the State's aggregate output has exceeded the 262144
        character quota described in Stepfunction Quotas page.
        https://docs.aws.amazon.com/step-functions/latest/dg/limits.html
        If so then we fail the execution.
        """
        if len(output_as_string) > MAX_DATA_LENGTH:
            error_message = ("The state/task '{}' returned a result with a size "
                             "exceeding the maximum number of characters "
                             "service limit.").format(state["Name"])
            event["data"] = {
                "Error": "States.DataLimitExceeded",
                "Cause": error_message,
            }
            self.logger.warning("States.DataLimitExceeded: {}".format(error_message))
            self.end_execution(state_machine_type, state_type, event)
            return

        self.update_execution_history(
            state_machine_type,
            context["Execution"]["Id"],
            state_type + "StateExited",
            {"output": output_as_string, "name": state["Name"]},
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

    def end_execution(self, state_machine_type, state_type, event):
        """
        End the state machine execution and update execution metadata.
        """
        data = event["data"]
        """
        Note that the "output" field of the execution detail and history is a
        JSON string not a JSON object, hence the json.dumps().
        https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeExecution.html
        https://docs.aws.amazon.com/step-functions/latest/apireference/API_GetExecutionHistory.html
        """
        output_as_string = json.dumps(data)
        context = event["context"]
        state = context["State"]
        execution_arn = context["Execution"]["Id"]

        execution_failed = isinstance(data, dict) and data.get("Error")

        # Stepfunctions don't transition to StateExited if the execution fails.
        if not execution_failed:
            self.update_execution_history(
                state_machine_type,
                execution_arn,
                state_type + "StateExited",
                {"output": output_as_string, "name": state["Name"]},
            )

        if state_machine_type == "STANDARD":
            execution_detail = self.executions[execution_arn]
            state_machine_arn = execution_detail["stateMachineArn"]
        else:
            """
            For "EXPRESS" workflows we don't store the execution metadata in
            self.executions, however the missing information for the current
            execution is actually available in the execution context or can
            be derived from the execution ARN.
            """
            # Derive missing fields from execution_arn
            split = execution_arn.rpartition(':')
            arn = parse_arn(split[0])
            arn["resource_type"] = "stateMachine"
            state_machine_arn = create_arn(arn)
            name = split[2]

            execution = context["Execution"]
            start_date = parse_rfc3339_datetime(execution["StartTime"]).timestamp()

            execution_detail = {
                "executionArn": execution_arn,
                "input": json.dumps(execution["Input"]),
                "name": name,
                "output": None,
                "startDate": start_date,
                "stateMachineArn": state_machine_arn,
                "status": "RUNNING",
                "stopDate": None,
            }

        execution_detail["stopDate"] = time.time()

        with opentracing.tracer.start_active_span(
            operation_name="StartExecution:ExecutionEnding",
            child_of=span_context("text_map", context.get("Tracer", {}), self.logger),
            tags={
                "component": "state_engine",
                "execution_arn": execution_arn
            }
        ) as scope:
            if execution_failed:
                opentracing.tracer.active_span.set_tag("error", True)
                opentracing.tracer.active_span.log_kv(
                    {
                        "message": "ExecutionFailed",
                    }
                )
                opentracing.tracer.active_span.set_tag("status", "FAILED")
                execution_detail["status"] = "FAILED"
                """
                As per the DescribeExecution API, "output" is set only if the
                execution succeeds. If the execution fails, this field is null. 
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeExecution.html
                """
                execution_detail["output"] = None
                self.update_execution_history(
                    state_machine_type,
                    execution_arn,
                    "ExecutionFailed",
                    {"cause": data.get("Cause"), "error": data.get("Error")}
                )

                if self.execution_metrics:
                    if data.get("Error") == "States.Timeout":
                        self.execution_metrics["ExecutionsTimedOut"].inc(
                            {"StateMachineArn": state_machine_arn}
                        )

                    self.execution_metrics["ExecutionsFailed"].inc(
                        {"StateMachineArn": state_machine_arn}
                    )
            else:
                opentracing.tracer.active_span.set_tag("status", "SUCCEEDED")
                execution_detail["status"] = "SUCCEEDED"
                execution_detail["output"] = output_as_string
                self.update_execution_history(
                    state_machine_type,
                    execution_arn,
                    "ExecutionSucceeded",
                    {"output": output_as_string}
                )

                if self.execution_metrics:
                    self.execution_metrics["ExecutionsSucceeded"].inc(
                        {"StateMachineArn": state_machine_arn}
                    )
        
        # Tidy up self.branch_results for current execution_arn.
        if execution_arn in self.branch_results:
            del self.branch_results[execution_arn]

        if self.execution_metrics:
            duration = (execution_detail["stopDate"] - 
                        execution_detail["startDate"]) * 1000.0
            self.execution_metrics["ExecutionTime"].observe(
                {"StateMachineArn": state_machine_arn}, duration 
            )

        self.broadcast_notification(execution_arn, execution_detail, context)

    def update_execution_history(
            self, state_machine_type, execution_arn, update_type, details
        ):

        """
        TODO For "EXPRESS" State Machines we might actually want to log with a
        higher level than debug. "STANDARD" State Machines store execution
        history and other execution metadata but "EXPRESS" State Machines don't.
        However Express Workflows send execution history to CloudWatch Logs.
        https://aws.amazon.com/blogs/compute/new-express-workflows-for-aws-step-functions/
        """
        if state_machine_type == "EXPRESS":
            self.logger.debug("{} {} {}".format(execution_arn, update_type, details))
            return

        """
        Store the execution history information in a way that will be accessible
        to the REST API.

        Only do DEBUG logging, as the details object is likely to be expensive
        and in any case the idea is to get execution history via the REST API
        https://docs.aws.amazon.com/step-functions/latest/apireference/API_GetExecutionHistory.html
        """
        self.logger.debug("{} {} {}".format(execution_arn, update_type, details))

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
            arn = parse_arn(split[0])
            arn["resource_type"] = "stateMachine"
            state_machine_arn = create_arn(arn)
            name = split[2]

            self.executions[execution_arn] = {
                "executionArn": execution_arn,
                "input": None,
                "name": name,
                "output": None,
                "startDate": time.time(),
                "stateMachineArn": state_machine_arn,
                "status": "RUNNING",
                "stopDate": None,
            }
            self.execution_history[execution_arn] = []

        history = self.execution_history[execution_arn]
        """
        Events are numbered sequentially, starting at one.
        https://docs.aws.amazon.com/step-functions/latest/apireference/API_HistoryEvent.html
        """
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

        if id == 1:
            """
            Defer setting execution_history ttl to here, as we now have an item
            in the execution_history store to associate the ttl with.
            """
            self.execution_history.set_ttl(execution_arn, self.execution_ttl)

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
            self.log_and_drop("event {} has no $.context field", event, id)
            return

        state_machine = context.get("StateMachine")
        if state_machine == None:
            self.log_and_drop("event {} has no $.context.StateMachine field", event, id)
            return

        state_machine_arn = state_machine.get("Id")
        if not state_machine_arn:
            self.log_and_drop("event {} has no $.context.StateMachine.Id field", event, id)
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
            arn = parse_arn(state_machine_arn)
            creation_date = time.time()
            self.asl_store[state_machine_arn] = {
                "creationDate": creation_date,
                "definition": state_machine["Definition"],
                "name": arn["resource"],
                "roleArn": "arn:aws:iam:::role/dummy-role/dummy",
                "stateMachineArn": state_machine_arn,
                "updateDate": creation_date,
                "status": "ACTIVE",
                "type": "STANDARD",
            }
            del state_machine["Definition"]


        asl_item = self.asl_store.get_cached_view(state_machine_arn, {})
        ASL = asl_item.get("definition")
        if not ASL:
            """
            Dropping the message is possibly not the right thing to do in this
            scenario as we *could* simply add the State Machine and retry. OTOH
            if a State Machine is deleted executions should also be deleted.
            """
            self.log_and_drop("State Machine {} does not exist", state_machine_arn, id)
            return

        """
        Record if the State Machine for this execution is "STANDARD" or "EXPRESS".
        If "EXPRESS" certain features, like recording execution descriptions and
        history, will be disabled as per the Stepfunction API.
        """
        state_machine_type = asl_item.get("type")

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
            self.start_execution(state_machine_type, current_state, event)

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
        outside of their own “States” field. TODO use current_state_machine to
        check that state transitions only occur within the correct "States".
        """
        state, current_state_machine = find_state(ASL["States"], current_state)
        if state == None:  # state should be valid by this point
            self.log_and_drop("State {} does not exist", current_state, id)
            return

        # Determine the ASL state type of the current state.
        state_type = state["Type"]

        """
        print("state_machine_arn = " + state_machine_arn)
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

            if state_machine_type == "STANDARD":
                execution_arn = context["Execution"]["Id"]
                id = len(self.execution_history[execution_arn])
                boiler_plate = (
                    "An error occurred while executing the state "
                    "'{}' (entered at the event id #{}). "
                ).format(current_state, id)
            else:
                boiler_plate = (
                    "An error occurred while executing the state '{}'. "
                ).format(current_state)

            """
            An execution failed due to some exception that could not be
            processed. Often these are caused by errors at runtime, such
            as attempting to apply InputPath or OutputPath on a null
            JSON payload. A States.Runtime error is not retriable, and
            will always cause the execution to fail.
            A retry or catch on States.ALL will not catch States.Runtime errors. 
            """
            runtime_error = (error_type == "States.Runtime")

            retry = state.get("Retry")
            if not runtime_error and retry and isinstance(retry, list):
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
            if not runtime_error and not retry_matched and catch and isinstance(catch, list):
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
                            result["Cause"] = boiler_plate + error_message

                        """
                        A Catcher MAY have an “ResultPath” field, which works
                        exactly like a state’s top-level “ResultPath”, and
                        may be used to inject the Error Output into the
                        state’s original raw input to create the input for the
                        Catcher’s “Next” state. The default value, if the
                        “ResultPath” field is not provided, is “$”, meaning
                        that the output consists entirely of the Error Output. 
                        """
                        event["data"] = merge_result(data, context, result, catcher, "$")

                        self.change_state(
                            state_machine_type,
                            state_type,
                            catcher.get("Next"),
                            event
                        )
                        break

            """
            When a state reports an error, the default course of action for the
            interpreter is to fail the whole state machine, so if no retriers or
            catchers match then fail the execution.
            """
            if not retry_matched and not catch_matched:
                result = {"Error": error_type}
                if error_message:
                    result["Cause"] = boiler_plate + error_message

                event["data"] = result
                self.end_execution(state_machine_type, state_type, event)

        # ----------------------------------------------------------------------


        def handle_terminal_state(state_type, event, id=None):
            """
            This function handles the boilerplate needed for terminal states.
            """
            if "Branch" in context["State"]:
                execution_arn = context["Execution"]["Id"]
                self.update_execution_history(
                    state_machine_type,
                    execution_arn,
                    state_type + "StateExited",
                    {"output": json.dumps(event["data"]), "name": current_state},
                )
                asl_state_collect_results(state_type)
            else:
                self.end_execution(state_machine_type, state_type, event)
                if id != None:
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
            try:
                input = apply_path(data, context, state.get("InputPath", "$"))

                """
                https://states-language.net/spec.html#using-paths

                If the “Parameters” field is provided, its value, after the
                extraction and embedding, becomes the effective input.
                """
                parameters = evaluate_payload_template(
                    input, context, state.get("Parameters")
                )

                """
                A Pass State MAY have a field named “Result”. If present, its
                value is treated as the output of a virtual task, and placed as
                prescribed by the “ResultPath” field, if any, to be passed on
                to the next state.

                If “Result” is not provided, the output is the input. Thus if
                neither “Result” nor “ResultPath” are provided, the Pass state
                copies its input through to its output.
                """
                result = state.get(
                    "Result", parameters
                ) # Default is the "effective input" as per ASL spec.

                # Pass state applies ResultPath to "raw input"
                event["data"] = merge_result(data, context, result, state)

                if state.get("End"):
                    handle_terminal_state(state_type, event, id)
                else:
                    self.change_state(
                        state_machine_type,
                        state_type,
                        state.get("Next"),
                        event
                    )
                    self.event_dispatcher.acknowledge(id)
            except IntrinsicFailure as e:
                handle_error("States.IntrinsicFailure", str(e))
                self.event_dispatcher.acknowledge(id)
            except PathMatchFailure as e:
                handle_error("States.Runtime", str(e))
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
                    if error_type == "States.DataLimitExceeded":
                        error_message = ("The state/task '{}' returned a result "
                                         "with a size exceeding the maximum "
                                         "number of characters service limit.").format(
                                            state.get("Resource")
                                         )
                    else:
                        error_message = result.get("errorMessage", "")
                    self.logger.warning("{}: {}".format(error_type, error_message))
                    handle_error(error_type, error_message)
                    self.event_dispatcher.acknowledge(id)
                else:  # No error
                    try:
                        """
                        https://states-language.net/spec.html#using-paths

                        The value of "ResultSelector" MUST be a Payload Template,
                        whose input is the result, and whose payload replaces
                        and becomes the effective result.
                        """
                        result = evaluate_payload_template(
                            result, context, state.get("ResultSelector")
                        )

                        # Task state applies ResultPath to "raw input"
                        event["data"] = merge_result(data, context, result, state)

                        if state.get("End"):
                            handle_terminal_state(state_type, event, id)
                        else:
                            self.change_state(
                                state_machine_type,
                                state_type,
                                state.get("Next"),
                                event
                            )
                            self.event_dispatcher.acknowledge(id)
                    except IntrinsicFailure as e:
                        handle_error("States.IntrinsicFailure", str(e))
                        self.event_dispatcher.acknowledge(id)
                    except PathMatchFailure as e:
                        handle_error("States.Runtime", str(e))
                        self.event_dispatcher.acknowledge(id)
                    except ResultPathMatchFailure as e:
                        handle_error("States.ResultPathMatchFailure", str(e))
                        self.event_dispatcher.acknowledge(id)


            try:
                input = apply_path(data, context, state.get("InputPath", "$"))

                """
                The Task State (identified by "Type":"Task") causes the interpreter
                to execute the work identified by the state’s “Resource” field.

                A Task State MUST include a “Resource” field, whose value MUST
                be a URI that uniquely identifies the specific task to execute.
                The States language does not constrain the URI scheme nor any
                other part of the URI.
                """
                resource_arn = state.get("Resource")

                """
                https://states-language.net/spec.html#using-paths

                If the “Parameters” field is provided, its value, after the
                extraction and embedding, becomes the effective input.
                """
                parameters = evaluate_payload_template(
                    input, context, state.get("Parameters")
                )

                """
                Tasks can optionally specify timeouts. Timeouts (the
                “TimeoutSeconds” and “HeartbeatSeconds” fields) are specified
                in seconds and MUST be positive integers. If provided, the
                “HeartbeatSeconds” interval MUST be smaller than the
                “TimeoutSeconds” value.

                If the state runs longer than the specified timeout, or if
                more time than the specified heartbeat elapses between
                heartbeats from the task, then the interpreter fails the state
                with a States.Timeout Error Name.

                Get the execution and state entry timestamps and the execution
                and state timeout values (if present) and use those to compute
                the Task expiry time that will be checked against the current
                time each heartbeat and will eventually result in the Task
                returning a States.Timeout Error if it expires before it
                completes. The default State Machine execution timeout is
                1 year 60*60*24*365 = 31536000
                https://docs.aws.amazon.com/step-functions/latest/dg/limits.html
                The default Task state timeout is more confusing as this document
                https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-task-state.html says: "If not provided, the default value is 99999999" whereas
                https://states-language.net/spec.html#task-state says "If not
                provided, the default value of “TimeoutSeconds” is 60."
                """
                current_timestamp = time.time()

                start_time = context["Execution"].get("StartTime")
                execution_timestamp = parse_rfc3339_datetime(start_time).timestamp()
                execution_timeout = ASL.get("TimeoutSeconds", 31536000)
                t1 = (execution_timestamp + execution_timeout - current_timestamp) * 1000
                # Negative timeouts could occur if messages are redelivered or
                # backlogged so we set them to 0 if that happens.
                t1 = t1 if t1 > 0 else 0

                entered_time = context["State"].get("EnteredTime")
                state_timestamp = parse_rfc3339_datetime(entered_time).timestamp()

                state_timeout = state.get("TimeoutSecondsPath")
                if state_timeout:
                    state_timeout = apply_path(data, context, state_timeout)
                    state_timeout = state_timeout if isinstance(state_timeout, int) else 0
                else:
                    state_timeout = state.get("TimeoutSeconds", 99999999)
                    #state_timeout = state.get("TimeoutSeconds", 60)  # 60s default
                    #state_timeout = state.get("TimeoutSeconds", 10)  # 10s default

                t2 = (state_timestamp + state_timeout - current_timestamp) * 1000
                # Negative timeouts could occur if messages are redelivered or
                # backlogged so we set them to 0 if that happens.
                t2 = t2 if t2 > 0 else 0

                # The computed timeout is the lowest of the Execution timeout
                # (t1) or the State timeout (t2). 
                timeout = t1 if t1 < t2 else t2

                self.task_dispatcher.execute_task(
                    resource_arn, parameters, on_response, timeout, context
                )
            except IntrinsicFailure as e:
                handle_error("States.IntrinsicFailure", str(e))
                self.event_dispatcher.acknowledge(id)
            except PathMatchFailure as e:
                handle_error("States.Runtime", str(e))
                self.event_dispatcher.acknowledge(id)

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
            try:
                input = apply_path(data, context, state.get("InputPath", "$"))
            except PathMatchFailure as e:
                handle_error("States.Runtime", str(e))
                self.event_dispatcher.acknowledge(id)
                return

            """
            The choose function implements the actual choice logic. We must
            first extract the Variable field and use its value as JSONPath to
            scan the effective input for the actual value the we wish to match.
            """
            def choose(choice):
                """
                "Variable" field may be None for And, Or, Not choice rules
                The throw_exception_on_failed_match flag provides a way to
                test for a Variable/path actually being present.
                """
                path_match_failed = False
                try:
                    variable = apply_path(
                        input,
                        context,
                        choice.get("Variable"),
                        throw_exception_on_failed_match=True
                    )
                except PathMatchFailure:
                    variable = False
                    path_match_failed = True

                next = choice.get("Next", True)


                def isnumber(x):
                    # General test for numeric - any number x zero is zero
                    try:
                        return not isinstance(x, bool) and 0 == x * 0
                    except:
                        return False

                def next_if(variable, op, value, comp_type):
                    # Boiler plate test
                    if (
                        isinstance(variable, comp_type)
                        and isinstance(value, comp_type)
                        and op(variable, value)
                    ):
                        return next

                def next_if_numeric(variable, op, value):
                    # Boiler plate test
                    if (
                        isnumber(variable)
                        and isnumber(value)
                        and op(variable, value)
                    ):
                        return next

                def next_if_timestamp(variable, op, value):
                    """
                    Boiler plate test. The ASL spec. is a little vague on
                    timestamps. The approach we take here is to parse the
                    rfc3339 string into an epoch timestamp and perform the
                    comparisons on those. The thinking with that is because
                    different rfc3339 representations can resolve to the
                    same actual time e.g. a representation in Zulu or local
                    time plus offset can both refer to the same time.
                    """
                    variable = parse_rfc3339_datetime(variable).timestamp()
                    value = parse_rfc3339_datetime(value).timestamp()
                    if op(variable, value):
                        return next
    
                def asl_choice_And(value):
                    if all(choose(ch) for ch in value):
                        return next

                def asl_choice_Or(value):
                    if any(choose(ch) for ch in value):
                        return next

                def asl_choice_Not(value):
                    if (not choose(value)):
                        return next

                def asl_choice_BooleanEquals(value):
                    return next_if(variable, operator.eq, value, bool)

                def asl_choice_NumericEquals(value):
                    return next_if_numeric(variable, operator.eq, value)

                def asl_choice_NumericGreaterThan(value):
                    return next_if_numeric(variable, operator.gt, value)

                def asl_choice_NumericGreaterThanEquals(value):
                    return next_if_numeric(variable, operator.ge, value)

                def asl_choice_NumericLessThan(value):
                    return next_if_numeric(variable, operator.lt, value)

                def asl_choice_NumericLessThanEquals(value):
                    return next_if_numeric(variable, operator.le, value)

                def asl_choice_StringEquals(value):
                    return next_if(variable, operator.eq, value, str)

                def asl_choice_CaseInsensitiveStringEquals(value):
                    # Not covered in ASL spec. but useful and trivial to handle.
                    return next_if(variable.lower(), operator.eq, value.lower(), str)

                def asl_choice_StringGreaterThan(value):
                    return next_if(variable, operator.gt, value, str)

                def asl_choice_StringGreaterThanEquals(value):
                    return next_if(variable, operator.ge, value, str)

                def asl_choice_StringLessThan(value):
                    return next_if(variable, operator.lt, value, str)

                def asl_choice_StringLessThanEquals(value):
                    return next_if(variable, operator.le, value, str)

                def asl_choice_StringMatches(value):
                    # Change the \ escape to fnmatch [seq] escape and also
                    # escape [ to allow things like a literal [hello]
                    value = value.replace("[", "[[]").replace("\*", "[*]")
                    if fnmatch.fnmatch(variable, value):
                        return next

                def asl_choice_TimestampEquals(value):
                    return next_if_timestamp(variable, operator.eq, value)

                def asl_choice_TimestampGreaterThan(value):
                    return next_if_timestamp(variable, operator.gt, value)

                def asl_choice_TimestampGreaterThanEquals(value):
                    return next_if_timestamp(variable, operator.ge, value)

                def asl_choice_TimestampLessThan(value):
                    return next_if_timestamp(variable, operator.lt, value)

                def asl_choice_TimestampLessThanEquals(value):
                    return next_if_timestamp(variable, operator.le, value)

                def asl_choice_IsBoolean(value):
                    if not path_match_failed and (isinstance(variable, bool) == value):
                        return next

                def asl_choice_IsNull(value):
                    if ((variable is None) == value):
                        return next

                def asl_choice_IsNumeric(value):
                    if (isnumber(variable) == value):
                        return next

                def asl_choice_IsString(value):
                    if (isinstance(variable, str) == value):
                        return next

                def asl_choice_IsPresent(value):
                    if (not path_match_failed == value):
                        return next

                def asl_choice_IsTimestamp(value):
                    try:
                        parse_rfc3339_datetime(variable)
                        if (value):
                            return next
                    except Exception:
                        if (not value):
                            return next

                for key in choice:
                    """
                    Determine the ASL choice operator of the current choice and
                    use that to dynamically invoke the appropriate choice handler.
                    """
                    try:
                        value = choice[key]
                        key = "asl_choice_" + key
                        if key.endswith("Path"):  # Handle variable to variable comparison
                            # Slice off "Path" suffix and get value from path
                            key = key[:-4]
                            value = apply_path(data, context, value)

                        next_state = locals().get(key, lambda: None)(value)
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

            try:
                event["data"] = apply_path(input, context, state.get("OutputPath", "$"))
            except PathMatchFailure as e:
                handle_error("States.Runtime", str(e))
                self.event_dispatcher.acknowledge(id)
                return

            """
            The interpreter will raise a run-time States.NoChoiceMatched error
            if a “Choice” state fails to match a Choice Rule and no “Default”
            transition was specified. 
            """
            if next_state:
                self.change_state(
                    state_machine_type,
                    state_type,
                    next_state,
                    event
                )
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
            try:
                input = apply_path(data, context, state.get("InputPath", "$"))
            except PathMatchFailure as e:
                handle_error("States.Runtime", str(e))
                self.event_dispatcher.acknowledge(id)
                return

            """
            It's important for this function to be nested as we want the event,
            state and id to be wrapped in its closure, to be used when the
            timeout actually fires.
            """
            def on_timeout():
                try:
                    event["data"] = apply_path(input, context, state.get("OutputPath", "$"))
                    if state.get("End"):
                        handle_terminal_state(state_type, event, id)
                    else:
                        self.change_state(
                            state_machine_type,
                            state_type,
                            state.get("Next"),
                            event
                        )
                        self.event_dispatcher.acknowledge(id)
                except PathMatchFailure as e:
                    handle_error("States.Runtime", str(e))
                    self.event_dispatcher.acknowledge(id)

            """
            Calculate the difference between supplied rfc3339 and current time.
            """
            def get_timeout_from_rfc3339_datetime(rfc3339):
                try:
                    target_timestamp = parse_rfc3339_datetime(rfc3339).timestamp()
                    current_timestamp = time.time()
                    return (target_timestamp - current_timestamp) * 1000
                except ValueError as e:
                    self.logger.warning(
                        "timestamp {} failed to parse correctly, "
                        "defaulting to zero delay".format(rfc3339)
                    )
                    return 0

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

            try:
                timeout = 0
                if seconds:
                    current_timestamp = time.time()
                    timeout = (state_timestamp + seconds - current_timestamp) * 1000
                elif seconds_path:
                    seconds = apply_path(input, context, seconds_path)
                    if seconds:
                        current_timestamp = time.time()
                        timeout = (state_timestamp + seconds - current_timestamp) * 1000
                elif timestamp:
                    timeout = get_timeout_from_rfc3339_datetime(timestamp)
                elif timestamp_path:
                    timestamp = apply_path(input, context, timestamp_path)
                    if timestamp:
                        timeout = get_timeout_from_rfc3339_datetime(timestamp)

                # Negative timeouts could occur if messages are redelivered or
                # backlogged so we set them to 0 if that happens.
                timeout = timeout if timeout > 0 else 0

                """
                Schedule the timeout. This is slightly subtle, the idea is that
                the event instance, state and id for this call are wrapped in
                the on_timeout function's closure, so when the timeout fires the
                correct event should be published and the correct id acknowledged.
                """
                self.event_dispatcher.set_timeout(on_timeout, timeout)
            except PathMatchFailure as e:
                handle_error("States.Runtime", str(e))
                self.event_dispatcher.acknowledge(id)

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
            try:
                input = apply_path(data, context, state.get("InputPath", "$"))
                event["data"] = apply_path(input, context, state.get("OutputPath", "$"))
                handle_terminal_state(state_type, event, id)
            except PathMatchFailure as e:
                handle_error("States.Runtime", str(e))
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

            Fail states don't allow InputPath, OutputPath or ResultPath
            """
            event["data"] = {
                "Error": state.get("Error", "Unspecified"),
                "Cause": state.get("Cause", "Unspecified"),
            }

            handle_terminal_state(state_type, event, id)

        def asl_state_Parallel():
            """
            https://states-language.net/spec.html#parallel-state
            https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-parallel-state.html

            A Parallel state causes the interpreter to execute each branch
            starting with the state named in its “StartAt” field, as concurrently
            as possible.

            The "wait until each branch terminates (reaches a terminal state)
            before processing the Parallel state's “Next” field." part of the 
            Parallel state is implemented in asl_state_collect_results()

            The Parallel state passes its input (potentially as filtered by the
            “InputPath” field) as the input to each branch’s “StartAt” state.
            """
            try:
                input = apply_path(data, context, state.get("InputPath", "$"))

                """
                https://states-language.net/spec.html#using-paths

                If the “Parameters” field is provided, its value, after the
                extraction and embedding, becomes the effective input.
                """
                parameters = evaluate_payload_template(
                    input, context, state.get("Parameters")
                )

                """
                A Parallel State MUST contain a field named “Branches” which
                is an array whose elements MUST be objects. Each object MUST
                contain fields named “States” and “StartAt” whose meanings are
                exactly like those in the top level of a State Machine.

                Iterate through the branches and launch each branch State Machine.
    
                The index of each branch and the Parallel state's raw input is
                needed to create the final result, so we have to store those
                in the context of the events traversing the branch state
                machines. Moreover, it is possible that we could have Parallel
                or Map states nested in the branch state machines. To cater for
                this we add a "Branch" field to the "State" object in the
                context. The "Branch" field is a list of objects holding Input
                and Index as necessary. Each nested Map or Parallel state can
                append to that list, which behaves like a stack.
                """
                context_state = context["State"]
                if not "Branch" in context_state:
                    context_state["Branch"] = []
                context_state["Branch"].append({})

                branches = state.get("Branches", [])
                for index, branch in enumerate(branches):
                    event["data"] = parameters
                    branch_info = {
                        "Parent": current_state,
                        "Input": data,  # Save the raw input to the Parallel state
                        "Index": index, # Index of the current branch
                    }

                    context_state["Name"] = branch.get("StartAt")
                    context_state["EnteredTime"] = (
                        datetime.now(timezone.utc).astimezone().isoformat()
                    )
                    context_state["Branch"][-1] = branch_info

                    self.event_dispatcher.publish(event)

                self.event_dispatcher.acknowledge(id)
            except IntrinsicFailure as e:
                handle_error("States.IntrinsicFailure", str(e))
                self.event_dispatcher.acknowledge(id)
            except PathMatchFailure as e:
                handle_error("States.Runtime", str(e))
                self.event_dispatcher.acknowledge(id)

        def get_start_index(context):
            """
            Boilerplate to retrieve the "Start" index of the Map Iterator. This
            field is used in the implementation of MaxConcurrency. The idea is
            when we process the Map state we launch MaxConcurrency child state
            machine executions and when those complete we re-enter the Map state
            and process the next block of MaxConcurrency, and so on. The "Start"
            field, if present, holds the index of the start of the next block
            to be processed. The "Branch" metadata is a list so we can handle
            the case of nested Map and Parallel states.
            """
            start = 0
            context_state = context["State"]
            if "Branch" in context_state and len(context_state["Branch"]):
                start = context_state["Branch"][-1].get("Start", 0)
            return start

        def asl_state_Map():
            """
            https://states-language.net/spec.html#map-state
            https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-map-state.html

            A Map State causes the interpreter to process all the elements of an
            array, potentially in parallel, with the processing of each element
            independent of the others. The specification uses the term “iteration”
            to describe each such nested execution.

            The Parallel state applies multiple different state-machine branches
            to the same input, while the Map state applies a single state machine
            to multiple input elements.

            The “InputPath” field operates as usual, selecting part of the raw input .
            """
            try:
                input = apply_path(data, context, state.get("InputPath", "$"))

                """
                The “ItemsPath” field’s value is a reference path identifying
                where in the effective input the array field is found.

                The default value of “ItemsPath” is “$”, which is to say the
                whole effective input. So, if a Map State has neither an
                “InputPath” nor a “ItemsPath” field, it is assuming that the
                raw input to the state will be a JSON array.
                """
                items_path = apply_path(input, context, state.get("ItemsPath", "$"))
                if not isinstance(items_path, list):
                    items_path = []

                length = len(items_path)

                """
                A Map State MUST contain an object field named “Iterator” which
                MUST contain fields named “States” and “StartAt”, whose meanings
                are exactly like those in the top level of a State Machine.

                The “Iterator” field’s value is an object that defines a state
                machine which will process each element of the array.
                """
                iterator = state.get("Iterator", {})

                """
                The “MaxConcurrency” field’s value is an integer that provides
                an upper bound on how many invocations of the Iterator may run
                in parallel.
                """
                max_concurrency = state.get("MaxConcurrency", 0)
                if max_concurrency == 0:
                    max_concurrency = length

                """
                Save these values from the Map state. We want to use these
                values in the context evaluated by the parameter evaluation.
                """
                context_state = context["State"]
                map_state_name = context_state["Name"]
                map_state_entered = context_state["EnteredTime"]

                """
                Iterate through the items_path and launch each item's State
                Machine. The index and value of each item is needed to evaluate
                the parameter, so we store it in the "Map" field of the context
                as described in the AWS documentation for the context object.
                https://docs.aws.amazon.com/step-functions/latest/dg/input-output-contextobject.html

                Also, like the Parallel state, the index of each branch and the
                Map state's raw input is needed to create the final result, so
                we again add a "Branch" field to the "State" object in the context.

                The start variable is used with MaxConcurrency. We collect
                results for each "block" of MaxConcurrency results and when all
                have been returned we publish a new event to re-enter the Map
                state, with a start incremented by MaxConcurrency. We carry on
                collecting blocks of MaxConcurrency and re-entering the Map
                state with updated start until we have finished processing
                all items in items_path.
                """
                if length and not "Branch" in context_state:
                    context_state["Branch"] = []

                start = get_start_index(context)
                if length and start == 0:
                    context_state["Branch"].append({})

                end = min(start + max_concurrency, length)

                """
                Need to iterate through the items_path list between start and
                end indices and also retrieve the current index. This is quite
                fiddly in Python when compared to doing similar in lower level
                languages. 
                https://stackoverflow.com/questions/34384523/how-can-loop-through-a-list-from-a-certain-index
                """
                for index, item in enumerate(items_path[start:end], start=start):
                    """
                    Ensure the context evaluated by parameter evaluation is that
                    of the parent Map state not the new Iterator StartAt state.
                    """
                    context_state["Name"] = map_state_name
                    context_state["EnteredTime"] = map_state_entered

                    params = state.get("Parameters")
                    if params:
                        # Store the index and value in the context as described above.
                        context["Map"] = {
                            "Item": {
                                "Index": index,
                                "Value": item,
                            },
                        }

                        """
                        https://states-language.net/spec.html#using-paths

                        If the “Parameters” field is provided, its value, after
                        extraction and embedding, becomes the effective input.
                        """
                        parameters = evaluate_payload_template(
                            input, context, params
                        )

                        del context["Map"]  # Delete after parameters have been processed
                    else:
                        """
                        If no parameters are supplied the effective input to the
                        iteration is the current item i.e $$.Map.Item.Value
                        """
                        parameters = item

                    event["data"] = parameters
                    branch_info = {
                        "Parent": current_state,
                        "Input": data,     # Save the raw input to the Map state
                        "Index": index,    # Index of the current branch
                        "Length": length,  # Number of branches/Iterator instances
                    }
                    if start:
                        branch_info["Start"] = start

                    context_state["Name"] = iterator.get("StartAt")
                    context_state["EnteredTime"] = (
                        datetime.now(timezone.utc).astimezone().isoformat()
                    )
                    context_state["Branch"][-1] = branch_info

                    self.event_dispatcher.publish(event)

                """
                It's not clear what the correct course of action is when the
                effective input is empty, none of the documentation covers this.
                Most likely the result is just an empty array, need to check...
                """
                if length:
                    self.event_dispatcher.acknowledge(id)
                else:
                    """
                    https://states-language.net/spec.html#using-paths

                    The value of "ResultSelector" MUST be a Payload Template,
                    whose input is the result, and whose payload replaces and
                    becomes the effective result.
                    """
                    result = evaluate_payload_template(
                        [], context, state.get("ResultSelector")
                    )

                    # Parallel and Map states apply ResultPath to "raw input"
                    event["data"] = merge_result(data, context, result, state)

                    if state.get("End"):
                        handle_terminal_state(state_type, event, id)
                    else:
                        self.change_state(
                            state_machine_type,
                            state_type,
                            state.get("Next"),
                            event
                        )
                        self.event_dispatcher.acknowledge(id)
            except IntrinsicFailure as e:
                handle_error("States.IntrinsicFailure", str(e))
                self.event_dispatcher.acknowledge(id)
            except PathMatchFailure as e:
                handle_error("States.Runtime", str(e))
                self.event_dispatcher.acknowledge(id)
            except ResultPathMatchFailure as e:
                handle_error("States.ResultPathMatchFailure", str(e))
                self.event_dispatcher.acknowledge(id)

        def asl_state_collect_results(state_type):
            """
            Collect the results from the branches of Parallel and Map states.
            Wait until every branch terminates (reaches a terminal state) before
            processing the Parallel or Map state's “Next” field.
            """
            # Get data object from event again to ensure we have result not input.
            data = event["data"]

            # Retrieve the index, input, length, etc. info for the current branch.
            branch_info = context["State"]["Branch"][-1]  # Top of stack (last item)
            index = branch_info["Index"]

            current_state = branch_info["Parent"]  # Get parent info from "stack"

            state, current_state_machine = find_state(ASL["States"], current_state)
            if state == None:  # state should be valid by this point
                self.log_and_drop("State {} does not exist", current_state, id)
                return

            previous_state_type = state_type
            state_type = state["Type"]

            execution_arn = context["Execution"]["Id"]
            # Initialise branch_results if necessary
            if not execution_arn in self.branch_results:
                self.branch_results[execution_arn] = {}
            if not current_state in self.branch_results[execution_arn]:
                if "Branches" in state:  # Parallel
                    length = len(state["Branches"])
                else:  # Map
                    length = branch_info["Length"]

                self.branch_results[execution_arn][current_state] = {
                    "results": [None]*length,
                    "ids": [None]*length,
                }

            """
            Record the event id so we can acknowledge the event when the
            results from all branches have eventually been returned.
            """
            branch_results = self.branch_results[execution_arn][current_state]
            result = branch_results["results"]
            event_ids = branch_results["ids"]
            result[index] = data
            if previous_state_type != "Parallel" and previous_state_type != "Map":
                event_ids[index] = id

            #print("asl_state_collect_results")
            #print(result)
            
            #print("----------")
            #print(context["State"]["Branch"])
            #print("----------")

            """
            If we haven't yet received the results from all branches check if
            MaxConcurrency has been set and if it has check if we've received
            all of the results for the current block of MaxConcurrency. If we
            haven't then simply return, but if we have then create and publish
            an event to re-enter the Map state with an updated "Start" index
            in order to process the next block of MaxConcurrency.
            """
            if None in result:
                max_concurrency = state.get("MaxConcurrency", 0)
                if max_concurrency:
                    start = branch_info.get("Start", 0)
                    end = min(start + max_concurrency, len(result))

                    partial = result[start:end]
                    if not None in partial:
                        """
                        If we've got all results for a batch of max_concurrency
                        send an event to re-enter the Map state and trigger
                        processing of the next batch.
                        """
                        event["data"] = branch_info["Input"]  # Get saved raw input
                        context["State"]["Name"] = current_state
                        context["State"]["Branch"][-1] = {"Start": end}

                        self.event_dispatcher.publish(event)

                return

            """
            Remove last item from the "Branch" list, then if it becomes empty
            delete "Branch" from context as we've finished with it.
            """
            del context["State"]["Branch"][-1]
            if len(context["State"]["Branch"]) == 0:
                del context["State"]["Branch"]

            context["State"]["Name"] = current_state

            # Parallel and Map states apply ResultPath to "raw input"
            data = branch_info["Input"]  # Get saved raw input

            """
            https://states-language.net/spec.html#using-paths

            The value of "ResultSelector" MUST be a Payload Template, whose
            input is the result, and whose payload replaces and becomes the
            effective result.
            """
            try:
                result = evaluate_payload_template(
                    result, context, state.get("ResultSelector")
                )

                event["data"] = merge_result(data, context, result, state)
            except IntrinsicFailure as e:
                handle_error("States.IntrinsicFailure", str(e))
                # Acknowledge the events for each branch's terminal state
                for event_id in event_ids:
                    if event_id:
                        self.event_dispatcher.acknowledge(event_id)
                return
            except PathMatchFailure as e:
                handle_error("States.Runtime", str(e))
                # Acknowledge the events for each branch's terminal state
                for event_id in event_ids:
                    if event_id:
                        self.event_dispatcher.acknowledge(event_id)
                return
            except ResultPathMatchFailure as e:
                handle_error("States.ResultPathMatchFailure", str(e))
                # Acknowledge the events for each branch's terminal state
                for event_id in event_ids:
                    if event_id:
                        self.event_dispatcher.acknowledge(event_id)
                return

            """
            Publish any new state change before acknowledging the events.
            """
            if not state.get("End"):
                self.change_state(
                    state_machine_type,
                    state_type,
                    state.get("Next"),
                    event
                )

            # Acknowledge the events for each branch's terminal state
            for event_id in event_ids:
                if event_id:
                    self.event_dispatcher.acknowledge(event_id)

            """
            Need to do this *after* acknowledging the events as it deletes the
            Parallel or Map branch results for the current execution.
            """
            if state.get("End"):
                handle_terminal_state(state_type, event)


        """
        End of nested state handler functions.
        """
        # ----------------------------------------------------------------------


        """
        Update the execution history with StateEntered information. The test
        for RetryCount is because when we trigger a retry we do so by publishing
        an event to the message queue that triggers a transition back to the
        state being retried, but as this represents a *retry* and not an actual
        entry to the state we want to suppress the history update.
        Similarly, the test for reentered_map is because if MaxConcurrency is
        set we will re-enter the Map state, possibly several times, to process
        the next batch of items so again we want to suppress the history update.
        """
        execution_arn = context["Execution"]["Id"]
        reentered_map = state_type == "Map" and get_start_index(context) != 0
        if not context["State"].get("RetryCount") and not reentered_map:
            self.update_execution_history(
                state_machine_type,
                execution_arn,
                state_type + "StateEntered",
                {"input": json.dumps(data), "name": current_state},
            )

        """
        We check the execution history hasn't exceeded the 25000 event limit in
        https://docs.aws.amazon.com/step-functions/latest/dg/limits.html
        here rather than in update_execution_history, because here we can more
        cleanly call handle_error() and fail the execution in a way that is
        consistent with other failures. Note that this approach may result in
        the stored history being a few events more than the actual limit. Doing
        the test here also means that ExecutionSucceeded/ExecutionFailed events
        won't cause the execution history limit to be exceeded, as failing the
        execution on those events doesn't seem particularly sensible.
        The error code of States.ExecutionHistoryLimitExceeded is chosen to
        mirror the code States.DataLimitExceeded used when the input or output
        size has exceeded their limit, however the actual error code used for
        this failure unfortunately doesn't seem to be documented anywhere.
        """
        if state_machine_type == "STANDARD":
            execution_history_length = len(self.execution_history[execution_arn])
            #print(execution_history_length)
            if execution_history_length > MAX_EXECUTION_HISTORY_LENGTH:
                message = ("Execution '{}' has a history of {} events and has "
                           "exceeded the maximum execution history size.").format(
                            execution_arn, execution_history_length
                          )
                self.logger.info(message)
                handle_error("States.ExecutionHistoryLimitExceeded", message)
                self.event_dispatcher.acknowledge(id)
                return

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

