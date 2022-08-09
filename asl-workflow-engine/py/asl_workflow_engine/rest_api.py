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

"""
This implements the REST API for the ASL Workflow Engine. The intention is to
implement the AWS Step Functions API as described in the AWS documentation:
https://docs.aws.amazon.com/step-functions/latest/apireference/API_Operations.html

By implementing the AWS REST API semantics it becomes possible to use Amazon's
CLI and SDK so applications can use this ASL Workflow Engine as an alternative
to Amazon's for scenarios such as hybrid cloud workloads.


Example actions using AWS CLI:

# List state machines
aws stepfunctions --endpoint http://localhost:4584 list-state-machines --max-results 20

# Create a new state machine
aws stepfunctions --endpoint http://localhost:4584 create-state-machine --name my-state-machine --definition '{"Comment":"A Hello World example of the Amazon States Language using a Pass state","StartAt":"HelloWorld","States":{"HelloWorld":{"Type":"Pass","End":true}}}' --role-arn arn:aws:iam::0123456789:role/service-role/MyRole

# Create a new state machine from a file
aws stepfunctions --endpoint http://localhost:4584 create-state-machine --name simple_state_machine --definition file://<path-to-ASL-JSON> --role-arn arn:aws:iam::0123456789:role/service-role/MyRole

# Update a state machine
aws stepfunctions --endpoint http://localhost:4584 update-state-machine --definition '{"Comment":"A Hello World example of the Amazon States Language using a Pass state","StartAt":"HelloWorld","States":{"HelloWorld":{"Type":"Pass","End":true}}}' --role-arn arn:aws:iam::0123456789:role/service-role/MyRole --state-machine-arn arn:aws:states:local:0123456789:stateMachine:my-state-machine

# Describe state machine
aws stepfunctions --endpoint http://localhost:4584 describe-state-machine --state-machine-arn arn:aws:states:local:0123456789:stateMachine:my-state-machine

# Delete state machine
aws stepfunctions --endpoint http://localhost:4584 delete-state-machine --state-machine-arn arn:aws:states:local:0123456789:stateMachine:my-state-machine

# Start state machine execution
aws stepfunctions --endpoint http://localhost:4584 start-execution --state-machine-arn arn:aws:states:local:0123456789:stateMachine:my-state-machine --name my-execution --input '{"comment":"I am a great input !"}'

# List state machine executions
aws stepfunctions --endpoint http://localhost:4584 list-executions --state-machine-arn arn:aws:states:local:0123456789:stateMachine:my-state-machine

# Describe execution
aws stepfunctions --endpoint http://localhost:4584 describe-execution --execution-arn arn:aws:states:local:0123456789:execution:my-state-machine:my-execution

# Describe state machine related to execution
aws stepfunctions --endpoint http://localhost:4584 describe-state-machine-for-execution --execution-arn arn:aws:states:local:0123456789:execution:my-state-machine:my-execution

# Get execution history
aws stepfunctions --endpoint http://localhost:4584 get-execution-history --execution-arn arn:aws:states:local:0123456789:execution:my-state-machine:my-execution
"""

import sys
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3

import re, time, uuid, logging, opentracing
from datetime import datetime, timezone
from flask import Flask, escape, request, jsonify, abort

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.open_tracing_factory import span_context, inject_span
from asl_workflow_engine.arn import *
from asl_workflow_engine.state_engine import MAX_DATA_LENGTH, MAX_STATE_MACHINE_LENGTH

try:  # Attempt to use ujson if available https://pypi.org/project/ujson/
    import ujson as json
except:  # Fall back to standard library json
    import json


def valid_name(name):
    return (
        isinstance(name, str)
        and len(name) > 0
        and len(name) < 81
        and not re.search(r"^.*[ <>{}[\]?*\"#%\\^|~`$&,;:/].*$", name)
    )

def valid_role_arn(arn):
    return (
        isinstance(arn, str)
        and len(arn) > 0
        and len(arn) < 257
        and re.search(r"^arn:aws:iam::[0-9]+:role\/.+$", arn)
    )

def valid_state_machine_arn(arn):
    return (
        isinstance(arn, str)
        and len(arn) > 0
        and len(arn) < 257
        and re.search(r"^arn:aws:states:.+:[0-9]+:stateMachine:.+$", arn)
    )

def valid_execution_arn(arn):
    return (
        isinstance(arn, str)
        and len(arn) > 0
        and len(arn) < 257
        and re.search(r"^arn:aws:states:.+:[0-9]+:execution:.+$", arn)
    )

def aws_error(code, message=None):
    """
    Boiler plate to return errors in the correct form for the SDKs to throw
    the expected exceptions. The format doesn't seem to be documented anywhere
    so this was grokked by looking at the botocore source code in
    https://github.com/boto/botocore/blob/develop/botocore/parsers.py
    BaseJSONParser._do_error_parse(self, response, shape)
    """
    return jsonify({
        "__type": code,
        "message": message if message else code,
    })

class RestAPI(object):
    def __init__(self, state_engine, event_dispatcher, config):
        """
        """
        self.logger = init_logging(log_name="asl_workflow_engine")
        self.logger.info("Creating {}.RestAPI, using {} JSON parser".format(
            __name__, json.__name__
        ))

        config = config.get("rest_api")
        if config:
            self.host = config.get("host", "0.0.0.0")
            self.port = config.get("port", 4584)
            self.region = config.get("region", "local")

        self.asl_store = state_engine.asl_store
        self.executions = state_engine.executions
        self.execution_history = state_engine.execution_history
        self.event_dispatcher = event_dispatcher

    def create_app(self):
        app = Flask(__name__)

        # Turn off Flask standard logging
        app.logger.disabled = True
        log = logging.getLogger("werkzeug")
        log.disabled = True

        """
        Flask "catch-all" URL
        see https://gist.github.com/fitiavana07/bf4eb97b20bbe3853681e153073c0e5e
        """
        @app.route("/", defaults={"path": ""}, methods=["POST"])
        @app.route("/<path:path>", methods=["POST"])
        def handle_post(path):
            """
            Perform initial validation of the HTTP request. The AWS Step 
            Functions API is a slightly "weird" REST API as it mostly seems to
            rely on POST and rather than using HTTP resources it uses the
            x-amz-target header to specify the action to be performed.
            """
            if not request.content_type == "application/x-amz-json-1.0":
                return "Unexpected Content-Type {}".format(request.content_type), 400

            target = request.headers.get("x-amz-target")
            if not target:
                return "Missing header x-amz-target", 400
            if not target.startswith("AWSStepFunctions."):
                return "Malformed header x-amz-target", 400

            action = target.split(".")[1]
            # print(action)

            data = request.data

            try:
                params = json.loads(data.decode("utf8"))
            except ValueError as e:
                params = ""
                self.logger.error(
                    "Message body {} does not contain valid JSON".format(data)
                )

            # ------------------------------------------------------------------

            """
            Define nested functions as handlers for each supported API action.
            Using nested functions so we can use the context from handle_post.

            That the methods are prefixed with "aws_api_" is a mitigation against
            accidentally or deliberately placing an invalid action in the API.
            """
            def aws_api_CreateStateMachine():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_CreateStateMachine.html
                """
                name = params.get("name")
                if not valid_name(name):
                    self.logger.warning(
                        "RestAPI CreateStateMachine: {} is an invalid name".format(name)
                    )
                    return aws_error("InvalidName"), 400

                role_arn = params.get("roleArn")
                if not valid_role_arn(role_arn):
                    self.logger.warning(
                        "RestAPI CreateStateMachine: {} is an invalid Role ARN".format(
                            role_arn
                        )
                    )
                    return aws_error("InvalidArn"), 400

                # Form stateMachineArn from roleArn and name
                arn = parse_arn(role_arn)
                state_machine_arn = create_arn(
                    service="states",
                    region=self.region,
                    account=arn["account"],
                    resource_type="stateMachine",
                    resource=name,
                )

                # Get State Machine type (STANDARD or EXPRESS) if supplied
                type = params.get("type", "STANDARD")
                if type not in {"STANDARD", "EXPRESS"}:
                    self.logger.error(
                        "RestAPI CreateStateMachine: State Machine type {} "
                        "is not supported".format(type)
                    )
                    return aws_error("StateMachineTypeNotSupported"), 400

                """
                Look up stateMachineArn. Use get() not get_cached_view() here as
                calls to CreateStateMachine might reasonably *expect* no match.
                """
                match = self.asl_store.get(state_machine_arn)
                if match:
                    # Info seems more appropriate than error here as creation is
                    # an idempotent action.
                    self.logger.info(
                        "RestAPI CreateStateMachine: State Machine {} already exists".format(
                            state_machine_arn
                        )
                    )
                    return aws_error("StateMachineAlreadyExists"), 400

                definition = params.get("definition", "")
                """
                First check if the definition length has exceeded the 1048576
                character limit described in the CreateStateMachine API page.
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_CreateStateMachine.html
                """
                if len(definition) == 0 or len(definition) > MAX_STATE_MACHINE_LENGTH:
                    self.logger.error(
                        "RestAPI CreateStateMachine: Invalid definition size for State Machine '{}'.".format(name)
                    )
                    return aws_error("InvalidDefinition"), 400

                try:
                    definition = json.loads(definition)
                except ValueError as e:
                    definition = None
                    self.logger.error(
                        "RestAPI CreateStateMachine: State Machine definition {} does not contain valid JSON".format(
                            params.get("definition")
                        )
                    )
                    return aws_error("InvalidDefinition"), 400

                if not (name and role_arn and definition):
                    self.logger.warning(
                        "RestAPI CreateStateMachine: name, roleArn and definition must be specified"
                    )
                    return aws_error("MissingRequiredParameter"), 400

                # TODO ASL Validator??

                creation_date = time.time()
                self.asl_store[state_machine_arn] = {
                    "creationDate": creation_date,
                    "definition": definition,
                    "name": name,
                    "roleArn": role_arn,
                    "stateMachineArn": state_machine_arn,
                    "updateDate": creation_date,
                    "status": "ACTIVE",
                    "type": type,
                }

                resp = {
                    "creationDate": creation_date,
                    "stateMachineArn": state_machine_arn,
                }

                return jsonify(resp), 200

            def aws_api_ListStateMachines():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_ListStateMachines.html
                """
                # TODO handle nextToken stuff
                next_token = ""

                """
                Populate response using list and dict comprehensions
                https://www.pythonforbeginners.com/basics/list-comprehensions-in-python
                https://stackoverflow.com/questions/5352546/extract-subset-of-key-value-pairs-from-python-dictionary-object
                """
                state_machines = [
                    {
                        k1: v[k1] for k1 in ("creationDate", "name",
                            "stateMachineArn", "type")
                    }
                    for k, v in self.asl_store.items()
                ]

                resp = {
                    "stateMachines": state_machines
                }
                if next_token:
                    resp["nextToken"] = next_token

                return jsonify(resp), 200

            def aws_api_DescribeStateMachine():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeStateMachine.html
                """
                state_machine_arn = params.get("stateMachineArn")
                if not state_machine_arn:
                    self.logger.warning(
                        "RestAPI DescribeStateMachine: stateMachineArn must be specified"
                    )
                    return aws_error("MissingRequiredParameter"), 400

                if not valid_state_machine_arn(state_machine_arn):
                    self.logger.warning(
                        "RestAPI DescribeStateMachine: {} is an invalid State Machine ARN".format(
                            state_machine_arn
                        )
                    )
                    return aws_error("InvalidArn"), 400

                """
                Look up stateMachineArn. Using get_cached_view() here means that
                the state_machine is JSON serialisable, as the cached view is a
                simple dict rather than say a RedisDict.
                """
                state_machine = self.asl_store.get_cached_view(state_machine_arn)
                if not state_machine:
                    self.logger.info(
                        "RestAPI DescribeStateMachine: State Machine {} does not exist".format(
                            state_machine_arn
                        )
                    )
                    return aws_error("StateMachineDoesNotExist"), 400
                
                """
                In the API the "definition" field is actually a string not a
                JSON object, hence the json.dumps() here. We do the conversion
                here rather than storing it as a string because the State Engine
                uses the deserialised definition as a key part of its core
                state transition behaviour.
                """
                resp = state_machine.copy()
                resp["definition"] = json.dumps(state_machine["definition"])

                return jsonify(resp), 200

            def aws_api_DescribeStateMachineForExecution():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeStateMachineForExecution.html
                """
                execution_arn = params.get("executionArn")
                if not execution_arn:
                    self.logger.warning(
                        "RestAPI DescribeStateMachineForExecution: executionArn must be specified"
                    )
                    return aws_error("MissingRequiredParameter"), 400

                if not valid_execution_arn(execution_arn):
                    self.logger.warning(
                        "RestAPI DescribeStateMachineForExecution: {} is an invalid Execution ARN".format(
                            execution_arn
                        )
                    )
                    return aws_error("InvalidArn"), 400

                # Look up executionArn
                execution = self.executions.get(execution_arn)
                if not execution:
                    self.logger.info(
                        "RestAPI DescribeStateMachineForExecution: Execution {} does not exist".format(
                            execution_arn
                        )
                    )
                    return aws_error("ExecutionDoesNotExist"), 400

                state_machine_arn = execution.get("stateMachineArn")

                if not valid_state_machine_arn(state_machine_arn):
                    self.logger.warning(
                        "RestAPI DescribeStateMachineForExecution: {} is an invalid State Machine ARN".format(
                            state_machine_arn
                        )
                    )
                    return aws_error("InvalidArn"), 400

                # Look up stateMachineArn
                state_machine = self.asl_store.get_cached_view(state_machine_arn)
                if not state_machine:
                    self.logger.info(
                        "RestAPI DescribeStateMachineForExecution: State Machine {} does not exist".format(
                            state_machine_arn
                        )
                    )
                    return aws_error("StateMachineDoesNotExist"), 400

                """
                As with DescribeStateMachine the "definition" field is actually
                a string not a JSON object, hence the json.dumps() here.
                """
                resp = {
                    k: state_machine[k] for k in ("definition", "name", "roleArn",
                        "stateMachineArn", "updateDate")
                }
                resp["definition"] = json.dumps(state_machine["definition"])

                return jsonify(resp), 200

            def aws_api_UpdateStateMachine():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_UpdateStateMachine.html
                """
                state_machine_arn = params.get("stateMachineArn")
                if not state_machine_arn:
                    self.logger.warning(
                        "RestAPI UpdateStateMachine: stateMachineArn must be specified"
                    )
                    return aws_error("MissingRequiredParameter"), 400

                if not valid_state_machine_arn(state_machine_arn):
                    self.logger.warning(
                        "RestAPI UpdateStateMachine: {} is an invalid State Machine ARN".format(
                            state_machine_arn
                        )
                    )
                    return aws_error("InvalidArn"), 400

                """
                Look up stateMachineArn. Use get() rather than get_cached_view()
                as we are going to be updating the retrieved State Machine.
                """
                state_machine = self.asl_store.get(state_machine_arn)
                if not state_machine:
                    self.logger.info(
                        "RestAPI UpdateStateMachine: State Machine {} does not exist".format(
                            state_machine_arn
                        )
                    )
                    return aws_error("StateMachineDoesNotExist"), 400

                role_arn = params.get("roleArn")
                if role_arn:
                    if not valid_role_arn(role_arn):
                        self.logger.warning(
                            "RestAPI UpdateStateMachine: {} is an invalid Role ARN".format(
                                role_arn
                            )
                        )
                        return aws_error("InvalidArn"), 400
                    state_machine["roleArn"] = role_arn

                definition = params.get("definition", "")
                if definition:
                    """
                    First check if the definition length has exceeded the 1048576
                    character limit described in the UpdateStateMachine API page.
                    https://docs.aws.amazon.com/step-functions/latest/apireference/API_UpdateStateMachine.html
                    """
                    if len(definition) == 0 or len(definition) > MAX_STATE_MACHINE_LENGTH:
                        self.logger.error(
                            "RestAPI CreateStateMachine: Invalid definition size for State Machine '{}'.".format(name)
                        )
                        return aws_error("InvalidDefinition"), 400

                    try:
                        definition = json.loads(definition)
                    except ValueError as e:
                        definition = None
                        self.logger.error(
                            "RestAPI UpdateStateMachine: State Machine definition {} does not contain valid JSON".format(
                                params.get("definition")
                            )
                        )
                        return aws_error("InvalidDefinition"), 400

                    # TODO ASL Validator??
                    state_machine["definition"] = definition

                if not role_arn and not definition:
                    self.logger.warning(
                        "RestAPI UpdateStateMachine: either roleArn or definition must be specified"
                    )
                    return aws_error("MissingRequiredParameter"), 400

                update_date = time.time()
                state_machine["updateDate"] = update_date

                self.asl_store[state_machine_arn] = state_machine

                resp = {"updateDate": update_date}

                return jsonify(resp), 200

            def aws_api_DeleteStateMachine():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_DeleteStateMachine.html
                TODO This should really mark the state machine for deletion and
                "The state machine itself is deleted after all executions are 
                completed or deleted."
                """
                state_machine_arn = params.get("stateMachineArn")
                if not state_machine_arn:
                    self.logger.warning(
                        "RestAPI DeleteStateMachine: stateMachineArn must be specified"
                    )
                    return aws_error("MissingRequiredParameter"), 400

                if not valid_state_machine_arn(state_machine_arn):
                    self.logger.warning(
                        "RestAPI DeleteStateMachine: {} is an invalid State Machine ARN".format(
                            state_machine_arn
                        )
                    )
                    return aws_error("InvalidArn"), 400

                # Look up stateMachineArn
                state_machine = self.asl_store.get_cached_view(state_machine_arn)
                if not state_machine:
                    self.logger.info(
                        "RestAPI DeleteStateMachine: State Machine {} does not exist".format(
                            state_machine_arn
                        )
                    )
                    return aws_error("StateMachineDoesNotExist"), 400

                del self.asl_store[state_machine_arn]

                return "", 200

            def aws_api_StartExecution():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_StartExecution.html
                """
                # print(params)
                state_machine_arn = params.get("stateMachineArn")
                if not state_machine_arn:
                    self.logger.warning(
                        "RestAPI StartExecution: stateMachineArn must be specified"
                    )
                    return aws_error("MissingRequiredParameter"), 400

                if not valid_state_machine_arn(state_machine_arn):
                    self.logger.warning(
                        "RestAPI StartExecution: {} is an invalid State Machine ARN".format(
                            state_machine_arn
                        )
                    )
                    return aws_error("InvalidArn"), 400

                """
                If name isn't provided create one from a UUID. TODO names should
                be unique within a 90 day period, at the moment there is no code
                to check for uniqueness of provided names so client code that
                doesn't honour this may currently succeed in this implementation
                but fail if calling real AWS StepFunctions.
                """
                name = params.get("name", str(uuid.uuid4()))
                if not valid_name(name):
                    self.logger.warning(
                        "RestAPI StartExecution: {} is an invalid name".format(name)
                    )
                    return aws_error("InvalidName"), 400

                input = params.get("input", "{}")
                """
                First check if the input length has exceeded the 262144 character
                quota described in Stepfunction Quotas page.
                https://docs.aws.amazon.com/step-functions/latest/dg/limits.html
                """
                if len(input) > MAX_DATA_LENGTH:
                    self.logger.error(
                        "RestAPI StartExecution: input size for execution '{}' exceeds "
                        "the maximum number of characters service limit.".format(name)
                    )
                    return aws_error("InvalidExecutionInput"), 400

                try:
                    input = json.loads(input)
                except TypeError as e:
                    self.logger.error("RestAPI StartExecution: Invalid input, {}".format(e))
                    return aws_error("InvalidExecutionInput"), 400
                except ValueError as e:
                    self.logger.error(
                        "RestAPI StartExecution: input {} does not contain valid JSON".format(
                            input
                        )
                    )
                    return aws_error("InvalidExecutionInput"), 400

                # Look up stateMachineArn
                state_machine = self.asl_store.get_cached_view(state_machine_arn)
                if not state_machine:
                    self.logger.info(
                        "RestAPI StartExecution: State Machine {} does not exist".format(
                            state_machine_arn
                        )
                    )
                    return aws_error("StateMachineDoesNotExist"), 400


                # Form executionArn from stateMachineArn and name
                arn = parse_arn(state_machine_arn)
                execution_arn = create_arn(
                    service="states",
                    region=arn.get("region", self.region),
                    account=arn["account"],
                    resource_type="execution",
                    resource=arn["resource"] + ":" + name,
                )

                with opentracing.tracer.start_active_span(
                    operation_name="StartExecution:ExecutionLaunching",
                    child_of=span_context("http_headers", request.headers, self.logger),
                    tags={
                        "component": "rest_api",
                        "execution_arn": execution_arn
                    }
                ) as scope:
                    """
                    The application context is described in the AWS documentation:
                    https://docs.aws.amazon.com/step-functions/latest/dg/input-output-contextobject.html
                    """
                    # https://stackoverflow.com/questions/8556398/generate-rfc-3339-timestamp-in-python
                    start_time = datetime.now(timezone.utc).astimezone().isoformat()
                    context = {
                        "Tracer": inject_span("text_map", scope.span, self.logger),
                        "Execution": {
                            "Id": execution_arn,
                            "Input": input,
                            "Name": name,
                            "RoleArn": state_machine.get("roleArn"),
                            "StartTime": start_time,
                        },
                        "State": {"EnteredTime": start_time, "Name": ""},  # Start state
                        "StateMachine": {
                            "Id": state_machine_arn,
                            "Name": state_machine.get("name"),
                        },
                    }

                    event = {"data": input, "context": context}

                    """
                    threadsafe=True is important here as the RestAPI runs in a
                    different thread to the main event_dispatcher loop.
                    """
                    try:
                        self.event_dispatcher.publish(
                            event, threadsafe=True, start_execution=True
                        )
                    except:
                        message = ("RestAPI StartExecution: Internal messaging "
                                  "error, start message could not be published.")
                        self.logger.error(message)
                        return aws_error("InternalError", message), 500

                    resp = {"executionArn": execution_arn, "startDate": time.time()}

                    return jsonify(resp), 200

            def aws_api_ListExecutions():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_ListExecutions.html
                """
                state_machine_arn = params.get("stateMachineArn")
                if not state_machine_arn:
                    self.logger.warning(
                        "RestAPI ListExecutions: stateMachineArn must be specified"
                    )
                    return aws_error("MissingRequiredParameter"), 400

                if not valid_state_machine_arn(state_machine_arn):
                    self.logger.warning(
                        "RestAPI ListExecutions: {} is an invalid State Machine ARN".format(
                            state_machine_arn
                        )
                    )
                    return aws_error("InvalidArn"), 400

                # Look up stateMachineArn
                state_machine = self.asl_store.get_cached_view(state_machine_arn)
                if not state_machine:
                    self.logger.info(
                        "RestAPI ListExecutions: State Machine {} does not exist".format(
                            state_machine_arn
                        )
                    )
                    return aws_error("StateMachineDoesNotExist"), 400

                status_filter = params.get("statusFilter")
                if status_filter and status_filter not in {
                    "RUNNING",
                    "SUCCEEDED",
                    "FAILED",
                    "TIMED_OUT",
                    "ABORTED",
                }:
                    status_filter = None

                """
                Populate response using list and dict comprehensions
                https://www.pythonforbeginners.com/basics/list-comprehensions-in-python
                https://stackoverflow.com/questions/5352546/extract-subset-of-key-value-pairs-from-python-dictionary-object

                TODO handle nextToken stuff.
                Note that ListExecutions is potentially a very expensive call as
                there might well be a large number of executions for any given
                State Machine and moreover the execution details are stored as
                Redis hashes that are themselves keyed by the execution ARN. In
                other words it is not *natively* a list and under the covers
                listing the executions is implemented by a redis.scan. One
                option for improving things might be to use the next_token to
                wrap a scan cursor. That approach should works as the maxResults
                in the API call is only a hint and the actual number of results
                returned per call might be fewer than the specified maximum, so
                that fits somewhat to the constraints of Redis scan cursors.
                """
                next_token = ""

                executions = [
                    {
                        k1: v[k1] for k1 in ("executionArn", "name", "startDate",       
                            "stateMachineArn", "status", "stopDate")
                    }
                    for k, v in self.executions.items()
                    if v["stateMachineArn"] == state_machine_arn
                    and (status_filter == None or v["status"] == status_filter)
                ]

                resp = {
                    "executions": executions
                }
                if next_token:
                    resp["nextToken"] = next_token

                return jsonify(resp), 200

            def aws_api_DescribeExecution():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeExecution.html
                """
                execution_arn = params.get("executionArn")
                if not execution_arn:
                    self.logger.warning(
                        "RestAPI DescribeExecution: executionArn must be specified"
                    )
                    return aws_error("MissingRequiredParameter"), 400

                if not valid_execution_arn(execution_arn):
                    self.logger.warning(
                        "RestAPI DescribeExecution: {} is an invalid Execution ARN".format(
                            execution_arn
                        )
                    )
                    return aws_error("InvalidArn"), 400

                # Look up executionArn
                execution = self.executions.get(execution_arn)
                if not execution:
                    self.logger.info(
                        "RestAPI DescribeExecution: Execution {} does not exist".format(
                            execution_arn
                        )
                    )
                    return aws_error("ExecutionDoesNotExist"), 400

                if not isinstance(execution , dict):  # May be (non JSON) RedisDict
                    execution = dict(execution)
                return jsonify(execution), 200

            def aws_api_GetExecutionHistory():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_GetExecutionHistory.html
                """
                # print(params)

                execution_arn = params.get("executionArn")
                if not execution_arn:
                    self.logger.warning(
                        "RestAPI GetExecutionHistory: executionArn must be specified"
                    )
                    return aws_error("MissingRequiredParameter"), 400

                if not valid_execution_arn(execution_arn):
                    self.logger.warning(
                        "RestAPI GetExecutionHistory: {} is an invalid Execution ARN".format(
                            execution_arn
                        )
                    )
                    return aws_error("InvalidArn"), 400

                reverse_order = params.get("reverseOrder", False)

                # Look up executionArn
                history = self.execution_history.get(execution_arn)
                if not history:
                    self.logger.info(
                        "RestAPI GetExecutionHistory: Execution {} does not exist".format(
                            execution_arn
                        )
                    )
                    return aws_error("ExecutionDoesNotExist"), 400

                """
                Reverse via slicing: [start:stop:step] so step is -1
                https://stackoverflow.com/questions/3940128/how-can-i-reverse-a-list-in-python

                TODO handle nextToken stuff.

                Note that GetExecutionHistory is potentially an expensive call
                if the history is large. The store self.execution_history has
                list semantics, but is backed by an external (e.g. Redis) store.
                Under the covers it will do a redis.lrange, so the next_token
                behaviour when implemented should "slice" the appropriate range.
                Note that doing this for GetExecutionHistory should be easier
                than for ListExecutions - see comment in ListExecutions for why.
                """
                if reverse_order:
                    history = history[::-1]
                else:
                    history = history[:]

                next_token = ""

                resp = {"events": history}
                if next_token:
                    resp["nextToken"] = next_token

                return jsonify(resp), 200

            def aws_api_InvalidAction():
                self.logger.error("RestAPI invalid action: {}".format(action))
                return "InvalidAction", 400

            # ------------------------------------------------------------------

            """
            Use the API action to dynamically invoke the appropriate handler.
            The "aws_api_" prefix mitigates the risk of the action value
            executing an arbitrary function, so disable semgrep warning.
            """
            try:
                # nosemgrep
                value, code = locals().get("aws_api_" + action, aws_api_InvalidAction)()
                return value, code
            except Exception as e:
                self.logger.error(
                    "RestAPI action {} failed unexpectedly with exception: {}".format(
                        action, e
                    )
                )
                return "InternalError", 500

        # Have an endpoint to check the health of the ASL engine
        @app.route("/health")
        def health_check():
            # Check if the session channel is open (alike session.channel.is_open())
            # Give response dependent on outcome of session
            if self.event_dispatcher.session.is_open():
                return "Ok", 200
            else:
                return "Service Unavailable: Service liveness probe failed due to an internal error.", 503

        return app
