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


import re, json, time, uuid, logging
from datetime import datetime, timezone
from flask import Flask, escape, request, jsonify, abort

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.arn import *

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


class RestAPI(object):
    def __init__(self, state_engine, event_dispatcher, config):
        """
        """
        self.logger = init_logging(log_name="asl_workflow_engine")
        self.logger.info("Creating RestAPI")
        config = config.get("rest_api")
        if config:
            self.host = config.get("host", "0.0.0.0")
            self.port = config.get("port", 4584)
            self.region = config.get("region", "local")

        self.asl_cache = state_engine.asl_cache
        self.executions = state_engine.executions
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
            print(action)

            try:
                params = json.loads(request.data.decode("utf8"))
            except ValueError as e:
                self.logger.error(
                    "Message body {} does not contain valid JSON".format(request.data)
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
                    return "InvalidName", 400

                role_arn = params.get("roleArn")
                if not valid_role_arn(role_arn):
                    self.logger.warning(
                        "RestAPI CreateStateMachine: {} is an invalid Role ARN".format(
                            role_arn
                        )
                    )
                    return "InvalidArn", 400

                # Form stateMachineArn from roleArn and name
                arn = parse_arn(role_arn)
                state_machine_arn = create_arn(
                    service="states",
                    region=self.region,
                    account=arn["account"],
                    resource_type="stateMachine",
                    resource=name,
                )

                # Look up stateMachineArn
                match = self.asl_cache.get(state_machine_arn)
                if match:
                    # Info seems more appropriate than error here as creation is
                    # an idempotent action.
                    self.logger.info(
                        "RestAPI CreateStateMachine: State Machine {} already exists".format(
                            state_machine_arn
                        )
                    )
                    return "StateMachineAlreadyExists", 400

                try:
                    definition = json.loads(params.get("definition", ""))
                except ValueError as e:
                    self.logger.error(
                        "RestAPI CreateStateMachine: State Machine definition {} does not contain valid JSON".format(
                            params.get("definition")
                        )
                    )

                if not (name and role_arn and definition):
                    self.logger.warning(
                        "RestAPI CreateStateMachine: name, roleArn and definition must be specified"
                    )
                    return "MissingRequiredParameter", 400

                # TODO ASL Validator??

                creation_date = time.time()
                self.asl_cache[state_machine_arn] = {
                    "name": name,
                    "definition": definition,
                    "creationDate": creation_date,
                    "roleArn": role_arn,
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

                resp = {
                    # Populate using list comprehensions
                    # https://www.pythonforbeginners.com/basics/list-comprehensions-in-python
                    "stateMachines": [
                        {
                            "creationDate": v["creationDate"],
                            "name": v["name"],
                            "stateMachineArn": k,
                        }
                        for k, v in self.asl_cache.items()
                    ]
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
                    return "MissingRequiredParameter", 400

                if not valid_state_machine_arn(state_machine_arn):
                    self.logger.warning(
                        "RestAPI DescribeStateMachine: {} is an invalid State Machine ARN".format(
                            state_machine_arn
                        )
                    )
                    return "InvalidArn", 400

                # Look up stateMachineArn
                match = self.asl_cache.get(state_machine_arn)
                if not match:
                    self.logger.info(
                        "RestAPI DescribeStateMachine: State Machine {} does not exist".format(
                            state_machine_arn
                        )
                    )
                    return "StateMachineDoesNotExist", 400

                resp = {
                    "creationDate": match["creationDate"],
                    "definition": match["definition"],
                    "name": match["name"],
                    "roleArn": match["roleArn"],
                    "stateMachineArn": state_machine_arn,
                    "status": "ACTIVE",
                }

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
                    return "MissingRequiredParameter", 400

                if not valid_execution_arn(execution_arn):
                    self.logger.warning(
                        "RestAPI DescribeStateMachineForExecution: {} is an invalid Execution ARN".format(
                            execution_arn
                        )
                    )
                    return "InvalidArn", 400

                # Look up executionArn
                match = self.executions.get(execution_arn)
                if not match:
                    self.logger.info(
                        "RestAPI DescribeStateMachineForExecution: Execution {} does not exist".format(
                            execution_arn
                        )
                    )
                    return "ExecutionDoesNotExist", 400

                state_machine_arn = match.get("stateMachineArn")

                if not valid_state_machine_arn(state_machine_arn):
                    self.logger.warning(
                        "RestAPI DescribeStateMachineForExecution: {} is an invalid State Machine ARN".format(
                            state_machine_arn
                        )
                    )
                    return "InvalidArn", 400

                # Look up stateMachineArn
                match = self.asl_cache.get(state_machine_arn)
                if not match:
                    self.logger.info(
                        "RestAPI DescribeStateMachineForExecution: State Machine {} does not exist".format(
                            state_machine_arn
                        )
                    )
                    return "StateMachineDoesNotExist", 400

                resp = {
                    "definition": match["definition"],
                    "name": match["name"],
                    "roleArn": match["roleArn"],
                    "stateMachineArn": state_machine_arn,
                    "updateDate": match["creationDate"],
                }

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
                    return "MissingRequiredParameter", 400

                if not valid_state_machine_arn(state_machine_arn):
                    self.logger.warning(
                        "RestAPI UpdateStateMachine: {} is an invalid State Machine ARN".format(
                            state_machine_arn
                        )
                    )
                    return "InvalidArn", 400

                # Look up stateMachineArn
                match = self.asl_cache.get(state_machine_arn)
                if not match:
                    self.logger.info(
                        "RestAPI UpdateStateMachine: State Machine {} does not exist".format(
                            state_machine_arn
                        )
                    )
                    return "StateMachineDoesNotExist", 400

                role_arn = params.get("roleArn")
                if role_arn:
                    if not valid_role_arn(role_arn):
                        self.logger.warning(
                            "RestAPI UpdateStateMachine: {} is an invalid Role ARN".format(
                                role_arn
                            )
                        )
                        return "InvalidArn", 400
                    match["roleArn"] = role_arn

                if params.get("definition"):
                    try:
                        definition = json.loads(params.get("definition", ""))
                    except ValueError as e:
                        self.logger.error(
                            "RestAPI UpdateStateMachine: State Machine definition {} does not contain valid JSON".format(
                                params.get("definition")
                            )
                        )
                    # TODO ASL Validator??
                    match["definition"] = definition

                if not role_arn and not definition:
                    self.logger.warning(
                        "RestAPI UpdateStateMachine: either roleArn or definition must be specified"
                    )
                    return "MissingRequiredParameter", 400

                update_date = time.time()
                match["creationDate"] = update_date

                self.asl_cache[state_machine_arn] = match

                resp = {"updateDate": update_date}

                return jsonify(resp), 200

            def aws_api_DeleteStateMachine():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_DeleteStateMachine.html
                TODO This should really mark the state machine for deletion and
                "The state machine itself is deleted after all executions are 
                completed or deleted.". Not sure how best to implement this in
                a clustered environment without being over chatty, as the start
                and end states could be run on different instances
                """
                state_machine_arn = params.get("stateMachineArn")
                if not state_machine_arn:
                    self.logger.warning(
                        "RestAPI DeleteStateMachine: stateMachineArn must be specified"
                    )
                    return "MissingRequiredParameter", 400

                if not valid_state_machine_arn(state_machine_arn):
                    self.logger.warning(
                        "RestAPI DeleteStateMachine: {} is an invalid State Machine ARN".format(
                            state_machine_arn
                        )
                    )
                    return "InvalidArn", 400

                # Look up stateMachineArn
                match = self.asl_cache.get(state_machine_arn)
                if not match:
                    self.logger.info(
                        "RestAPI DeleteStateMachine: State Machine {} does not exist".format(
                            state_machine_arn
                        )
                    )
                    return "StateMachineDoesNotExist", 400

                del self.asl_cache[state_machine_arn]

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
                    return "MissingRequiredParameter", 400

                if not valid_state_machine_arn(state_machine_arn):
                    self.logger.warning(
                        "RestAPI DescribeStateMachine: {} is an invalid State Machine ARN".format(
                            state_machine_arn
                        )
                    )
                    return "InvalidArn", 400

                name = params.get("name", str(uuid.uuid4()))
                if not valid_name(name):
                    self.logger.warning(
                        "RestAPI StartExecution: {} is an invalid name".format(name)
                    )
                    return "InvalidName", 400

                input = params.get("input", {})
                try:
                    input = json.loads(input)
                except ValueError as e:
                    self.logger.error(
                        "RestAPI StartExecution: input {} does not contain valid JSON".format(
                            input
                        )
                    )
                    return "InvalidExecutionInput", 400

                # Look up stateMachineArn
                match = self.asl_cache.get(state_machine_arn)
                if not match:
                    self.logger.info(
                        "RestAPI StartExecution: State Machine {} does not exist".format(
                            state_machine_arn
                        )
                    )
                    return "StateMachineDoesNotExist", 400


                # Form executionArn from stateMachineArn and name
                arn = parse_arn(state_machine_arn)
                execution_arn = create_arn(
                    service="states",
                    region=arn.get("region", self.region),
                    account=arn["account"],
                    resource_type="execution",
                    resource=arn["resource"] + ":" + name,
                )

                """
                The application context is described in the AWS documentation:
                https://docs.aws.amazon.com/step-functions/latest/dg/input-output-contextobject.html
                """
                # https://stackoverflow.com/questions/8556398/generate-rfc-3339-timestamp-in-python
                start_time = datetime.now(timezone.utc).astimezone().isoformat()
                context = {
                    "Execution": {
                        "Id": execution_arn,
                        "Input": input,
                        "Name": name,
                        "RoleArn": match.get("roleArn"),
                        "StartTime": start_time,
                    },
                    "State": {"EnteredTime": start_time, "Name": ""},  # Start state
                    "StateMachine": {
                        "Id": state_machine_arn,
                        "Name": match.get("name"),
                    },
                }

                event = {"data": input, "context": context}

                """
                threadsafe=True is important here as the RestAPI runs in a
                different thread to the main event_dispatcher loop.
                """
                self.event_dispatcher.publish(event, threadsafe=True)

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
                    return "MissingRequiredParameter", 400

                if not valid_state_machine_arn(state_machine_arn):
                    self.logger.warning(
                        "RestAPI ListExecutions: {} is an invalid State Machine ARN".format(
                            state_machine_arn
                        )
                    )
                    return "InvalidArn", 400

                # Look up stateMachineArn
                match = self.asl_cache.get(state_machine_arn)
                if not match:
                    self.logger.info(
                        "RestAPI ListExecutions: State Machine {} does not exist".format(
                            state_machine_arn
                        )
                    )
                    return "StateMachineDoesNotExist", 400

                status_filter = params.get("statusFilter")
                if status_filter and status_filter not in {
                    "RUNNING",
                    "SUCCEEDED",
                    "FAILED",
                    "TIMED_OUT",
                    "ABORTED",
                }:
                    self.logger.warning(
                        "RestAPI ListExecutions: {} is an invalid Status Filter Value".format(
                            status_filter
                        )
                    )
                    return "InvalidstatusFilterValue", 400

                # TODO handle nextToken stuff
                next_token = ""

                resp = {
                    # Populate using list comprehensions
                    # https://www.pythonforbeginners.com/basics/list-comprehensions-in-python
                    "executions": [
                        {
                            "executionArn": k,
                            "name": v["name"],
                            "startDate": v.get("startDate"),
                            "stateMachineArn": v["stateMachineArn"],
                            "status": v["status"],
                            "stopDate": v.get("stopDate"),
                        }
                        for k, v in self.executions.items()
                        if v["stateMachineArn"] == state_machine_arn
                        and (status_filter == None or v["status"] == status_filter)
                    ]
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
                    return "MissingRequiredParameter", 400

                if not valid_execution_arn(execution_arn):
                    self.logger.warning(
                        "RestAPI DescribeExecution: {} is an invalid Execution ARN".format(
                            execution_arn
                        )
                    )
                    return "InvalidArn", 400

                # Look up executionArn
                match = self.executions.get(execution_arn)
                if not match:
                    self.logger.info(
                        "RestAPI DescribeExecution: Execution {} does not exist".format(
                            execution_arn
                        )
                    )
                    return "ExecutionDoesNotExist", 400

                resp = {
                    "executionArn": execution_arn,
                    "input": match["input"],
                    "name": match["name"],
                    "output": match["output"],
                    "startDate": match["startDate"],
                    "stateMachineArn": match["stateMachineArn"],
                    "status": match["status"],
                    "stopDate": match["stopDate"],
                }

                return jsonify(resp), 200

            def aws_api_GetExecutionHistory():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_GetExecutionHistory.html
                """
                print(params)

                execution_arn = params.get("executionArn")
                if not execution_arn:
                    self.logger.warning(
                        "RestAPI GetExecutionHistory: executionArn must be specified"
                    )
                    return "MissingRequiredParameter", 400

                if not valid_execution_arn(execution_arn):
                    self.logger.warning(
                        "RestAPI GetExecutionHistory: {} is an invalid Execution ARN".format(
                            execution_arn
                        )
                    )
                    return "InvalidArn", 400

                # Look up executionArn
                match = self.executions.get(execution_arn)
                if not match:
                    self.logger.info(
                        "RestAPI GetExecutionHistory: Execution {} does not exist".format(
                            execution_arn
                        )
                    )
                    return "ExecutionDoesNotExist", 400

                # TODO handle nextToken stuff
                next_token = ""

                resp = {"events": match["history"]}
                if next_token:
                    resp["nextToken"] = next_token

                return jsonify(resp), 200

            def aws_api_InvalidAction():
                self.logger.error("RestAPI invalid action: {}".format(action))
                return "InvalidAction", 400

            # ------------------------------------------------------------------

            # Use the API action to dynamically invoke the appropriate handler.
            try:
                value, code = locals().get("aws_api_" + action, aws_api_InvalidAction)()
                return value, code
            except Exception as e:
                self.logger.error(
                    "RestAPI action {} failed unexpectedly with exception: {}".format(
                        action, e
                    )
                )
                return "InternalError", 500

        return app

