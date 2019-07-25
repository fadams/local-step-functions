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
# PYTHONPATH=.. python3 rest_api.py
#
"""
This implements the REST API for the ASL Workflow Engine. The intention is to
implement the AWS Step Functions API as described in the AWS documentation:
https://docs.aws.amazon.com/step-functions/latest/apireference/API_Operations.html

By implementing the AWS REST API semantics it becomes possible to use Amazon's
CLI and SDK so applications can use this ASL Workflow Engine as an alternative
to Amazon's for scenarios such as hybrid cloud workloads.


# List state machines
aws stepfunctions --endpoint http://localhost:4584 list-state-machines --max-results 20

# Create a new state machine
aws stepfunctions --endpoint http://localhost:4584 create-state-machine --name my-state-machine --definition '{"Comment":"A Hello World example of the Amazon States Language using a Pass state","StartAt":"HelloWorld","States":{"HelloWorld":{"Type":"Pass","End":true}}}' --role-arn arn:aws:iam::0123456789:role/service-role/MyRole

# Update a state machine
aws stepfunctions --endpoint http://localhost:4584 update-state-machine --definition '{"Comment":"A Hello World example of the Amazon States Language using a Pass state","StartAt":"HelloWorld","States":{"HelloWorld":{"Type":"Pass","End":true}}}' --role-arn arn:aws:iam::0123456789:role/service-role/MyRole --state-machine-arn arn:aws:states:local:0123456789:stateMachine:my-state-machine

# Describe state machine
aws stepfunctions --endpoint http://localhost:4584 describe-state-machine --state-machine-arn arn:aws:states:local:0123456789:stateMachine:my-state-machine

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
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import re
import json
import time
import logging
from flask import Flask, escape, request, jsonify, abort

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.arn import *

class RestAPI(object):
    def __init__(self, state_engine, config):
        """
        """
        self.logger = init_logging(log_name='asl_workflow_engine')
        self.logger.info("Creating RestAPI")
        config = config.get("rest_api")
        if config:
            self.host = config.get("host", "0.0.0.0")
            self.port = config.get("port", 4584)
            self.region = config.get("region", "local")

        self.asl_cache = state_engine.asl_cache

    def create_app(self):
        app = Flask(__name__)

        # Turn off Flask standard logging
        app.logger.disabled = True
        log = logging.getLogger('werkzeug')
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

            action = target.split('.')[1];
            print(action)

            try:
                params = json.loads(request.data.decode("utf8"))
            except ValueError as e:
                self.logger.error("Message body {} does not contain valid JSON".format(request.data))

            #-------------------------------------------------------------------
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
                if not isinstance(name, str) or \
                       len(name) < 1 or len(name) > 256 or \
                       re.search(r"^.*[ <>{}[\]?*\"#%\\^|~`$&,;:/].*$", name):
                    self.logger.error("RestAPI CreateStateMachine: {} is an invalid name".format(name))
                    return "InvalidName", 400

                roleArn = params.get("roleArn")
                if not isinstance(roleArn, str) or \
                       len(roleArn) < 1 or len(roleArn) > 256 or \
                       not re.search(r"^arn:aws:iam::[0-9]+:role\/.+$", roleArn):
                    self.logger.error("RestAPI CreateStateMachine: {} is an invalid Role ARN".format(roleArn))
                    return "InvalidArn", 400

                # Form stateMachineArn from roleArn and name
                arn = parse_arn(roleArn)
                stateMachineArn = create_arn(service="states",
                                             region=self.region,
                                             account=arn["account"], 
                                             resource_type="stateMachine",
                                             resource=name)

                # Look up stateMachineArn
                match = self.asl_cache.get(stateMachineArn)
                if match:
                    self.logger.error("RestAPI CreateStateMachine: State Machine {} already exists".format(stateMachineArn))
                    return "StateMachineAlreadyExists", 400

                try:
                    definition = json.loads(params.get("definition", ""))
                except ValueError as e:
                    self.logger.error("RestAPI CreateStateMachine: State Machine definition {} does not contain valid JSON".format(params.get("definition")))

                # TODO ASL Validator??

                creationDate = time.time()
                self.asl_cache[stateMachineArn] = {
                    "name": name,
                    "definition": definition,
                    "creationDate": creationDate,
                    "roleArn": roleArn
                }

                resp = {
                    "creationDate": creationDate,
                    "stateMachineArn": stateMachineArn
                }

                return jsonify(resp), 200

            def aws_api_ListStateMachines():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_ListStateMachines.html
                """
                # TODO handle nextToken stuff
                nextToken = ""

                resp = {
                    "stateMachines": [
                        {
                            "creationDate": v["creationDate"],
                            "name": v["name"],
                            "stateMachineArn": k
                        } for k, v in self.asl_cache.items()
                    ]
                }
                if nextToken: resp["nextToken"] = nextToken

                return jsonify(resp), 200

            def aws_api_DescribeStateMachine():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeStateMachine.html
                """
                stateMachineArn = params.get("stateMachineArn")
                if not isinstance(stateMachineArn, str) or \
                       len(stateMachineArn) < 1 or \
                       len(stateMachineArn) > 256 or \
                       not re.search(r"^arn:aws:states:.+:[0-9]+:stateMachine:.+$",
                                     stateMachineArn):
                    self.logger.error("RestAPI DescribeStateMachine: {} is an invalid State Machine ARN".format(stateMachineArn))
                    return "InvalidArn", 400

                # Look up stateMachineArn
                match = self.asl_cache.get(stateMachineArn)
                if not match:
                    self.logger.error("RestAPI DescribeStateMachine: State Machine {} does not exist".format(stateMachineArn))
                    return "StateMachineDoesNotExist", 400

                resp = {
                    "creationDate": match["creationDate"],
                    "definition": match["definition"],
                    "name": match["name"],
                    "roleArn": match["roleArn"],
                    "stateMachineArn": stateMachineArn,
                    "status": "ACTIVE"
                }

                return jsonify(resp), 200

            def aws_api_DescribeStateMachineForExecution():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeStateMachineForExecution.html
                """
                print(params)

                resp = {
                    "definition": "string",
                    "name": "string",
                    "roleArn": "string",
                    "stateMachineArn": "string",
                    "updateDate": 10
                }

                return jsonify(resp), 200

            def aws_api_UpdateStateMachine():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_UpdateStateMachine.html
                """
                if not params.get("roleArn") and not params.get("definition"):
                    self.logger.error("RestAPI UpdateStateMachine: either roleArn or definition must be specified")
                    return "MissingRequiredParameter", 400

                stateMachineArn = params.get("stateMachineArn")
                if not isinstance(stateMachineArn, str) or \
                       len(stateMachineArn) < 1 or \
                       len(stateMachineArn) > 256 or \
                       not re.search(r"^arn:aws:states:.+:[0-9]+:stateMachine:.+$",
                                     stateMachineArn):
                    self.logger.error("RestAPI UpdateStateMachine: {} is an invalid State Machine ARN".format(stateMachineArn))
                    return "InvalidArn", 400

                # Look up stateMachineArn
                match = self.asl_cache.get(stateMachineArn)
                if not match:
                    self.logger.error("RestAPI UpdateStateMachine: State Machine {} does not exist".format(stateMachineArn))
                    return "StateMachineDoesNotExist", 400

                roleArn = params.get("roleArn")
                if roleArn:
                    if not isinstance(roleArn, str) or \
                           len(roleArn) < 1 or len(roleArn) > 256 or \
                           not re.search(r"^arn:aws:iam::[0-9]+:role\/.+$", roleArn):
                        self.logger.error("RestAPI UpdateStateMachine: {} is an invalid Role ARN".format(roleArn))
                        return "InvalidArn", 400
                    match["roleArn"] = roleArn

                if params.get("definition"):
                    try:
                        definition = json.loads(params.get("definition", ""))
                    except ValueError as e:
                        self.logger.error("RestAPI UpdateStateMachine: State Machine definition {} does not contain valid JSON".format(params.get("definition")))
                    # TODO ASL Validator??
                    match["definition"] = definition

                updateDate = time.time()
                match["creationDate"] = updateDate

                self.asl_cache[stateMachineArn] = match

                resp = {
                    "updateDate": updateDate
                }

                return jsonify(resp), 200

            def aws_api_DeleteStateMachine():
                """
                https://docs.aws.amazon.com/step-functions/latest/apireference/API_DeleteStateMachine.html
                """
                print(params)

                return "", 200

            def aws_api_InvalidAction():
                self.logger.error("RestAPI invalid action: {}".format(action))
                return "InvalidAction", 400

            #-------------------------------------------------------------------
            try:
                value, code = locals().get("aws_api_" + action,
                                       aws_api_InvalidAction)()
                return value, code
            except Exception as e:
                self.logger.error("RestAPI action {} failed unexpectedly with exception: {}".format(action, e))
                return "InternalError", 500

        return app
