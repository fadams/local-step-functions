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
# PYTHONPATH=.. python3 workflow_engine.py
#
"""
This is the main application entry point to the ASL Workflow Engine.
This class reads the JSON configuration file config.json and stores the
config object for the rest of the application to use, it then creates and starts
an EventDispatcher.

TODO start a Web Server to handle AWS CLI/SDK REST API invocations for Step
Functions and any other infrastructure services needed to manage ASL state
machine instances based on Step Functions API.
"""

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import json
from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.state_engine import StateEngine
from asl_workflow_engine.event_dispatcher import EventDispatcher

class WorkflowEngine(object):

    def __init__(self, configuration_file):
        """
        :param configuration_file: Path to coordinator configuration file
        :type configuration_file: str
        :raises IOError: If configuration file does not exist, or is not readable
        :raises ValueError: If configuration file does not contain valid JSON
        :raises AssertionError: If configuration file does not contain the required fields
        """
        # Initialise logger
        logger = init_logging(log_name='asl_workflow_engine')

        # Load the configuration file. TODO it probably makes sense to also add
        # a mechanism to override config values with values obtained from the
        # command line or environment variables, the latter being especially
        # useful if we want to deploy this application to Kubernetes.
        try:
            with open(configuration_file, 'r') as fp:
                config = json.load(fp)
            logger.info("Creating WorkflowEngine")
        except IOError as e:
            logger.error("Unable to read configuration file: {}".format(configuration_file))
            raise
        except ValueError as e:
            logger.error("Configuration file does not contain valid JSON")
            raise

        state_engine = StateEngine(logger, config)
        event_dispatcher = EventDispatcher(logger, state_engine, config)

if __name__ == "__main__":
    workflow_engine = WorkflowEngine('config.json')

