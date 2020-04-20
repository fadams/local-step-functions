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
This class reads the JSON configuration file config.json and stores the config
object for the rest of the application to use, it then creates and starts a
StateEngine and EventDispatcher and a Web Server to handle AWS CLI/SDK REST API.
"""

import sys
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3


import json, os
import threading  # Run REST API in its own thread
from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.metrics import init_metrics
from asl_workflow_engine.open_tracing_factory import create_tracer
from asl_workflow_engine.state_engine import StateEngine
from asl_workflow_engine.event_dispatcher import EventDispatcher
from asl_workflow_engine.rest_api import RestAPI

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
        logger = init_logging(log_name="asl_workflow_engine")

        # Load the configuration file.
        try:
            with open(configuration_file, "r") as fp:
                config = json.load(fp)
            logger.info("Creating WorkflowEngine")
        except IOError as e:
            logger.error(
                "Unable to read configuration file: {}".format(configuration_file)
            )
            raise
        except ValueError as e:
            logger.error("Configuration file does not contain valid JSON")
            raise

        # Provide defaults for any unset config key
        config["event_queue"] = config.get("event_queue", {})
        config["notifier"] = config.get("notifier", {})
        config["state_engine"] = config.get("state_engine", {})
        config["rest_api"] = config.get("rest_api", {})
        config["tracer"] = config.get("tracer", {})
        config["metrics"] = config.get("metrics", {})

        """
        Override config values if a field is set as an environment variable.
        There is also a USE_STRUCTURED_LOGGING environment variable used by
        the logger to select between automation friendly structured logging
        or more human readable "traditional" logs.
        """
        eq = config["event_queue"]
        eq["queue_name"] = os.environ.get(
            "EVENT_QUEUE_QUEUE_NAME", eq.get("queue_name")
        )
        eq["instance_id"] = os.environ.get(
            "EVENT_QUEUE_INSTANCE_ID", eq.get("instance_id")
        )
        eq["queue_type"] = os.environ.get(
            "EVENT_QUEUE_QUEUE_TYPE", eq.get("queue_type")
        )
        eq["connection_url"] = os.environ.get(
            "EVENT_QUEUE_CONNECTION_URL", eq.get("connection_url")
        )
        eq["connection_options"] = os.environ.get(
            "EVENT_QUEUE_CONNECTION_OPTIONS", eq.get("connection_options")
        )

        no = config["notifier"]
        no["topic"] = os.environ.get(
            "NOTIFIER_TOPIC", no.get("topic")
        )

        se = config["state_engine"]
        se["store_url"] = os.environ.get("STATE_ENGINE_STORE_URL", se.get("store_url"))

        ra = config["rest_api"]
        ra["host"] = os.environ.get("REST_API_HOST", ra.get("host"))
        ra["port"] = int(os.environ.get("REST_API_PORT", ra.get("port")))
        ra["region"] = os.environ.get("REST_API_REGION", ra.get("region"))

        # Initialise opentracing.tracer before creating the StateEngine,
        # EventDispatcher and RestAPIinstances.
        create_tracer("asl_workflow_engine", config["tracer"])

        init_metrics("asl_workflow_engine", config["metrics"])

        state_engine = StateEngine(config)
        self.event_dispatcher = EventDispatcher(state_engine, config)
        self.rest_api = RestAPI(state_engine, self.event_dispatcher, config)

    def start(self):
        self.event_dispatcher.start()

if __name__ == "__main__":
    workflow_engine = WorkflowEngine("config.json")
    app = workflow_engine.rest_api.create_app()
    # https://stackoverflow.com/questions/31264826/start-a-flask-application-in-separate-thread/31265602#31265602
    threading.Thread(
        target=app.run,
        kwargs={
            "host": workflow_engine.rest_api.host,
            "port": workflow_engine.rest_api.port,
        },
    ).start()
    workflow_engine.start()

