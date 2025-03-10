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
# PYTHONPATH=.. LOG_LEVEL=DEBUG python3 workflow_engine.py
#
# Run with cProfile enabled:
# PYTHONPATH=.. python3 -m cProfile -s tottime workflow_engine.py > prof-tottime.txt
#
"""
This is the main application entry point to the ASL Workflow Engine.
This class reads the JSON configuration file config.json and stores the config
object for the rest of the application to use, it then creates and starts a
StateEngine and EventDispatcher and a Web Server to handle AWS CLI/SDK REST API.

NOTE Due to the addition of asyncio support Python 3.6 is now required.
"""

import sys
assert sys.version_info >= (3, 6)  # Bomb out if not running Python3.6


import asyncio, json, os
import threading  # Run REST API in its own thread
from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.open_tracing_factory import create_tracer
from asl_workflow_engine.state_engine import StateEngine
from asl_workflow_engine.event_dispatcher import EventDispatcher

import asl_workflow_engine.rest_api
import asl_workflow_engine.rest_api_asyncio

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
        self.logger = init_logging(log_name="asl_workflow_engine")

        # Load the configuration file.
        try:
            with open(configuration_file, "r") as fp:
                config = json.load(fp)
            self.logger.info("Creating WorkflowEngine")
        except IOError as e:
            self.logger.error(
                "Unable to read configuration file: {}".format(configuration_file)
            )
            raise
        except ValueError as e:
            self.logger.error("Configuration file does not contain valid JSON")
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
        eq["queue_implementation"] = os.environ.get(
            "EVENT_QUEUE_QUEUE_IMPLEMENTATION",
            eq.get("queue_implementation", "AMQP-0.9.1-asyncio")
        )
        eq["queue_type"] = os.environ.get(
            "EVENT_QUEUE_QUEUE_TYPE", eq.get("queue_type", "classic")
        )
        eq["connection_url"] = os.environ.get(
            "EVENT_QUEUE_CONNECTION_URL", eq.get("connection_url")
        )
        # Not currently used.
        eq["connection_options"] = os.environ.get(
            "EVENT_QUEUE_CONNECTION_OPTIONS", eq.get("connection_options")
        )
        eq["shared_event_consumer_capacity"] = os.environ.get(
            "EVENT_QUEUE_SHARED_EVENT_CONSUMER_CAPACITY", 
            eq.get("shared_event_consumer_capacity")
        )
        eq["instance_event_consumer_capacity"] = os.environ.get(
            "EVENT_QUEUE_INSTANCE_EVENT_CONSUMER_CAPACITY", 
            eq.get("instance_event_consumer_capacity")
        )
        eq["reply_to_consumer_capacity"] = os.environ.get(
            "EVENT_QUEUE_REPLY_TO_CONSUMER_CAPACITY",
            eq.get("reply_to_consumer_capacity")
        )
        # The time in milliseconds to retain "orphaned" Task responses.
        # Default is 10 minutes = 10*60*1000
        eq["orphaned_response_retention_ms"] = os.environ.get(
            "EVENT_QUEUE_ORPHANED_RESPONSE_RETENTION_MS",
            eq.get("orphaned_response_retention_ms", 600000)
        )

        no = config["notifier"]
        no["topic"] = os.environ.get(
            "NOTIFIER_TOPIC", no.get("topic")
        )
        no["message_ttl"] = os.environ.get(
            "NOTIFIER_MESSAGE_TTL", no.get("message_ttl", 0)
        )

        se = config["state_engine"]
        se["store_url"] = os.environ.get("STATE_ENGINE_STORE_URL",
                                         se.get("store_url"))
        se["execution_ttl"] = os.environ.get("STATE_ENGINE_EXECUTION_TTL",
                                             se.get("execution_ttl", 86400))

        ra = config["rest_api"]
        ra["host"] = os.environ.get("REST_API_HOST", ra.get("host"))
        ra["port"] = int(os.environ.get("REST_API_PORT", ra.get("port")))
        ra["region"] = os.environ.get("REST_API_REGION", ra.get("region"))
        ra["validate_asl"] = ra.get("validate_asl", False)
        validate_asl = os.environ.get("REST_API_VALIDATE_ASL")
        if validate_asl:  # Should be "true"/"false" if env var is explicitly set
            if validate_asl.lower() == "true":
                ra["validate_asl"] = True
            elif validate_asl.lower() == "false":
                ra["validate_asl"] = False

        tr = config["tracer"]
        tr["implementation"] = os.environ.get("TRACER_IMPLEMENTATION", 
                                              tr.get("implementation", "None"))

        if tr["implementation"] == "Jaeger":
            # The Jaeger specific env vars are derived from this document:
            # https://www.jaegertracing.io/docs/1.22/client-features/
            tr["config"] = tr.get("config", {})
            tr["config"]["sampler"] = tr["config"].get("sampler", {})

            sampler = tr["config"]["sampler"]
            sampler["type"] = os.environ.get(
                "JAEGER_SAMPLER_TYPE", sampler.get("type", "probabilistic")
            )
            sampler["param"] = os.environ.get(
                "JAEGER_SAMPLER_PARAM", sampler.get("param", 0.01)
            )
        elif tr["implementation"] == "OpenTelemetry":
            tr["config"] = tr.get("config", {})
            tr["config"]["exporter"] = os.environ.get(
                "OTEL_EXPORTER_TYPE", tr["config"].get("exporter", "otlp-proto-grpc")
            )
            """
            Allow configuration of *additional* propagators to add to default
            propagators. This is in addition to also honouring the
            OTEL_PROPAGATORS env var. Initially we default this to jaeger so
            uber-trace-id headers are propagated to facilitate transiion to
            OpenTelemetry. In due course we will default to no additional
            propagators.
            """
            tr["config"]["additional_propagators"] = os.environ.get(
                "OTEL_ADDITIONAL_PROPAGATORS",
                tr["config"].get("additional_propagators", "jaeger")
            )

        metrics = config["metrics"]
        metrics["implementation"] = os.environ.get(
            "METRICS_IMPLEMENTATION", metrics.get("implementation", "None")
        )
        metrics["namespace"] = os.environ.get(
            "METRICS_NAMESPACE", metrics.get("namespace", "")
        )

        """
        Initialise opentracing.tracer before creating the StateEngine,
        EventDispatcher and RestAPIinstances.

        Call asyncio.get_event_loop() here, because if we are using asyncio we
        want the tracer to use the main asyncio event loop rather than create
        a new ThreadLoop, which is the default behaviour unless a tornado IOLoop
        is passed. In recent versions of Tornado that delegates to asyncio loop.
        """
        if eq["queue_implementation"].endswith("-asyncio"):
            # Attempt to use uvloop libuv based event loop if available
            # https://github.com/MagicStack/uvloop
            try:
                import uvloop
                uvloop.install()
                self.logger.info("Using uvloop asyncio event loop")
            except:  # Fall back to standard library asyncio epoll event loop
                self.logger.info("Using standard library asyncio event loop")

            loop = asyncio.get_event_loop()
            create_tracer("asl_workflow_engine", config["tracer"], use_asyncio=True)
        else:
            create_tracer("asl_workflow_engine", config["tracer"])

        self.state_engine = StateEngine(config)
        self.event_dispatcher = EventDispatcher(self.state_engine, config)

        self.config = config

    def start(self):
        if self.event_dispatcher.name.endswith("_asyncio"):
            def global_exception_handler(loop, context):
                """
                Just swallow "exception was never retrieved" as we handle the
                main exceptions that we care about in EventDispatcher.start_asyncio()
                """
                # context["message"] will always be there; but context["exception"] may not
                self.logger.error(context.get("message"))
                exception = context.get("exception")
                if exception:
                    self.logger.error(repr(exception))


            self.rest_api = asl_workflow_engine.rest_api_asyncio.RestAPI(
                self.state_engine, self.event_dispatcher, self.config
            )
            app = self.rest_api.create_app()

            loop = asyncio.get_event_loop()
            loop.set_exception_handler(global_exception_handler)
            loop.create_task(self.event_dispatcher.start_asyncio())
            """
            Start moving towards having Quart run via an external ASGI server
            so it's easier to compare the performance of different ones.
            Doing that is a little bit fiddly as at requires some refactoring
            to make the REST API the main application entry point rather than
            the WorkflowEngine class. Another complication is ensuring the
            EventDispatcher gets passed the correct event loop when refactoring.
            For now just start via the API serve function as described here:
            https://pgjones.gitlab.io/hypercorn/how_to_guides/api_usage.html
            instead of using app.run()
            """
            
            # hypercorn
            #app.run(host=self.rest_api.host, port=self.rest_api.port, loop=loop)

            from hypercorn.asyncio import serve
            from hypercorn.config import Config
            from hypercorn.run import run

            config = Config()
            config.bind = ["{}:{}".format(self.rest_api.host, self.rest_api.port)]

            loop.run_until_complete(serve(app, config))
            
            """
            # uvicorn
            import uvicorn

            # Setting loop="none" in uvicorn.run actually means use current event loop

            #import yappi
            #yappi.set_clock_type("WALL")
            #with yappi.run():

            uvicorn.run(
                app, host=self.rest_api.host, port=self.rest_api.port,
                loop="none", log_level="error"
            )

            #yappi.get_func_stats().print_all(columns={
            #    0: ("name", 140),
            #    1: ("ncall", 8),
            #    2: ("tsub", 8),
            #    3: ("ttot", 8),
            #    4: ("tavg", 8)
            #})
            """
        else:
            self.rest_api = asl_workflow_engine.rest_api.RestAPI(
                self.state_engine, self.event_dispatcher, self.config
            )
            app = self.rest_api.create_app()
            # https://stackoverflow.com/questions/31264826/start-a-flask-application-in-separate-thread/31265602#31265602
            threading.Thread(
                target=app.run,
                kwargs={
                    "host": self.rest_api.host,
                    "port": self.rest_api.port,
                },
                daemon=True,
            ).start()
            self.event_dispatcher.start()

if __name__ == "__main__":
    WorkflowEngine("config.json").start()

