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
This creates an OpenTracing tracer instance. It will default to the null
opentracing tracer and use JaegerTracing if configured to do so.
"""

import sys
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3

import opentracing  # Provides a default null opentracing.tracer instance

from asl_workflow_engine.logger import init_logging

def create_tracer(config, log_name="asl_workflow_engine"):
    logger = init_logging(log_name)
    logger.info("Creating OpenTracing Tracer")

    config = config.get("tracer")
    if config:
        # Store default tracer in case creating concrete implementation fails.
        tracer = opentracing.tracer
        if config.get("implementation") == "Jaeger":
            try:
                # import deferred until Jaeger is selected in config.
                import jaeger_client

                jaeger = jaeger_client.Config(
                    service_name=config.get("service_name", "asl_workflow_engine"),
                    config=config.get("config", {}),
                )

                """
                The init_logging(log_name="tornado") is important, though a bit
                obtuse. Without it all subsequent log messages generated will
                be duplicated. The issue is that "under the covers" Jaeger uses
                the tornado https://www.tornadoweb.org async networking library.
                Tornado's IOLoop creates a log handler if necessary when it
                is started, because if there were no handler configured
                you'd never see any of its event loop exception messages.
                The default handler is created for the root logger and ends up
                resulting in duplicate messages for other logs. By explicitly
                adding a handler for the tornado logger, as the following line
                does, the logging should be correctly handled. See:
                https://stackoverflow.com/questions/30373620/why-does-ioloop-in-tornado-seem-to-add-root-logger-handler
                """
                init_logging(log_name="tornado")
                jaeger.initialize_tracer()
                logger.info("Jaeger Tracer initialised")
            except Exception as e:
                logger.warning("Failed to initialise Jaeger Tracer : {}".format(e))
                opentracing.tracer = tracer

