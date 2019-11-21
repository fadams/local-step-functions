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
This creates an OpenTracing tracer. It will default to the null opentracing
tracer and use JaegerTracing if configured to do so.
"""

import sys
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3

import opentracing  # Provides a default null opentracing.tracer instance

from asl_workflow_engine.logger import init_logging

def create_tracer(config):
    logger = init_logging(log_name="asl_workflow_engine")
    logger.info("Creating OpenTracing Tracer")

    config = config.get("tracer")
    #print(config)

    if config:
        # Store default tracer in case creating implementation fails.
        tracer = opentracing.tracer
        if config.get("implementation") == "Jaeger":
            try:
                import jaeger_client

                jaeger = jaeger_client.Config(
                    service_name=config.get("service_name", "asl_workflow_engine"),
                    config=config.get("config", {}),
                )

                """
                jaeger = jaeger_client.Config(
                    config={
                        'sampler': {
                            'type': 'const',
                            'param': 1,
                        },
                    'logging': False,
                    },
                    service_name="service",
                )
                """

                # Initialising Jaeger tracer seems to break logging????
                # When this line is enabled from then on I get everything
                # logged twice, not sure what is happening but it's not right!
                #jaeger.initialize_tracer()
                logger.info("Jaeger Tracer initialised")
            except Exception as e:
                logger.warning("Failed to initialise Jaeger Tracer : {}".format(e))
                opentracing.tracer = tracer

    print(opentracing.tracer)

