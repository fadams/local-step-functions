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
This provides implementation agnostic metrics hooks.
"""

import sys
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3

from asl_workflow_engine.logger import init_logging

impl = None
#Metrics()

def init_metrics(service_name, config):
    """
    Initialise metrics implementation

    """
    init_metrics.logger = init_logging(service_name)
    init_metrics.logger.info("Creating Metrics endpoint")
    if config is not None:
        if config.get("implementation") == "Prometheus":
            try:
                # import deferred until Prometheus is selected in config.
                from prometheus_client import start_http_server
    
                """
                If Implementation = Prometheus get the Prometheus config from the config
                dict if available, if not present create a sane default config.
                """
                prom_config = config.get("config")
                # Start prometheus https server
                start_http_server(prom_config["port"])
                
                # TODO: Override impl variable with prometheus implementation
            except Exception as e:
                init_metrics.logger.warning("Failed to initialise Prometheus Metrics : {}".format(e))

class Metrics(object):
    def item_received():
        pass
        
    def item_completed():
        pass
    
    def item_failed():
        pass
