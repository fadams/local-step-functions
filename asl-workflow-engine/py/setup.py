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

from setuptools import setup, find_packages

setup(
    name="asl_workflow_engine",
    version="1.11.1",
    description="A workflow engine based on the Amazon States Language (ASL).",
    long_description="A workflow engine based on the Amazon States Language (ASL). It is intended to provide the features and API of AWS Step Functions in an engine that can be deployed to a range of different hosting environments.",
    packages=find_packages(),
    install_requires=["pika",
                      "structlog",
                      "ujson",
                      "jsonpath",
                      "flask",
                      "quart",
                      "redis",
                      "pottery",
                      "opentracing>=2.2",
                      "aioprometheus",
                      "jaeger_client"]
)
