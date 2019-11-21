#!/bin/bash
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

#-------------------------------------------------------------------------------
# Runs the Jaeger all-in-one image. Launches the Jaeger UI, collector, query,
# and agent, with an in memory storage component.
# See https://www.jaegertracing.io/docs/1.14/getting-started/#all-in-one
#
# Navigate to http://localhost:16686 to access the Jaeger UI.
#
# Port 	Protocol Component 	Function
# 5775 	UDP 	 agent 	    accept zipkin.thrift over compact thrift protocol
#                           (deprecated, used by legacy clients only)
# 6831 	UDP 	 agent 	    accept jaeger.thrift over compact thrift protocol
# 6832 	UDP 	 agent 	    accept jaeger.thrift over binary thrift protocol
# 5778 	HTTP 	 agent 	    serve configs
# 16686 HTTP 	 query 	    serve frontend
# 14268 HTTP 	 collector 	accept jaeger.thrift directly from clients
# 14250 HTTP 	 collector 	accept model.proto
# 9411 	HTTP 	 collector 	Zipkin compatible endpoint (optional)
#-------------------------------------------------------------------------------

docker run --rm -it --name jaeger \
  -e COLLECTOR_ZIPKIN_HTTP_PORT=9411 \
  -p 5775:5775/udp \
  -p 6831:6831/udp \
  -p 6832:6832/udp \
  -p 5778:5778 \
  -p 16686:16686 \
  -p 14268:14268 \
  -p 9411:9411 \
  jaegertracing/all-in-one:1.14

