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
# The draw.io repository https://github.com/jgraph/drawio has a Dockerfile in
# the /etc/docker, however that builds from source which seems overkill for
# our needs, especially when there are packaged releases of the war file at
# https://github.com/jgraph/drawio/releases.
#
# This Dockerfile takes a slightly simpler approach, simply ADDing the draw.war
# as ROOT.war into a tomcat container.

FROM tomcat:9.0-slim

ENV DRAW_VERSION v12.9.5
ADD https://github.com/jgraph/drawio/releases/download/${DRAW_VERSION}/draw.war /usr/local/tomcat/webapps/ROOT.war

RUN rm -rf /usr/local/tomcat/webapps/ROOT && rm -rf /var/lib/apt/lists/*

CMD ["catalina.sh", "run"]

#-------------------------------------------------------------------------------
# 
# To build the image
# docker build -t draw .
#
# To run
# docker run --rm -p 8888:8080 draw
#
# Browse to:
# http://localhost:8888
#
# step functions plugin at:
# https://cdn.jsdelivr.net/gh/sakazuki/step-functions-draw.io@0.6.2/dist/aws-step-functions.js
