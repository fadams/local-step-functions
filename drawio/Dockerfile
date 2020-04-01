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

FROM tomcat:9.0-slim

ENV DRAW_VERSION v12.9.5
ENV STEP_FUNCTIONS_PLUGIN_VERSION 0.6.2

RUN apt-get update && DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends \
    curl && \
    # Change permission so Tomcat can start as non-root user
    chmod 777 /usr/local/tomcat/conf && \
    # Install draw.io
    cd /usr/local/tomcat/webapps/ROOT && rm -rf * && \
    curl -O -sSL https://github.com/jgraph/drawio/releases/download/${DRAW_VERSION}/draw.war && \
    jar xvf draw.war && rm draw.war && \
    # Install the step-functions-draw.io plugin
    cd plugins && \
    curl -O -sSL https://cdn.jsdelivr.net/gh/sakazuki/step-functions-draw.io@${STEP_FUNCTIONS_PLUGIN_VERSION}/dist/aws-step-functions.js && \
    # Hack/patch the draw.io App.pluginRegistry in
    # /js/app.min.js to add the aws-step-functions plugin to
    # the registry, so that it can be run with &p=sfn as per 
    # https://desk.draw.io/support/solutions/articles/16000056430-what-plugins-are-available-
    # The patch method is described in this article.
    # https://groups.google.com/forum/#!topic/drawio/ZFwk6hWGs74
    sed -i 's/"\/plugins\/tags.js"/"\/plugins\/tags.js",sfn:"\/plugins\/aws-step-functions.js"/g' /usr/local/tomcat/webapps/ROOT/js/app.min.js && \
    sed -i 's/var mxIsElectron/var plugins = "\?p=sfn";\n\t\tif (window.location.search !== plugins) {window.location.replace("index.html"+plugins);}\n\t\tvar mxIsElectron/g' /usr/local/tomcat/webapps/ROOT/index.html && \
    # Tidy up
    apt-get clean && \
    apt-get purge -y curl && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

USER 1001
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

