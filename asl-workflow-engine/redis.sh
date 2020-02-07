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
# Runs up a redis server for dev purposes.
# N.B. Exposes redis port 6379 for simplicity only use on trusted networks!
# Documentation is here: https://hub.docker.com/_/redis/
# redis-server runs as user redis so there's no need to explicitly use
# -u $(id -u):$(id -g)
# or similar to force the container to run as a user with fewer privileges.
# 
# Using Append Only File persistence
#
# To use cli:
# docker run --rm -it redis redis-cli -h <redis-server-hostname>
#-------------------------------------------------------------------------------

# Create store if it doesn't already exist.
mkdir -p redis-store

docker run --rm \
    -p 6379:6379 \
    -v $PWD/redis-store:/data:rw \
    redis redis-server --appendonly yes

