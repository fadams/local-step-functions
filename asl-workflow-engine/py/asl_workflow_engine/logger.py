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
This is a deliberately trivial logger implementation. As per 12FA it is treating
logs as an event stream, in this case stderr, see https://12factor.net/logs
A twelve-factor app never concerns itself with routing or storage of its output
stream. It should not attempt to write to or manage logfiles. Instead, each
running process writes its event stream, unbuffered, to stdout (or stderr).
"""

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import logging

def init_logging(log_name):
    """
    Create a logger to use

    :param log_name: Name of log, usually processor name or similar
    :type log_name: str
    :return: Logger to use
    """
    logger = logging.getLogger(log_name)

    # If logger already has handlers just return it as it is already initialised
    if logger.hasHandlers(): return logger

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('[%(asctime)s] %(levelname)-8s - %(name)-15s : %(message)s')
    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel(logging.DEBUG)
    stderr_handler.setFormatter(formatter)

    logger.addHandler(stderr_handler)

    return logger
