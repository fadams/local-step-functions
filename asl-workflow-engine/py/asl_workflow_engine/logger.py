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

import os, logging
import structlog


def inject_context(logger, method_name, event_dict):
    # inject current structlog context to stdlib logger calls from dependencies
    context_class = structlog.get_config().get('context_class')
    if context_class:
        context = context_class()
        # context object is not always subscriptable, so create list of kv pairs instead
        kv_pairs = [(k, context.get(k)) for k in context.keys()]
        event_dict.update(kv_pairs)
    return event_dict

def init_logging(log_name, log_level=logging.INFO):
    """
    Create a logger to use

    :param log_name: Name of log, usually processor name or similar
    :type log_name: str
    :return: Logger to use
    """
    logger = logging.getLogger(log_name)

    # If logger already has handlers just return it as it is already initialised
    if logger.hasHandlers(): return logger

    # Select automation friendly structured logging or more "human readable"
    # logging based on the value of the USE_STRUCTURED_LOGGING environment var.
    if os.environ.get("USE_STRUCTURED_LOGGING"):
        # Use these processors for structlog and stdlib loggers
        timestamper = structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S")
        shared_processors = [
            inject_context,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            timestamper,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
        ]

        structlog.configure(
            processors=[structlog.stdlib.filter_by_level] +
                       shared_processors + [
                           structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
                       ],
            context_class=structlog.threadlocal.wrap_dict(dict),
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(),
            foreign_pre_chain=shared_processors,
        )
    else:
        formatter = logging.Formatter('[%(asctime)s] %(levelname)-8s - %(name)-15s : %(message)s')


    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(log_level)

    return logger
