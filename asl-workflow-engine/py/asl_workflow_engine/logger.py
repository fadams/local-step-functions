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
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3


import os, logging
from logging.handlers import RotatingFileHandler
import structlog


def inject_context(logger, method_name, event_dict):
    # inject current structlog context to stdlib logger calls from dependencies
    context_class = structlog.get_config().get("context_class")
    if context_class:
        context = context_class()
        # context object is not always subscriptable, so create list of kv pairs instead
        kv_pairs = [(k, context.get(k)) for k in context.keys()]
        event_dict.update(kv_pairs)
    return event_dict


# Use these processors for structlog and stdlib loggers
timestamper = structlog.processors.TimeStamper(fmt="iso",key="@timestamp")
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


def configure_structlog():

    structlog.configure(
        processors=[structlog.stdlib.filter_by_level]
        + shared_processors
        + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        context_class=structlog.threadlocal.wrap_dict(dict),
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_structlog_formatter():
    
    try:  # Attempt to use ujson if available https://pypi.org/project/ujson/
        import ujson as json
    except:  # Fall back to standard library json
        import json        

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(json.dumps),
        foreign_pre_chain=shared_processors,
    )
    return formatter

def init_logging(log_name, log_level=logging.INFO):
    """
    Create a logger to use

    :param log_name: Name of log, usually processor name or similar
    :type log_name: str
    :return: Logger to use
    """
    logger = logging.getLogger(log_name)

    # If logger already has handlers just return it as it is already initialised
    if logger.hasHandlers():
        return logger

    # If the LOG_LEVEL environment variable is set use it to set the log level.
    log_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARN": logging.WARN,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    configured_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    if configured_level in log_levels:
        log_level = log_levels.get(configured_level, logging.INFO)

    # Select automation friendly structured logging or more "human readable"
    # logging based on the value of the USE_STRUCTURED_LOGGING environment var.
    use_structured_logging = os.environ.get("USE_STRUCTURED_LOGGING", "false").lower() == "true"
    if use_structured_logging:
        configure_structlog()
    
    # Allows configuing the logger via an INI format configuration file
    # If a configuration file isn't supplied, we define sensible defaults in code
    # https://docs.python.org/3/library/logging.config.html#logging.config.fileConfig
    log_config_file = os.environ.get("LOG_CONFIG_FILE", "")
    if os.path.isfile(log_config_file):
        logging.config.fileConfig(log_config_file, disable_existing_loggers=False)

    else:
        if use_structured_logging:
            formatter = get_structlog_formatter()
        else:
            formatter = logging.Formatter(
                "[%(asctime)s] %(levelname)-8s - %(name)-15s : %(message)s"
            )

        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

        logger.addHandler(handler)
        logger.setLevel(log_level)

    logger.debug("DEBUG enabled")
    return logger
