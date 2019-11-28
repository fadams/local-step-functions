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
# Run with:
# PYTHONPATH=.. python3 workers.py
#

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import json, time, threading, opentracing

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.open_tracing_factory import create_tracer
from asl_workflow_engine.amqp_0_9_1_messaging import Connection, Message
from asl_workflow_engine.messaging_exceptions import *

class Worker(threading.Thread):

    def __init__(self, name):
        super().__init__(name=name) # Call Thread constructor
        # Initialise logger
        self.logger = init_logging(log_name=name)

    def handler(self, message):
        print(self.getName() + " working")
        print(message)

        """
        Extract the parent OpenTracing SpanContext from the Message application
        properties. Log message if we can't extract SpanContext.
        https://opentracing.io/docs/overview/inject-extract/
        """
        try:
            span_context = opentracing.tracer.extract(
                opentracing.Format.TEXT_MAP, message.properties
            )

        except Exception:
            self.logger.error("Missing trace-id property, unable to deserialise SpanContext. "
                              "Will have to create new trace")
            span_context = opentracing.tracer.start_span(operation_name="dangling_process")

        with opentracing.tracer.start_active_span(
            operation_name=self.getName(),
            child_of=span_context,
            tags={
                "component": "workers",
                "message_bus.destination": self.getName(),
                "span.kind": "consumer",
                "peer.address": "amqp://localhost:5672"
            }
        ) as scope:
            # Create simple reply. In a real processor **** DO WORK HERE ****
            reply = {"reply": self.getName() + " reply"}

            """
            Create the response message by reusing the request note that this
            approach retains the correlation_id, which is necessary. If a fresh
            Message instance is created we would need to get the correlation_id
            from the request Message and use that value in the response message.
            """

            """
            Start an OpenTracing trace for the rpcmessage response.
            https://opentracing.io/guides/python/tracers/ standard tags are from
            https://opentracing.io/specification/conventions/
            """
            with opentracing.tracer.start_active_span(
                operation_name=self.getName(),
                child_of=opentracing.tracer.active_span,
                tags={
                    "component": "workers",
                    "message_bus.destination": message.reply_to,
                    "span.kind": "producer",
                    "peer.address": "amqp://localhost:5672"
                }
            ) as scope:
                """
                Inject the OpenTracing SpanContext into a TEXT_MAP carrier
                in order to transport it via the Message application properties
                https://opentracing.io/docs/overview/inject-extract/
                """
                carrier = {}
                opentracing.tracer.inject(scope.span, opentracing.Format.TEXT_MAP, carrier)

                self.logger.info("Tracing active span: carrier {}".format(carrier))

                message.properties=carrier
                message.subject = message.reply_to
                message.reply_to = None
                message.body = json.dumps(reply)
                self.producer.send(message)
                message.acknowledge() # Acknowledges the original request

    def run(self):
        # Connect to worker queue and process data.
        connection = Connection("amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=0")
        try:
            connection.open()
            session = connection.session()
            self.consumer = session.consumer(self.getName() + '; {"node": {"auto-delete": true}}')
            self.consumer.capacity = 100; # Enable consumer prefetch
            self.consumer.set_message_listener(self.handler)
            self.producer = session.producer()

            #self.set_timeout = connection.set_timeout
            connection.start(); # Blocks until event loop exits.

        except MessagingError as e:
            self.logger.error(e)

        connection.close();

if __name__ == '__main__':
    """
    Initialising OpenTracing here rather than in the Worker constructor as
    opentracing.tracer is a per process object not per thread.
    """
    config = {
        "tracer": {
            "implementation": "Jaeger",
            "service_name": "workers",
            "config": {
                "sampler": {
                    "type": "const",
                    "param": 1
                },
                "logging": False
            }
        }
    }
    # Initialise opentracing.tracer
    create_tracer(config, log_name="workers")

    workers = ["SuccessLambda",
               "TimeoutLambda",
               "InternalErrorHandledLambda",
               "InternalErrorNotHandledLambda",
               "mime-id",]
    #workers = ["SuccessLambda"]
    for w in workers:
        worker = Worker(name = w)
        worker.start()

