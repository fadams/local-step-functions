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
# PYTHONPATH=.. python3 workers_asyncio_coroutines.py
# PYTHONPATH=.. LOG_LEVEL=DEBUG python3 workers_asyncio_coroutines.py
#
"""
This is an asyncio based version of the blocking workers.py example.

This version is entirely single threaded, using a single event loop and spawning
multiple coroutines as concurrent tasks. It is, however, structurally very similar
to the original workers.py that uses a BlockingConnection and multiple threads.
"""

import sys
assert sys.version_info >= (3, 6) # Bomb out if not running Python3.6

import asyncio, json, time, opentracing

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.open_tracing_factory import create_tracer, span_context, inject_span
from asl_workflow_engine.amqp_0_9_1_messaging_asyncio import Connection, Message
from asl_workflow_engine.messaging_exceptions import *

class Worker():

    def __init__(self, name):
        self.name = name
        # Initialise logger
        self.logger = init_logging(log_name=name)

    def handler(self, message):
        # Used to simulate a processor that doesn't respond and causes Timeout.
        if self.name == "NonExistentLambda":
            message.acknowledge() # Acknowledges the original request
            return

        print(self.name + " working")
        print(message)

        with opentracing.tracer.start_active_span(
            operation_name=self.name,
            child_of=span_context("text_map", message.properties, self.logger),
            tags={
                "component": "workers",
                "message_bus.destination": self.name,
                "span.kind": "consumer",
                "peer.address": "amqp://localhost:5672"
            }
        ) as scope:
            # Create simple reply. In a real processor **** DO WORK HERE ****
            reply = {"reply": self.name + " reply"}

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
                operation_name=self.name,
                child_of=opentracing.tracer.active_span,
                tags={
                    "component": "workers",
                    "message_bus.destination": message.reply_to,
                    "span.kind": "producer",
                    "peer.address": "amqp://localhost:5672"
                }
            ) as scope:
                message.properties=inject_span("text_map", scope.span, self.logger)
                message.subject = message.reply_to
                message.reply_to = None
                message.body = json.dumps(reply)
                self.producer.send(message)
                message.acknowledge() # Acknowledges the original request

    async def run(self):
        # Connect to worker queue and process data.
        connection = Connection("amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=0")
        try:
            await connection.open()
            session = await connection.session()
            self.consumer = await session.consumer(
                self.name + '; {"node": {"auto-delete": true}}'
            )
            self.consumer.capacity = 100; # Enable consumer prefetch
            await self.consumer.set_message_listener(self.handler)
            self.producer = await session.producer()

            await connection.start(); # Wait until connection closes.
    
        except MessagingError as e:  # ConnectionError, SessionError etc.
            self.logger.error(e)

        connection.close();

if __name__ == '__main__':
    """
    Initialising OpenTracing here rather than in the Worker constructor as
    opentracing.tracer is a per process object not per thread.
    """
    create_tracer("workers", {"implementation": "Jaeger"})

    workers = ["SuccessLambda",
               "TimeoutLambda",
               "InternalErrorHandledLambda",
               "InternalErrorNotHandledLambda",
               "NonExistentLambda",
               "mime-id",]
    #workers = ["SuccessLambda"]
    loop = asyncio.get_event_loop()
    # Create list of Worker tasks using list comprehension
    tasks = [loop.create_task(Worker(name=w).run()) for w in workers]
    # Run the tasks concurrently
    loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()
    
