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
# PYTHONPATH=.. python3 workers_blocking_asyncio_alternative.py
# PYTHONPATH=.. LOG_LEVEL=DEBUG python3 workers_blocking_asyncio_alternative.py
#
"""
This is an asyncio based version of the workers_blocking.py example.

The main loop uses asyncio, and a "handler_task" representing a long running
job that would block the event loop if called directly is launched as a Task
(Executor thread) by the main message_listener via a loop.run_in_executor call.

This similar to the workers_blocking_asyncio.py example but uses a slightly
different strategy. In the other example *all* of the behaviour of the
message listener gets "hoisted" into the handler_task. That is a very analogous
approach to that taken in the workers_blocking.py example which uses the
Pika BlockingConnection.

With the BlockingConnection that's the only real option because retrieving
the result of a concurrent.futures.Future is a *blocking* operation, however
with asyncio loop.run_in_executor the result is an asyncio.Future which is
*awaitable* (as opposed to blocking) so we can *await* the result of the
handler_task *without* blocking the event loop (and thus borking the heartbeats)
"""

import sys
assert sys.version_info >= (3, 6) # Bomb out if not running Python3.6

import asyncio, json, time, opentracing

import atexit, concurrent.futures, contextlib

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.open_tracing_factory import create_tracer, span_context, inject_span
from asl_workflow_engine.amqp_0_9_1_messaging_asyncio import Connection, Message
from asl_workflow_engine.messaging_exceptions import *

class Worker():

    def __init__(self, name):
        self.name = name
        # Initialise logger
        self.logger = init_logging(log_name=name)

        # Note that although we will use asyncio await loop.run_in_executor
        # to actually launch the tasks, the actual ThreadPoolExecutor itself
        # is not asyncio so we use ExitStack, enter_context and close rather
        # than AsyncExitStack, enter_async_context and aclose as we would for
        # an asyncio context manager.
        context_stack = contextlib.ExitStack()
        # Create the ThreadPoolExecutor here because creating it in the
        # message listener/handler is really expensive. Also running it as a
        # context manager using a with statement there won't actually sort the
        # issue as the context manager blocks until the threads exit so will
        # block the event loop in the main thread which is exactly what we
        # *don't* want to do.
        self.executor = context_stack.enter_context(
            concurrent.futures.ThreadPoolExecutor(max_workers=10)
        )

        # Note: exit_handler is a nested closure which "decouples" the lifetime
        # of the exit_handler from the lifetimes of instances of the class.
        # https://stackoverflow.com/questions/16333054/what-are-the-implications-of-registering-an-instance-method-with-atexit-in-pytho
        @atexit.register
        def exit_handler():
            print("exit_handler called")
            context_stack.close()
            print("exit_handler exiting")


    # Launched as an Executor Task from the message listener
    def handler_task(self, message):
        print(self.name + " handler_task started")

        # Delay for a while to simulate work then create simple reply.
        # In a real processor **** DO WORK HERE ****
        time.sleep(30)
        reply = {"reply": self.name + " reply"}

        print(self.name + " handler_task finished")

        return reply

    async def message_listener(self, message):
        print(self.name + " message_listener called")
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
            # Delay for a while to simulate work then create simple reply.
            # In a real processor **** DO WORK HERE ****

            # N.B. whether to directly call the actual processing or use an
            # Executor *should be configurable*. For IO bound processing it may
            # make matters worse to launch in an Executor unless the processor
            # is itself blocking on other IO. In general asyncio is a better way
            # to enable high IO concurrency and Executors may be better for a
            # small number of more CPU bound processors.

            # Directly call the real delegated handler
            #self.handler_task(message)
            # Indirectly call the real delegated handler as an Executor Task
            loop = asyncio.get_event_loop()
            reply = await loop.run_in_executor(self.executor, self.handler_task, message)

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

                # In this example these don't need to be thread safe as they
                # are being called from the main asyncio event loop thread.
                self.producer.send(message)
                message.acknowledge() # Acknowledges the original request

    async def run(self):
        # Connect to worker queue and process data.
        connection = Connection("amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=10")
        try:
            await connection.open()
            session = await connection.session()
            self.consumer = await session.consumer(
                self.name + '; {"node": {"auto-delete": true}}'
            )
            self.consumer.capacity = 100; # Enable consumer prefetch
            await self.consumer.set_message_listener(self.message_listener)
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

    workers = ["SimpleBlockingProcessor"]
    loop = asyncio.get_event_loop()
    # Create list of Worker tasks using list comprehension
    tasks = [loop.create_task(Worker(name=w).run()) for w in workers]
    # Run the tasks concurrently
    loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()
    
