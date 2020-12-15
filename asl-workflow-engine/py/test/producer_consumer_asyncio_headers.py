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
# PYTHONPATH=.. python3 producer_consumer_asyncio_headers.py
# PYTHONPATH=.. LOG_LEVEL=DEBUG python3 producer_consumer_asyncio_headers.py
#
"""
This example application is mainly for exercising the messaging API and isn't
directly related to the ASL engine per se. It is an asyncio example that creates
a bounded queue and runs a simple producer/consumer as coroutines.
"""

import sys
assert sys.version_info >= (3, 6) # Bomb out if not running Python3.6

import asyncio, json, time

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.amqp_0_9_1_messaging_asyncio import Connection, Message
from asl_workflow_engine.messaging_exceptions import *

class Worker():

    def __init__(self, name):
        self.name = name
        # Initialise logger
        self.logger = init_logging(log_name=name)
        self.count = 0

    def handler(self, message):
        #print(message)
        #print(message.body)
        self.count += 1
        print(self.count)

        # Safer but slower alternative to connection.session(auto_ack=True)
        #if self.count % 10 == 0:  # Periodically acknowledge consumed messages
        #    message.acknowledge()

    async def run(self):
        # Connect to worker queue and process data.
        connection = Connection("amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=0")
        try:
            await connection.open()
            #session = await connection.session()
            session = await connection.session(auto_ack=True)   

            """
            This illustrates a fairly "fancy" Address String to demonstrate
            AMQP Headers Exchange message routing. The extended syntax is
            creating an AMQP "node" that represents a named "subscription queue"
            and the message routing is defined by the exchange-to-queue bindings:
            "x-bindings": [{"exchange": "amq.match", "queue": "HeadersSubscriptionQueue", "key": "headers", "arguments": {"x-match": "all", "service": "news", "country": "uk"}}]
            These bindings match messages 'if service == news and country == uk'.
            The example sets these as message properties in the published message.
            """
            self.consumer = await session.consumer(
                "amq.match" + '; {"node": {"x-declare": {"auto-delete": true, "queue": "' + self.name + '", "arguments": {"x-max-length": 10, "x-overflow": "reject-publish"}}, "x-bindings": [{"exchange": "amq.match", "queue": "' + self.name + '", "key": "headers", "arguments": {"x-match": "all", "service": "news", "country": "uk"}}]}}'
            )

            self.consumer.capacity = 100; # Enable consumer prefetch
            await self.consumer.set_message_listener(self.handler)

            self.producer = await session.producer("amq.match")

            self.producer.enable_exceptions(sync=True)
            #self.producer.enable_exceptions()

            waiters = []
            for i in range(1000000):
                #print("message {}".format(self.producer.next_publish_seq_no))
                message = Message(body="hello message {}".format(
                    self.producer.next_publish_seq_no)
                )
                #message.subject = "news.uk"
                message.properties["service"] = "news"
                message.properties["country"] = "uk"
                #print(message)

                try:
                    # When producer.enable_exceptions(True) is set the send()
                    # method is made awaitable by returning a Future, resolved
                    # when the broker acks the message or exceptioned if the
                    # broker nacks. For fully "synchronous" publishing one can
                    # simply do: await self.producer.send(message)
                    # but that has a serious throughput/latency impact waiting
                    # for the publisher-confirms from the broker. The impact is
                    # especially bad for the case of durable queues.
                    # https://www.rabbitmq.com/confirms.html#publisher-confirms
                    # https://www.rabbitmq.com/confirms.html#publisher-confirms-latency
                    # To mitigate this the example below publishes in batches
                    # (optimally the batch size should be roughly the same as
                    # the session capacity). We store the awaitables in a list
                    # then periodically do an asyncio.gather of the waiters
                    waiters.append(self.producer.send(message))
                    if i % 100 == 0:
                        await asyncio.gather(*waiters)
                        waiters = []

                    # When producer.enable_exceptions(False) is set the send()
                    # method will raise an exception that contains the sequence
                    # numbers of messages that have failed to publish. If no
                    # call to enable_exceptions() is made send() just does
                    # basic_publish() with no confirmation, which could result
                    # in message loss. The asyncio.sleep() in the example is
                    # necessary to periodically yield to allow consumer delivery.
                    # If an exception is thrown either increase the sleep or
                    # catch the exception and resend the nacked messages that
                    # have been recorded in exception.undelivered
                    """
                    self.producer.send(message)
                    if i % 100 == 0:
                        await asyncio.sleep(.006)
                    """
                except SendError as e:
                    #print(waiters)
                    raise e

            await connection.start(); # Wait until connection closes.
    
        except MessagingError as e:  # ConnectionError, SessionError etc.
            self.logger.error(e)

        connection.close();

def global_exception_handler(loop, context):
    # context["message"] will always be there; but context["exception"] may not
    #msg = context.get("exception", context["message"])
    #print("********** global_exception_handler **********")
    #print(msg)
    pass  # Just swallow "exception was never retrieved"

if __name__ == '__main__':
    workers = ["HeadersSubscriptionQueue"]
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(global_exception_handler)
    # Create list of Worker tasks using list comprehension
    tasks = [loop.create_task(Worker(name=w).run()) for w in workers]
    # Run the tasks concurrently
    loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()
    
