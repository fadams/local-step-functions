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
# PYTHONPATH=.. python3 producer_consumer.py
# PYTHONPATH=.. LOG_LEVEL=DEBUG python3 producer_consumer.py
#
"""
This example application is mainly for exercising the messaging API and isn't
directly related to the ASL engine per se. It is a BlockingConnection example
that creates a bounded queue and runs a simple producer/consumer as threads.

Even when producer.enable_exceptions() is not called (and thus broker delivery
confirms aren't enabled) this multi-threaded example using BlockingConnection
has a much lower throughput than even the asyncio example that has
enable_exceptions(sync=True)
"""

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import json, time, threading

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.amqp_0_9_1_messaging import Connection, Message
from asl_workflow_engine.messaging_exceptions import *

class Worker(threading.Thread):

    def __init__(self, name):
        super().__init__(name=name) # Call Thread constructor
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

    def run(self):
        # Connect to worker queue and process data.
        connection = Connection("amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=0")
        try:
            connection.open()
            #session = connection.session()
            session = connection.session(auto_ack=True)   
            self.consumer = session.consumer(
                self.getName() + '; {"node": {"x-declare": {"auto-delete": true, "arguments": {"x-max-length": 10, "x-overflow": "reject-publish"}}}}'
                #self.getName() + '; {"node": {"x-declare": {"auto-delete": true}}}'
            )
            self.consumer.capacity = 100; # Enable consumer prefetch
            self.consumer.set_message_listener(self.handler)

            connection.start(); # Wait until connection closes.
    
        except MessagingError as e:  # ConnectionError, SessionError etc.
            self.logger.error(e)

        connection.close();

class Publisher(threading.Thread):

    def __init__(self, name):
        super().__init__(name=name) # Call Thread constructor
        # Initialise logger
        self.logger = init_logging(log_name=name)
        self.count = 0

    def run(self):
        # Connect to worker queue and process data.
        connection = Connection("amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=0")
        try:
            connection.open()
            session = connection.session()
            producer = session.producer(self.getName())

            producer.enable_exceptions(sync=True)
            #producer.enable_exceptions()

            for i in range(1000000):
                #print("message {}".format(producer.next_publish_seq_no))
                message = Message(body="hello message {}".format(
                   producer.next_publish_seq_no)
                )
                #print(message)

                try:
                    # When producer.enable_exceptions(True) is set the send()
                    # method will block until message delivery is confirmed
                    # when the broker acks the message or exceptioned if the
                    # broker nacks.
                    # Note that has a serious throughput/latency impact waiting
                    # for the publisher-confirms from the broker. The impact is
                    # especially bad for the case of durable queues.
                    # https://www.rabbitmq.com/confirms.html#publisher-confirms
                    # https://www.rabbitmq.com/confirms.html#publisher-confirms-latency
                    #
                    # When producer.enable_exceptions(False) is set the send()
                    # method will raise an exception that contains the sequence
                    # numbers of messages that have failed to publish. If no
                    # call to enable_exceptions() is made send() just does
                    # basic_publish() with no confirmation, which could result
                    # in message loss.
                    producer.send(message)                    
                except SendError as e:
                    raise e

            connection.start();
    
        except MessagingError as e:  # ConnectionError, SessionError etc.
            self.logger.error(e)

        time.sleep(1)  # Enough time to ensure all messages are sent before closing.
        connection.close();
    
if __name__ == '__main__':
    workers = ["BoundedQueue"]
    for w in workers:
        worker = Worker(name = w)
        worker.start()

    time.sleep(1)  # Enough time to ensure the consumer has created the queue before publishing.

    publisher = Publisher(name = "BoundedQueue")
    publisher.start()

