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

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import json
from asl_workflow_engine.exceptions import *

class EventDispatcher(object):

    def __init__(self, logger, state_engine, config):
        """
        :param logger: The Workflow Engine logger
        :type logger: logger
        :param config: Configuration dictionary
        :type config: dict
        """
        self.logger = logger
        self.logger.info("Creating EventDispatcher")
        self.config = config["event_queue"] # TODO Handle missing config
        # TODO validate that config contains the keys we need.

        """
        Create an association with the state engine and give that a reference
        back to this event dispatcher so that it can publish events and make
        use of the set_timeout time scheduler.
        """
        self.state_engine = state_engine
        self.state_engine.event_dispatcher = self

        """
        Incoming messages should only be acknowledged when they have been
        fully processed by the StateEngine to allow the messaging fabric to
        redeliver the message upon a failure. Because processing might take
        some time due to waiting for Task responses and the Wait and Parallel
        states we must retain any unacknowledged messages. The message count
        is a simple one-up number used as a key.
        """
        self.unacknowledged_messages = {}
        self.message_count = 0

#        print(self.config["queue_name"])
#        print(self.config["queue_type"])
#        print(self.config["connection_url"])
#        print(self.config["connection_options"])

        # Connection Factory for the event queue. The idea is to eventually
        # allow the ASL workflow engine to connect to alternative event queue
        # implementations in order to allow maximum flexibility.
        # TODO better error handling and logging.
        if self.config["queue_type"] == "AMQP-0.9.1":
            from asl_workflow_engine.amqp_0_9_1_messaging import Connection, Message
        elif self.config["queue_type"] == "AMQP-0.10": # TODO
            from asl_workflow_engine.amqp_0_10_messaging import Connection, Message
        elif self.config["queue_type"] == "AMQP-1": # TODO
            from asl_workflow_engine.amqp_1_messaging import Connection, Message
        elif self.config["queue_type"] == "NATS": # TODO
            from asl_workflow_engine.nats_messaging import Connection, Message
        elif self.config["queue_type"] == "SQS": # TODO AWS SQS
            from asl_workflow_engine.sqs_messaging import Connection, Message

        # Connect to event queue and start the main event loop.
        # TODO This code will connect on broker startup, but need to add code to
        # reconnect for cases where the broker fails and then restarts.
        connection = Connection(self.config["connection_url"])
        try:
            connection.open()
            session = connection.session()
            self.consumer = session.consumer(self.config["queue_name"])
            self.consumer.capacity = 100; # Enable consumer prefetch
            self.consumer.set_message_listener(self.dispatch)
            self.producer = session.producer(self.config["queue_name"])

            """
            TODO Passing connection.set_timeout here is kind of ugly but I can't
            think of a nicer way yet. The issue is that we want the state engine
            to be able to support timeouts, however that is being called from
            Pika's event loop, which isn't thread-safe and we want asynchronous
            timeouts not blocking timeouts as when in an ASL Wait state we still
            want to be able to process other events that might be available on
            the event queue. This (hopefully temporary) approach is Pika specific.
            """
            self.set_timeout = connection.set_timeout

            """
            Share messaging connection with state_engine.task_dispatcher. This
            is to allow rpcmessage invocations that share the same message
            fabric instance as the event queue to reuse connections etc.
            """
            #state_engine.task_dispatcher.connect(connection)
            state_engine.task_dispatcher.connect(session)

            connection.start(); # Blocks until event loop exits.
        except ConnectionError as e:
            self.logger.error(e)
        except SessionError as e:
            self.logger.error(e)

        connection.close();

    def dispatch(self, message):
        """
        Dispatch the body of the message received from event queue hosted on the
        underlying messaging fabric. This method is mainly here to abstract some
        of the implementation details from the state engine, which should be
        somewhat agnostic of the event queue implementation.

        The message body *should* be a JSON string, but we trap any exception
        that may be thrown during conversion and simply dump the message and
        log the failure. Note that some messaging systems (such as Pika)
        incorrectly, IMHO, encode strings as *opaque binary* on the messaging
        fabric rather than string. This can lead to errors when doing things
        like JSON decoding as json.loads(message.body) works fine in Python 3.6
        but does not in earlier versions. This is kind of irksome given that
        Python has a string type and supports introspection, but there you go.
        """
        try:
            #print(message.subject)
            item = json.loads(message.body.decode("utf8"))
            self.unacknowledged_messages[self.message_count] = message
            self.state_engine.notify(item, self.message_count)
            self.message_count += 1
        except ValueError as e:
            self.logger.error("Message {} does not contain valid JSON".format(message.body))
            message.acknowledge()
        except Exception as e:
            """
            If state_engine.notify bombs out with an exception it is likely to
            be due to invalid data or the ASL not being handled correctly.
            It's hard to know the best course of action, but for now catch the
            Exception, log error then acknowledge the message to prevent it
            being endlessly redelivered.
            """
            self.logger.error("Message {} caused the exception: {}:{} - dropping the message!".format(message.body, type(e).__name__, str(e)))
            message.acknowledge()

    def acknowledge(self, id):
        """
        Look up the message with the given id in the unacknowledged_messages
        dictionary and acknowledge it, then remove the message from the
        dictionary. See the comment for unacknowledged_messages in constructor.
        """
        message = self.unacknowledged_messages[id]
        message.acknowledge()
        del self.unacknowledged_messages[id]

    def publish(self, item):
        # TODO this import should be handled by the "Connection Factory for the
        # event queue" code in the constructor.
        from asl_workflow_engine.amqp_0_9_1_messaging import Message
        """
        Publish supplied item to the event queue hosted on the underlying
        messaging fabric. This method is mainly here to abstract some of the
        implementation details from the state engine.

        Setting content_type to application/json isn't necessary for correct
        operation, however it is the correct thing to do:
        https://www.ietf.org/rfc/rfc4627.txt.
        """
        message = Message(json.dumps(item), content_type="application/json")
        self.producer.send(message)

