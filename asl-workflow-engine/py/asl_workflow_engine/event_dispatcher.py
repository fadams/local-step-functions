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
# PYTHONPATH=.. python3 event_dispatcher.py
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
        :param config: Event queue configuration dictionary
        :type config: dict
        """
        self.logger = logger
        self.logger.info("Creating EventDispatcher")
        self.config = config["event_queue"] # TODO Handle missing config

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
        some time due to waiting for Task responses and Wait and Parallel
        states we must retain any unacknowledged messages. The message count
        is a simple one-up number used as a key.
        """
        self.unacknowledged_messages = {}
        self.message_count = 0

        # TODO validate that config contains the keys we need.

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

        # Connect to event queue and start main event loop.
        # TODO This code will connect on broker startup, but need to add code to
        # reconnect for cases when the broker fails and then restarts.
        connection = Connection(self.config["connection_url"])
        try:
            connection.open()
            self.set_timeout = connection.set_timeout
            session = connection.session()
            self.receiver = session.receiver(self.config["queue_name"])
            self.receiver.capacity = 100; # Enable receiver prefetch
            #print(self.receiver.capacity)
            self.receiver.set_message_listener(self.dispatch)
            self.sender = session.sender(self.config["queue_name"])
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
        log the failure
        """
        try:
            item = json.loads(message.body)
            self.unacknowledged_messages[self.message_count] = message
            self.state_engine.notify(item, self.message_count)
            self.message_count += 1
        except ValueError as e:
            self.logger.info("Message {} does not contain valid JSON".format(message.body))
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
        from asl_workflow_engine.amqp_0_9_1_messaging import Message
        """
        Publish supplied item to the event queue hosted on the underlying
        messaging fabric. This method is mainly here to abstract some of the
        implementation details from the state engine.
        """
        message = Message(json.dumps(item))
        self.sender.send(message)

