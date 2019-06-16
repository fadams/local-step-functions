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

from asl_workflow_engine.state_engine import StateEngine
from asl_workflow_engine.exceptions import *

class EventDispatcher(object):

    def __init__(self, logger, config):
        """
        :param logger: The Workflow Engine logger
        :type logger: logger
        :param config: Event queue configuration dictionary
        :type config: dict
        """
        self.logger = logger
        self.logger.info("Creating EventDispatcher")
        self.config = config

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
            from asl_workflow_engine.amqp_0_9_1_messaging import Connection
        elif self.config["queue_type"] == "AMQP-0.10": # TODO
            from asl_workflow_engine.amqp_0_10_messaging import Connection
        elif self.config["queue_type"] == "AMQP-1": # TODO
            from asl_workflow_engine.amqp_1_messaging import Connection
        elif self.config["queue_type"] == "NATS": # TODO
            from asl_workflow_engine.nats_messaging import Connection
        elif self.config["queue_type"] == "SQS": # TODO AWS SQS
            from asl_workflow_engine.sqs_messaging import Connection

        self.state_engine = StateEngine()

        # Connect to event queue and start main event loop.
        connection = Connection(self.config["connection_url"])
        try:
            connection.open()
            session = connection.session()
            self.receiver = session.receiver(self.config["queue_name"])
            self.receiver.capacity = 100; # Enable receiver prefetch
            #print(self.receiver.capacity)
            self.receiver.set_message_listener(self.dispatch)

            self.sender = session.sender(self.config["queue_name"])

            connection.start();
            #connection.close();
        except ConnectionError as e:
            self.logger.error(e)
        except SessionError as e:
            self.logger.error(e)

    def dispatch(self, message):
        #print("EventDispatcher message_listener called")
        #print(message.body)
        #print(vars(message))
        self.state_engine.notify(message.body)
        """
        TODO putting message acknowledge here is just a placeholder. When the
        StateEngine actually starts doing useful things ASL Tasks might take
        an arbitrary time to execute and they might fail! We should really only
        acknowledge on success so the message might be redelivered to try
        again. Similarly for the Parallel State we should only acknowledge when
        all responses have returned. This stuff is quite a bit of extra work
        """
        message.acknowledge()


#if __name__ == "__main__":
#    event_dispatcher = EventDispatcher()

