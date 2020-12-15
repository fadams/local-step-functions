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
# PYTHONPATH=.. python3 subscribe_to_notifications_asyncio.py
#

"""
This test application acts as a simple "NotificationHandler" the idea being that
it subscribes to notification events from the asl_workflow_engine when the
State Machine starts or ends (e.g. RUNNING, SUCCEEDED or FAILED etc.)
The intention is to act rather like AWS CloudWatch events:
https://docs.aws.amazon.com/step-functions/latest/dg/cw-events.html
https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/CloudWatchEventsandEventPatterns.html

The events are published to:
asl_workflow_engine/<state_machine_arn>.<status>

That is to say the asl_workflow_engine topic exchange with a subject comprising
the state_machine_arn and the status separated by a dot. e.g. 
asl_workflow_engine/arn:aws:states:local:0123456789:stateMachine:simple_state_machine.SUCCEEDED

Subscribe to specific events using the full subject or groups of event using
wildcards, for example:
asl_workflow_engine/arn:aws:states:local:0123456789:stateMachine:simple_state_machine.*
Subscribes to all notification events published for a given state machine.

asl_workflow_engine/#
Subscribes to all events from the asl_workflow_engine

The format of the message body is as described in
https://docs.aws.amazon.com/step-functions/latest/dg/cw-events.html
the "detail" field gives access to the "executionArn", "stateMachineArn",
"input", "output" and other relevant information.
"""

# Just an aide-memoire for state machine and execution ARN formats
# state_machine_arn = "arn:aws:states:local:0123456789:stateMachine:simple_state_machine"
# name = str(uuid.uuid4())
# execution_arn = "arn:aws:states:local:0123456789:execution:simple_state_machine:" + name

import sys
assert sys.version_info >= (3, 6) # Bomb out if not running Python3.6

import asyncio, json, time

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.amqp_0_9_1_messaging_asyncio import Connection, Message
from asl_workflow_engine.messaging_exceptions import *

class NotificationHandler(object):
    def __init__(self, state_machines):
        # Initialise logger
        self.logger = init_logging(log_name="subscribe_to_notifications")
        self.state_machines = state_machines

    async def run(self):
        connection = Connection("amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=0")
        try:
            await connection.open()
            session = await connection.session()

            for s in self.state_machines:
                """
                Use nested function so we can capture the subscription value s.
                Now closures look at the value in the surrounding scope at the
                *time the function is called* so just using a simple nested
                def handler(message): function would always give the last value
                of the loop. The following creates a scoped closure where we
                pass in the subscription value in the loop and return the actual
                handler as a closure that has the subscription in its scope.
                https://stackoverflow.com/questions/12423614/local-variables-in-nested-functions
                """
                def scoped_handler(subscription):                
                    def handler(message):
                        print("Message received for subscription:")
                        print(subscription)
                        print(message)
                        print()
                        message.acknowledge() # Acknowledges the original request
                    return handler

                # Example topic subscription with options to create (declare)
                # the topic exchange if it doesn't already exist.
                #consumer = await session.consumer("asl_workflow_engine/" + s + '; {"node": {"x-declare": {"exchange": "asl_workflow_engine", "exchange-type": "topic", "durable": true}}}')

                # Simple topic subscription - will fail if the specified topic
                # exchange doesn't already exist.
                consumer = await session.consumer("asl_workflow_engine/" + s)
                consumer.capacity = 100; # Enable consumer prefetch
                await consumer.set_message_listener(scoped_handler(s))


            await connection.start(); # Blocks until event loop exits.

        except MessagingError as e:
            self.logger.error(e)

        connection.close();

if __name__ == '__main__':
    # Subscribe to notification events from these State Machines
    state_machines = [
        "arn:aws:states:local:0123456789:stateMachine:simple_state_machine.*",  
        "arn:aws:states:local:0123456789:stateMachine:caller_state_machine.*",
        "arn:aws:states:local:0123456789:stateMachine:parallel1.*",
        "arn:aws:states:local:0123456789:stateMachine:map1.*",
        "arn:aws:states:local:0123456789:stateMachine:iterate2_state_machine.*",
    ]
    notification_handler = NotificationHandler(state_machines)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(notification_handler.run())
    loop.close()

