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
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3


import asyncio, importlib
from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.messaging_exceptions import *

try:  # Attempt to use ujson if available https://pypi.org/project/ujson/
    import ujson as json
except:  # Fall back to standard library json
    import json

class EventDispatcher(object):
    def __init__(self, state_engine, config):
        """
        :param logger: The Workflow Engine logger
        :type logger: logger
        :param config: Configuration dictionary
        :type config: dict
        """
        self.logger = init_logging(log_name="asl_workflow_engine")
        self.logger.info("Creating EventDispatcher, using {} JSON parser".format(json.__name__))

        self.queue_config = config["event_queue"]  # TODO Handle missing config
        self.notifier_config = config["notifier"]  # TODO Handle missing config
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

        """
        Connection Factory for the event queue. The idea is to eventually
        allow the ASL workflow engine to connect to alternative event queue
        implementations in order to allow maximum flexibility.
        """
        name = (
            self.queue_config.get("queue_type", "AMQP-0.9.1")
            .lower()
            .replace("-", "_")
            .replace(".", "_")
        )

        if name.endswith("_asyncio"):
            self.name = "asl_workflow_engine." + name[:-8] + "_messaging_asyncio"
        else:
            self.name = "asl_workflow_engine." + name + "_messaging"

        self.logger.info("Loading messaging module {}".format(self.name))

        # Load the module whose name is derived from the specified queue_type.
        try:
            messaging = importlib.import_module(self.name)
            globals()["Connection"] = messaging.Connection
            globals()["Message"] = messaging.Message
        except ImportError as e:
            self.logger.error(e)
            sys.exit(1)

    
    def heartbeat(self):
        print("**** EventDispatcher heartbeat ****")
        #self.state_engine.heartbeat()
        self.set_timeout(self.heartbeat, 1000)
    

    def start(self):
        # Connect to event queue and start the main event loop.
        # TODO This code will connect on broker startup, but need to add code to
        # reconnect for cases where the broker fails and then restarts.
        connection = Connection(self.queue_config["connection_url"])
        try:
            connection.open()
            session = connection.session()
            """
            The asl_workflow_events queue is a shared event queue, that is to
            say every asl_workflow_engine instance receives events from this
            queue. This is used to publish StartExecution events to and is non-
            exclusive so multiple asl_workflow_engine instances can consume from
            it and will thus load-balance executions across multiple instances.
            """
            self.queue_name = self.queue_config.get("queue_name", "asl_workflow_events")
            shared_queue = self.queue_name + '; {"node": {"durable": true}}'
            shared_event_consumer = session.consumer(shared_queue)
            shared_event_consumer.capacity = 1000  # Enable consumer prefetch
            shared_event_consumer.set_message_listener(self.dispatch)

            """
            the instance_event_consumer is an event queue that is set up for
            each asl_workflow_engine instance. This should be an exclusive
            queue such that only a single instance can consume from it. This
            queue is used for the remainder of the events for each execution.
            The idea of having an event queue per instance is because most
            messaging implementations scale across queues so simply having lots
            of consumers on a single queue is fine if the bottleneck is due to
            the consumer performance, but if the bottleneck is due to limits
            of the messaging fabric then it won't help. Another reason for a
            per instance queue is because with the Parallel and Map states we
            would like each branch to notify the same instance when complete.
            """
            instance_id = self.queue_config.get("instance_id", "")
            self.instance_queue_name = self.queue_name + "-" + instance_id
            instance_queue = self.instance_queue_name + '; {"node": {"durable": true}, "link": {"x-subscribe": {"exclusive": true}}}'

            instance_event_consumer = session.consumer(instance_queue)
            instance_event_consumer.capacity = 1000  # Enable consumer prefetch
            instance_event_consumer.set_message_listener(self.dispatch)

            """
            The event_queue_producer is used to publish events corresponding
            to state transitions. This can publish to both the asl_workflow_events
            queue that is shared by every asl_workflow_engine instance and also
            the per-instance queues of each instance. The Message subject is
            used to select which queue the Message should be published to.
            """
            self.event_queue_producer = session.producer(self.queue_name)

            """
            The topic_producer specifies the topic that notification events
            should be published to. Notification events are analogous to AWS
            CloudWatch events and the JSON format of the notification Messages
            is the same as that used by CloudWatch.
            """
            self.topic_producer = session.producer(self.notifier_config["topic"])

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
            self.clear_timeout = connection.clear_timeout

            """
            Start a periodic "heartbeat". The idea of this is that sometimes
            "bad things happen", for example if a service goes down we might
            be sat stuck in a Task state. Now things *should* carry on when the
            service is restarted, but for that to work we'd need to ensure
            redelivery of unacknowledged messages via recover call. Similarly
            in the TaskDispatcher we might have message requests for which no
            responses have been received and those are likely to require reaping
            for cases where no explicit timeout has been added to the State.
            """
            #self.set_timeout(self.heartbeat, 1000)

            """
            Share messaging session with state_engine.task_dispatcher. This
            is to allow rpcmessage invocations that share the same message
            fabric instance as the event queue to reuse connections etc.
            """
            self.state_engine.task_dispatcher.start(session)

            connection.start()  # Blocks until event loop exits.
        except MessagingError as e:
            self.logger.error(e)
            sys.exit(1)

        connection.close()

    # asyncio version of the start() method above
    async def start_asyncio(self):
        # Connect to event queue and start the main event loop.
        # TODO This code will connect on broker startup, but need to add code to
        # reconnect for cases where the broker fails and then restarts.
        connection = Connection(self.queue_config["connection_url"])
        try:
            await connection.open()
            session = await connection.session()
            """
            The asl_workflow_events queue is a shared event queue, that is to
            say every asl_workflow_engine instance receives events from this
            queue. This is used to publish StartExecution events to and is non-
            exclusive so multiple asl_workflow_engine instances can consume from
            it and will thus load-balance executions across multiple instances.
            """
            self.queue_name = self.queue_config.get("queue_name", "asl_workflow_events")
            shared_queue = self.queue_name + '; {"node": {"durable": true}}'
            shared_event_consumer = await session.consumer(shared_queue)
            shared_event_consumer.capacity = 1000  # Enable consumer prefetch
            await shared_event_consumer.set_message_listener(self.dispatch)

            """
            the instance_event_consumer is an event queue that is set up for
            each asl_workflow_engine instance. This should be an exclusive
            queue such that only a single instance can consume from it. This
            queue is used for the remainder of the events for each execution.
            The idea of having an event queue per instance is because most
            messaging implementations scale across queues so simply having lots
            of consumers on a single queue is fine if the bottleneck is due to
            the consumer performance, but if the bottleneck is due to limits
            of the messaging fabric then it won't help. Another reason for a
            per instance queue is because with the Parallel and Map states we
            would like each branch to notify the same instance when complete.
            """
            instance_id = self.queue_config.get("instance_id", "")
            self.instance_queue_name = self.queue_name + "-" + instance_id
            instance_queue = self.instance_queue_name + '; {"node": {"durable": true}, "link": {"x-subscribe": {"exclusive": true}}}'

            instance_event_consumer = await session.consumer(instance_queue)
            instance_event_consumer.capacity = 1000  # Enable consumer prefetch
            await instance_event_consumer.set_message_listener(self.dispatch)

            """
            The event_queue_producer is used to publish events corresponding
            to state transitions. This can publish to both the asl_workflow_events
            queue that is shared by every asl_workflow_engine instance and also
            the per-instance queues of each instance. The Message subject is
            used to select which queue the Message should be published to.
            """
            self.event_queue_producer = await session.producer(self.queue_name)

            """
            The topic_producer specifies the topic that notification events
            should be published to. Notification events are analogous to AWS
            CloudWatch events and the JSON format of the notification Messages
            is the same as that used by CloudWatch.
            """
            self.topic_producer = await session.producer(self.notifier_config["topic"])

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
            self.clear_timeout = connection.clear_timeout

            """
            Start a periodic "heartbeat". The idea of this is that sometimes
            "bad things happen", for example if a service goes down we might
            be sat stuck in a Task state. Now things *should* carry on when the
            service is restarted, but for that to work we'd need to ensure
            redelivery of unacknowledged messages via recover call. Similarly
            in the TaskDispatcher we might have message requests for which no
            responses have been received and those are likely to require reaping
            for cases where no explicit timeout has been added to the State.
            """
            #self.set_timeout(self.heartbeat, 1000)

            """
            Share messaging session with state_engine.task_dispatcher. This
            is to allow rpcmessage invocations that share the same message
            fabric instance as the event queue to reuse connections etc.
            """
            await self.state_engine.task_dispatcher.start_asyncio(session)

            await connection.start()  # Blocks until event loop exits.
        except MessagingError as e:
            self.logger.error(e)
            sys.exit(1)

        connection.close()

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
            item = json.loads(message.body.decode("utf8"))
            self.unacknowledged_messages[self.message_count] = message
            self.state_engine.notify(item, self.message_count)
            self.message_count += 1
        except ValueError as e:
            self.logger.error(
                "Message {} does not contain valid JSON".format(message.body)
            )
            message.acknowledge(multiple=False)
        except Exception as e:
            """
            If state_engine.notify bombs out with an exception it is likely to
            be due to invalid data or the ASL not being handled correctly.
            It's hard to know the best course of action, but for now catch the
            Exception, log error then acknowledge the "poison" message to
            prevent it from being endlessly redelivered.
            """
            self.logger.error(
                "Message {} caused the exception: {}:{} - dropping the message!".format(
                    message.body, type(e).__name__, str(e)
                )
            )
            message.acknowledge(multiple=False)

    def acknowledge(self, id):
        """
        Look up the message with the given id in the unacknowledged_messages
        dictionary and acknowledge it, then remove the message from the
        dictionary. See the comment for unacknowledged_messages in constructor.
        """
        message = self.unacknowledged_messages[id]
        message.acknowledge(multiple=False)
        del self.unacknowledged_messages[id]

    def publish(self, item, threadsafe=False, start_execution=False):
        """
        Publish the supplied item to the event queue hosted on the underlying
        messaging fabric. This method is mainly here to abstract some of the
        implementation details from the state engine.

        The start_execution field is used to select between publishing to the
        event queue that is shared by all instances of asl_workflow_engine and
        the per instance queue.

        Setting content_type to application/json isn't necessary for correct
        operation, however it is the correct thing to do:
        https://www.ietf.org/rfc/rfc4627.txt.
        """
        if start_execution:
            subject = self.queue_name
        else:
            subject = self.instance_queue_name

        message = Message(json.dumps(item), content_type="application/json")
        message.subject = subject  # Selects the queue to publish to
        self.event_queue_producer.send(message, threadsafe)

    def broadcast(self, subject, item, carrier_properties=None):
        """
        Broadcast the supplied item to the topic hosted on the underlying
        messaging fabric. This method is mainly here to abstract some of the
        implementation details from the state engine.

        Setting content_type to application/json isn't necessary for correct
        operation, however it is the correct thing to do:
        https://www.ietf.org/rfc/rfc4627.txt.
        """
        message = Message(json.dumps(item), content_type="application/json", properties=carrier_properties)
        message.subject = subject
        self.topic_producer.send(message)

