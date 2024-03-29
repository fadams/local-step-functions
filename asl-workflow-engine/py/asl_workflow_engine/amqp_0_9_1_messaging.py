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
"""
Provides a JMS-like Connection/Session/Producer/Consumer/Message abstraction for
AMQP 0.9.1 connections (to RabbitMQ, though may work with other brokers).

By using JMS-like semantics the intention is to have an API that is already
reasonably abstracted from the underlying messaging fabric, so that it should
*hopefully* be a little simpler to build implementations for other fabrics.
"""

import sys
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3

import json

# Tested using Pika 1.0.1, may not work correctly with earlier versions.
import pika  # sudo pip3 install pika

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.messaging_exceptions import *

class Connection(object):
    def __init__(self, url="amqp://localhost:5672"):
        """
        Creates a connection. A newly created connection must be opened with the
        Connection.open() method before it can be used.

        For RabbitMQ AMQP Connection URL documentation see:
        https://pika.readthedocs.io/en/stable/modules/parameters.html
        https://pika.readthedocs.io/en/stable/examples/using_urlparameters.html
        """
        self.logger = init_logging(log_name="amqp_0_9_1_messaging")
        """
        We want to log the URL, but that would contain the password if present,
        so we create a redacted version for logging purposes.
        """
        p = pika.compat.urlparse(url)
        password = ":xxxxx@" if p.password else "@"
        userpass = p.username + password if p.username else ""
        port = ":" + str(p.port) if p.port else ""
        hostport = p.hostname + port
        netloc = userpass + hostport
        redacted_url = p._replace(netloc=netloc).geturl()
        self.logger.info("Creating Connection with url: {}".format(redacted_url))
        self.parameters = pika.URLParameters(url)

    def open(self, timeout=None):
        """
        Opens the connection.
        """
        if hasattr(self, "connection") and self.connection.is_open:
            raise ConnectionError(
                "Connection to {host}:{port} is already open".format(
                    host=self.parameters.host, port=self.parameters.port
                )
            )

        self.logger.info(
            "Opening Connection to {host}:{port}".format(
                host=self.parameters.host, port=self.parameters.port
            )
        )

        try:
            self.connection = pika.BlockingConnection(self.parameters)
        except pika.exceptions.AMQPConnectionError as e:
            raise ConnectionError(repr(e))

    def is_open(self):
        """
        Return True if the connection is open, False otherwise. 
        """ 
        return hasattr(self, "connection") and self.connection.is_open

    def close(self):
        """
        Closes the connection.
        """
        if self.is_open():
            self.logger.info(
                "Closing Connection to {host}:{port}".format(
                    host=self.parameters.host, port=self.parameters.port
                )
            )
            self.connection.close()

    def session(self, name=None, transactional=False, auto_ack=False):
        """
        Creates a Session object.

        TODO add code so connection can store and retrieve sessions by name.
        TODO with JMS calling createSession should create a Session instance
        and each Session's Consumer MessageListener will run in a different
        thread. Pika is single threaded and moreover generally not thread-safe
        so to have similar behaviour it is (generally) necessary to have a
        separate Pika *connection* per thread. We should threfore create a new
        connection for each session, but for now only support a single Session
        such that the first call to session() will create the Session and
        subsequent calls will return that Session.
        """
        if not hasattr(self, "_session"):
            self._session = Session(self, name, transactional, auto_ack)
        return self._session

    def set_timeout(self, callback, delay):
        """
        Executes the specified callback function after the specified ms delay.
        Intended to have the same semantics as the JavaScript setTimeout().
        NOTE: the timer callbacks are dispatched only in the scope of 
        BlockingConnection.process_data_events() and
        BlockingChannel.start_consuming() e.g. after the start() method below
        has been called.
        Clamp delay to >= 0 as call_later doesn't handle negative values.
        """
        if delay < 0:
            delay = 0
        return self.connection.call_later(delay / 1000, callback)

    def clear_timeout(self, timeout_id):
        """
        Remove a timer if it’s still in the timeout stack.
        Intended to have the same semantics as the JavaScript clearTimeout().
        """
        self.connection.remove_timeout(timeout_id)

    def start(self):
        """
        Starts (or restarts) a connection's delivery of incoming messages.
        A call to start on a connection that has already been started is ignored.

        TODO when multiple sessions are supported start them all.
        There is actually a lot going on in start_consuming as Pika is basically
        single threaded and largely not thread safe the main docs are here
        https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html

        One issue is how to communicate with the Pika dispatch loop from a
        different thread. I *think* that the way to do it is via the
        connection.add_callback_threadsafe(callback) method which requests a
        call to the given function as soon as possible in the context of this
        connection’s thread.

        It's also not clear how to consume on multiple channels as the
        start_consuming call is a method on channel, but it blocks, so how to
        consume on other channels in that case??
        """
        if hasattr(self, "_session"):
            try:
                self._session.channel.start_consuming()
            except pika.exceptions.ConnectionClosedByBroker as e:
                raise ConnectionError(repr(e))

# ------------------------------------------------------------------------------

class Session(object):
    def __init__(self, connection, name, transactional, auto_ack):
        """
        Sessions provide a context for sending and receiving Messages. Messages
        are sent and received using the Producer and Consumer objects associated
        with a Session.

        Each Producer and Consumer is created by supplying either a target or
        source address to the producer and consumer methods of the Session. 
        The address is supplied via a string syntax.
        """
        if not connection.is_open():
            raise SessionError("Unable to create Session as Connection is not open")

        connection.logger.info("Creating Session")  # TODO log name
        self.connection = connection
        self.name = name  # TODO do something useful with name
        # TODO maybe do something with transactional?
        self.auto_ack = auto_ack

        """
        For RabbitMQ AMQP Channel documentation see:
        https://pika.readthedocs.io/en/stable/modules/channel.html
        """
        self.channel = connection.connection.channel()

    def acknowledge(self, message=None, threadsafe=False):
        """
        Acknowledge the given Message. If message is None, then all 
        unacknowledged messages on the session are acknowledged.

        https://www.rabbitmq.com/confirms.html

        param message: the message to acknowledge or None
        type message: Message

        basic_ack(delivery_tag=0, multiple=False)
        If multiple is set to True, the delivery tag is treated as “up to and
        including”, so that multiple messages can be acknowledged with a single
        method. If set to False, the delivery tag refers to a single message.

        If the multiple field is True, and the delivery tag is zero, this 
        indicates acknowledgement of all outstanding messages.
        """
        if message == None:
            def ack():
                """
                The main purpose of this nested function is to make it fairly easy
                to either directly call the underlying pika basic_ack or defer
                to connection.add_callback_threadsafe as a callback.
                """
                self.channel.basic_ack(delivery_tag=0, multiple=True)

            if threadsafe:
                # If called from a foreign thread add ack to the event queue
                # as this is the only thread safe Pika API call.
                self.channel.connection.add_callback_threadsafe(ack)
            else:
                ack()
        else:
            # Only acknowledge the specific message.
            message.acknowledge(multiple=False, threadsafe=threadsafe)

    def recover(self, requeue=False):
        """
        Restarts message delivery with the oldest unacknowledged message. 
        This method asks the server to redeliver all unacknowledged messages on
        a specified channel. Zero or more messages may be redelivered.
        """
        self.channel.basic_recover(requeue)

    def is_open(self):
        """
        Return True if the session is open, False otherwise. 
        """ 
        return self.channel.is_open

    def close(self):
        """
        Closes the session.
        """
        if self.is_open():
            self.logger.info("Closing Session")  # TODO log session name
            self.channel.close()

    def producer(self, target=""):
        """
        Creates a Producer used to send messages to the specified target.
        :param target: The target to which messages will be sent
        :type target: str
        """
        return Producer(self, target)

    def consumer(self, source=""):
        """
        Creates a Consumer used to fetch messages from the specified source.
        :param source: The source of messages
        :type source: str
        """
        return Consumer(self, source)

# ------------------------------------------------------------------------------

class Destination(object):
    def __init__(self):
        """
        For RabbitMQ AMQP Channel documentation see:
        https://pika.readthedocs.io/en/stable/modules/channel.html
        Default values taken from exchange_declare, queue_declare, queue_bind
        """
        self.declare = {
            "queue": "",
            "exchange": "",
            "exchange-type": "direct",
            "passive": False,
            "internal": False,
            "durable": False,
            "exclusive": False,
            "auto-delete": False,
            "arguments": None,
        }

        # Defaults for subscription queues (exclusive and autodelete True)
        self.link_declare = {
            "queue": "",
            "passive": False,
            "internal": False,
            "durable": False,
            "exclusive": True,
            "auto-delete": True,
            "arguments": None,
        }

        """
        The x-subscribe map of a link controls the exclusive and arguments
        fields of a subscription, which relates to the AMQP 0.9.1 basic.consume
        or the AMQP 0.10 message.subscribe protocol commands. This is mainly
        used to request an exclusive subscription. This prevents other
        subscribers from subscribing to the queue.
        """
        self.link_subscribe = {
            "exclusive": False,
            "arguments": None,
        }

        self.bindings = []

    def parse_address(self, address):
        """
        Parses an address string with the following format:

        <name> [ / <subject> ] [ ; <options> ]

        Where options is of the form: { <key> : <value>, ... }

        And values may be numbers, strings, maps (dictionaries) or lists

        The options map permits the following parameters:

        <name> [ / <subject> ] ; {
            node: {
                type: queue | topic,
                durable: True | False,
                auto-delete: True | False,
                x-declare: { ... <declare-overrides> ... },
                x-bindings: [<binding_1>, ... <binding_n>]
            },
            link: {
                name: <link-name>,
                durable: True | False,
                reliability: unreliable | at-most-once | at-least-once | exactly-once,
                x-declare: { ... <declare-overrides> ... },
                x-bindings: [<binding_1>, ... <binding_n>]
            }
        }

        The node refers to the AMQP node e.g. a queue or exchange being referred
        to in the address, whereas the link allows configuration of a logical
        "subscriber queue" that will be created when the address node is an
        exchange such as, for example, news-service/sports.

        Bindings are specified as a map with the following options:

        {
            exchange: <exchange>,
            queue: <queue>,
            key: <key>,
            arguments: <arguments>
        }

        The x-declare map permits protocol specific keys and values to be
        specified when exchanges or queues are declared. These keys and
        values are passed through when creating a node or asserting facts
        about an existing node.

        For Producers the node implicitly defaults to a topic and for Consumers
        it implicitly defaults to a queue, but this may be overridden by setting
        exchange or queue in the x-declare object or by setting node type to
        queue or topic.

        Examples:
        myqueue; {"node": {"x-declare": {"durable": true, "exclusive": true, "auto-delete": true}}}'
        myqueue; {"node": {"x-declare": {"exchange": "test-headers", "exchange-type": "headers", "durable": true, "auto-delete": true}}}

        myqueue; {"node": {"x-declare": {"durable": true, "auto-delete": true}, "x-bindings": [{"exchange": "amq.match", "queue": "myqueue", "key": "data1", "arguments": {"x-match": "all", "data-service": "amqp-delivery", "item-owner": "Sauron"}}]}}

        myqueue; {"node": {"durable": true, "x-bindings": [{"exchange": "amq.match", "queue": "myqueue", "key": "data1", "arguments": {"x-match": "all", "data-service": "amqp-delivery", "item-owner": "Sauron"}}, {"exchange": "amq.match", "queue": "myqueue", "key": "data2", "arguments": {"x-match": "all", "data-service": "amqp-delivery", "item-owner": "Gandalf"}}]}}

        myqueue; {"node": {"x-declare": {"durable": true, "auto-delete": false}}, "link": {"x-subscribe": {"exclusive": true}}}'

        news-service/sports

        news-service/sports; {"node": {"x-declare": {"exchange": "news-service", "exchange-type": "topic"}}}

        news-service/sports; {"node": {"x-declare": {"exchange": "news-service", "exchange-type": "topic", "auto-delete": true}}, "link": {"x-declare": {"queue": "news-queue", "exclusive": false}}}

        Topic exchanges can be declared by message producers as follows:
        ; {"node": {"x-declare": {"exchange": "news-service", "exchange-type": "topic"}}}

        For the case where the address comprises just the options string
        the leading semicolon is optional e.g. the following is also valid:
        {"node": {"x-declare": {"exchange": "news-service", "exchange-type": "topic"}}}

        """
        kv = address.split(";")
        options_string = kv[1] if len(kv) == 2 else "{}"
        kv = kv[0].split("/")
        self.subject = kv[1].strip() if len(kv) == 2 else ""
        self.name = kv[0].strip()

        # Handle case where address comprises just the options string.
        if len(self.name) >= 2 and self.name[0] == "{":
            options_string = self.name
            self.name = ""

        options = json.loads(options_string)

        # print(options_string)
        # print(options)
        # print(self.subject)
        # print(self.name)

        node = options.get("node")
        if node:
            x_declare = node.get("x-declare")
            if x_declare and type(x_declare) == type(self.declare):
                self.declare.update(x_declare)
                if self.name:
                    # If node type is set then set queue or exchange name in
                    # declare if not already explicitly set in x-declare
                    if node.get("type") == "queue" and not self.declare.get("queue"):
                        self.declare["queue"] = self.name
                    if node.get("type") == "topic" and not self.declare.get("exchange"):
                        self.declare["exchange"] = self.name
                else:
                    # Handle case where address comprises just the options string.
                    if node.get("type") == "queue":
                        self.name = self.declare.get("queue", "")
                    if node.get("type") == "topic":
                        self.name = self.declare.get("exchange", "")
                    # Handle edge cases where node type is not explicitly set
                    if not self.name:
                        self.name = self.declare.get("exchange", "")
                    if not self.name:
                        self.name = self.declare.get("queue", "")

            # Can set durable and auto-delete on node as a shortcut if we don't
            # need any other declare overrides.
            if node.get("durable"):
                self.declare["durable"] = True
            if node.get("auto-delete"):
                self.declare["auto-delete"] = True
            # If x-bindings set populate Destination's bindings
            x_bindings = node.get("x-bindings")
            if x_bindings and type(x_bindings) == type(self.bindings):
                self.bindings = x_bindings

        link = options.get("link")
        if link:
            x_declare = link.get("x-declare")
            if x_declare and type(x_declare) == type(self.link_declare):
                self.link_declare.update(x_declare)
            x_subscribe = link.get("x-subscribe")
            if x_subscribe and type(x_subscribe) == type(self.link_subscribe):
                self.link_subscribe.update(x_subscribe)

# ------------------------------------------------------------------------------

class Producer(Destination):
    def __init__(self, session, target):
        """
        A client uses a Producer object to send messages to a destination.
        """
        super().__init__()  # Call Destination constructor

        session.connection.logger.info(
            "Creating Producer with address: {}".format(target)
        )
        self.session = session

        """
        Confirmation delivery_tags/sequence numbers start at 1
        https://www.rabbitmq.com/confirms.html#publisher-confirms
        """
        self.next_publish_seq_no = 1
        self.saved_ack_seq_no = 0
        self.saved_nack_seq_no = 0
        self.saved_seq_no = 0
        self.previous_nack_start = 1

        """
        self.undelivered is used when enable_exceptions(sync=False) has been
        called. It records the sequence numbers of undelivered (unpublished)
        messages as tuples, representing contiguous blocks of unpublished
        messages. This information will be recorded in an exception thrown by
        send() and may be used by applications to identify messages to resend.
        """
        self.undelivered = []

        try:
            self.parse_address(target)
        except ValueError as e:
            raise ProducerError("Failed to parse address: {} {}".format(target, e))

        # Check if an exchange with the name of this destination exists.
        if self.name:
            try:
                # Use temporary channel as the channel gets closed on an exception.
                temp_channel = self.session.connection.connection.channel()
                temp_channel.exchange_declare(self.name, passive=True)
                temp_channel.close()
            except pika.exceptions.ChannelClosedByBroker as e:
                # If 404 NOT_FOUND the specified exchange doesn't exist.
                if e.reply_code == 404:
                    # If no exchange declared assume default direct exchange.
                    if self.name != self.declare.get("exchange"):
                        self.subject = self.name
                        self.name = ""  # Set exchange to default direct.

        if self.declare.get("exchange"):
            # Exchange declare.
            self.session.channel.exchange_declare(
                exchange=self.declare["exchange"],
                exchange_type=self.declare["exchange-type"],
                passive=self.declare["passive"],
                durable=self.declare["durable"],
                auto_delete=self.declare["auto-delete"],
                arguments=self.declare["arguments"],
            )
        # print("name = " + self.name)
        # print("subject = " + self.subject)

    def set_return_callback(self, return_callback):
        """
        Set a callback to be triggered when basic_publish is sent a Message
        that has been rejected and returned by the server because, for example,
        he Message is unroutable.

        For RabbitMQ AMQP Channel documentation see:
        https://pika.readthedocs.io/en/stable/modules/channel.html

        Note that for BlockingConnection we use the underlying channel._impl
        as we actually *want* this to be called asynchronously.

        add_on_return_callback(callback)
        """
        self._return_callback = return_callback
        self.session.channel._impl.add_on_return_callback(self.return_callback)

    def return_callback(self, channel, method, properties, body):
        """
        This is the callback function that will be called when basic_publish is
        sent a message that has been rejected and returned by the server.

        https://pika.readthedocs.io/en/stable/modules/spec.html#pika.spec.Basic.Return
        """
        if hasattr(self, "_return_callback"):
            # Now call the registered return callback
            message = Message(
                body,
                properties=properties.headers,
                content_type=properties.content_type,
                content_encoding=properties.content_encoding,
                durable=(properties.delivery_mode == 2),
                priority=properties.priority,
                correlation_id=properties.correlation_id,
                reply_to=properties.reply_to,
                expiration=properties.expiration,
                message_id=properties.message_id,
                timestamp=properties.timestamp,
                type=properties.type,
                user_id=properties.user_id,
                app_id=properties.app_id,
                cluster_id=properties.cluster_id,
            )
            # These two private attributes are added to Message to enable
            # Message's acknowledge() methods, note however that returned
            # Messages should not be acknowledged.
            message._channel = channel
            message._delivery_tag = 0
            self._return_callback(message)

    def enable_exceptions(self, sync=False):
        """
        With JMS the spec tends towards implying a delivery guarantee, in
        particular if messages are marked persistent. However, most AMQP
        implementations publish asynchronously and have buffering to reduce
        latencies due to protocol round-trips. To meet JMS delivery guarantees
        implementations might allow a sync_publish option, which would wait
        for the basic_ack from the broker. Otherwise any exceptions on send,
        if any, might occur *after* the message that actually caused a failure.

        By default the send() method below publishes asynchronously and does
        *not* raise exceptions unless enabled by calling enable_exceptions().

        When enable_exceptions() is called the default behaviour is to raise an
        exception on the first call to send() after one or more basic_nack has
        been received from the broker.

        The exception contains the sequence numbers of the nacked messages and
        this information may be used by applications to identify messages that
        might need to be resent.

        If sync=True send() will instead block until delivery confirmation by
        the broker. This mode of operation is what the BlockingChannel
        confirm_delivery() does and the underlying basic_publish call will block
        until ack or nack is received. NOTE that setting sync=True could impact
        message throughput and latency considerably and unfortunately the pika
        BlockingChannel doesn't provide a convenient way to block for a batch
        of messages.

        WARNING: do not modify the exception_ack_nack_callback function unless
        you know what you are doing. The gist is that it records the sequence
        numbers of the ack and nack methods returned by the broker.

        The possibility of nacks filling "holes" in the sequencing is part of
        the reason the exception callback is quite complicated. The function
        is basically creating a list of tuples holding each range of undelivered
        messages and we process the Ack method to find self.previous_nack_start
        That is used if the nack sequence number arrives out of sequence so
        we can identify the "hole" to fill.

        One reason to be careful is that the out of sequence case is sporadic
        and only cropped up on edge cases.
        """

        def exception_ack_nack_callback(result):
            method = result.method
            seq_no = method.delivery_tag

            if isinstance(method, pika.spec.Basic.Ack):
                if not method.multiple:
                    prev_seq_no = seq_no
                elif self.saved_nack_seq_no > self.saved_ack_seq_no:
                    prev_seq_no = self.saved_nack_seq_no
                else:
                    prev_seq_no = self.saved_ack_seq_no
                                                        
                if self.saved_seq_no != prev_seq_no - 1:
                    self.previous_nack_start = self.saved_seq_no + 1
                self.saved_seq_no = seq_no

                self.saved_ack_seq_no = seq_no + 1
            elif isinstance(method, pika.spec.Basic.Nack):
                if seq_no < self.saved_ack_seq_no:
                    prev_seq_no = self.previous_nack_start
                else:
                    prev_seq_no = self.saved_ack_seq_no

                if self.undelivered:
                    last = self.undelivered[-1]
                    if last[0] == prev_seq_no:
                        self.undelivered[-1] = (prev_seq_no, seq_no)
                    else:
                        self.undelivered.append((prev_seq_no, seq_no))
                else:
                    self.undelivered.append((prev_seq_no, seq_no))

                self.saved_nack_seq_no = seq_no + 1

        if sync:
            self.session.channel.confirm_delivery()
        else:
            self.saved_ack_seq_no = 1
            self.saved_nack_seq_no = 1

            # Need to access the blocking channel's underlying async channel
            # for this mode of operation.
            self.session.channel._impl.confirm_delivery(
                ack_nack_callback=exception_ack_nack_callback,
            ) 

    def send(self, message, threadsafe=False):
        """
        For RabbitMQ AMQP Channel documentation see:
        https://pika.readthedocs.io/en/stable/modules/channel.html
        https://pika.readthedocs.io/en/stable/modules/spec.html#pika.spec.BasicProperties

        basic_publish(exchange, routing_key, body, properties=None,
                      mandatory=False)
        Delivery mode 2 makes the broker save the message to disk.
        """

        def publish():
            """
            The main purpose of this nested function is to make it fairly easy
            to either directly call the underlying pika basic_publish or defer
            to connection.add_callback_threadsafe as a callback.
            """

            # If message.subject is set use that as the routing_key, otherwise use
            # the Producer target default subject parsed from address string.
            subject = message.subject
            routing_key = subject if subject else self.subject

            # If message.expiration is set to an invalid value (like a
            # non-numeric or a negative value) we "clamp" it to a "0"
            clamped_expiration = None
            if message.expiration is not None:
                try:
                    clamped_expiration=str(int(float(message.expiration)))
                    if clamped_expiration.startswith("-"):
                        clamped_expiration = "0"
                except ValueError as e:
                    clamped_expiration = "0"

            properties = pika.BasicProperties(
                headers=message.properties,
                content_type=message.content_type,
                content_encoding=message.content_encoding,
                delivery_mode=2 if message.durable else 1,
                priority=message.priority,
                correlation_id=message.correlation_id,
                reply_to=message.reply_to,
                expiration=clamped_expiration,
                message_id=message.message_id,
                timestamp=message.timestamp,
                type=message.type,
                user_id=message.user_id,
                app_id=message.app_id,
                cluster_id=message.cluster_id,
            )

            try:
                self.session.channel.basic_publish(
                    exchange=self.name,
                    routing_key=routing_key,
                    body=message.body,
                    properties=properties,
                    mandatory=message.mandatory,
                )
            except pika.exceptions.NackError as e:
                undelivered = [(self.next_publish_seq_no, self.next_publish_seq_no)]
                send_exception = SendError(
                    "Failed to send message: undelivered = {}".format(undelivered)
                )
                send_exception.undelivered = undelivered
                raise send_exception

        if self.undelivered:  # Will only be True if enable_exceptions(sync=False).
            """
            If enable_exceptions() has been called with sync=False (the default)
            the sequence numbers of nacked messages are recorded in undelivered,
            which is a list of tuples each of which records the first and last
            sequence number of groups of messages that failed to be published.
            There may be multiple items if groups of nacked messages are non-
            contiguous. If there are any undelivered/unpublished messages an
            exception is thrown which contains a copy of this list of tuples.
            """
            send_exception = SendError(
                "Failed to send messages: undelivered = {}".format(
                    self.undelivered
                )
            )
            send_exception.undelivered = self.undelivered.copy()
            self.undelivered = []
            raise send_exception
        elif threadsafe:
            self.session.connection.connection.add_callback_threadsafe(publish)
        else:
            publish()

        self.next_publish_seq_no += 1

# ------------------------------------------------------------------------------

class Consumer(Destination):
    def __init__(self, session, source):
        """
        A client uses a Consumer object to receive messages from a destination.
        """
        super().__init__()  # Call Destination constructor

        session.connection.logger.info(
            "Creating Consumer with address: {}".format(source)
        )
        self.session = session

        # Set default capacity/message prefetch to 500
        self._capacity = 500
        self.session.channel.basic_qos(prefetch_count=self._capacity)

        try:
            self.parse_address(source)
        except ValueError as e:
            raise ConsumerError("Failed to parse address: {} {}".format(source, e))

        # Check if an exchange with the name of this destination exists.
        exchange = None
        if self.name:
            try:
                # Use temporary channel as the channel gets closed on an exception.
                temp_channel = self.session.connection.connection.channel()
                temp_channel.exchange_declare(self.name, passive=True)
                temp_channel.close()
                exchange = self.name
            except pika.exceptions.ChannelClosedByBroker as e:
                if self.subject:
                    if self.name == self.declare.get("exchange"):
                        exchange = self.name
                    else:
                        raise ConsumerError(e.reply_text)
                # Otherwise we assume default direct exchange

        if exchange:  # Is this address an exchange?
            # Destination is an exchange, create subscription queue and
            # add binding between exchange and queue with subject as key.
            if self.declare.get("queue"):
                self.name = self.declare.get("queue")
            elif self.link_declare.get("queue"):
                self.name = self.link_declare.get("queue")
            else:
                self.name = ""

            if len(self.bindings) == 0 and self.subject:
                self.bindings.append(
                    {"queue": self.name, "exchange": exchange, "key": self.subject}
                )

        # Declare queue, exchange and bindings as necessary
        if self.declare.get("exchange"):
            # Exchange declare - unusual scenario for Consumer, but handle it.
            self.session.channel.exchange_declare(
                exchange=self.declare["exchange"],
                exchange_type=self.declare["exchange-type"],
                passive=self.declare["passive"],
                durable=self.declare["durable"],
                auto_delete=self.declare["auto-delete"],
                arguments=self.declare["arguments"],
            )
        # Queue declare
        declare = self.declare
        if exchange and not self.declare.get("queue"):
            declare = self.link_declare
        if self.name == "":
            declare["auto-delete"] = True

        result = self.session.channel.queue_declare(
            queue=self.name,
            passive=declare["passive"],
            durable=declare["durable"],
            exclusive=declare["exclusive"],
            auto_delete=declare["auto-delete"],
            arguments=declare["arguments"],
        )
        """
        Get the queue name from the result of the queue_declare to deal with
        the case of server created names when we pass queue="" to queue_declare
        see https://www.rabbitmq.com/tutorials/tutorial-three-python.html
        """
        self.name = result.method.queue

        for binding in self.bindings:
            if binding["exchange"] == "":
                continue  # Can't bind to default
            self.session.channel.queue_bind(
                queue=binding["queue"],
                exchange=binding["exchange"], 
                routing_key=binding.get("key"),
                arguments=binding.get("arguments"),
            )

    def set_message_listener(self, message_listener):
        """
        For RabbitMQ AMQP Channel documentation see:
        https://pika.readthedocs.io/en/stable/modules/channel.html

        basic_consume(queue, on_message_callback, auto_ack=False,
                      exclusive=False, consumer_tag=None, arguments=None,
                      callback=None)

        To specify an exclusive subscription to a queue use an address like:
        myqueue; {"node": {"x-declare": {"durable": true, "auto-delete": false}}, "link": {"x-subscribe": {"exclusive": true}}}'
        """
        ex = self.link_subscribe.get("exclusive", False)
        args = self.link_subscribe.get("arguments", None)
        self._message_listener = message_listener
        try:
            self.session.channel.basic_consume(
                on_message_callback=self.message_listener,
                queue=self.name,
                auto_ack=self.session.auto_ack,
                exclusive=ex,
                arguments=args
            )
        except pika.exceptions.ChannelClosedByBroker as e:
            raise ConsumerError(e.reply_text)

    def message_listener(self, channel, method, properties, body):
        """
        This is the Consumer's default message listener, its job is to create
        a Message instance that encapsulates some of the AMQP details into
        something more akin to a JMS Message, then delegate to the registered
        message listener.

        https://pika.readthedocs.io/en/stable/modules/spec.html#pika.spec.Basic.Deliver
        https://pika.readthedocs.io/en/stable/modules/spec.html#pika.spec.BasicProperties

        channel: pika.Channel
        method: pika.spec.Basic.Deliver
        properties: pika.spec.BasicProperties
        body: bytes
        """
        if hasattr(self, "_message_listener"):
            # Now call the registered message listener
            message = Message(
                body,
                properties=properties.headers,
                content_type=properties.content_type,
                content_encoding=properties.content_encoding,
                redelivered=method.redelivered,
                durable=(properties.delivery_mode == 2),
                priority=properties.priority,
                correlation_id=properties.correlation_id,
                reply_to=properties.reply_to,
                expiration=properties.expiration,
                message_id=properties.message_id,
                timestamp=properties.timestamp,
                type=properties.type,
                user_id=properties.user_id,
                app_id=properties.app_id,
                cluster_id=properties.cluster_id,
            )
            # These two private attributes are added to Message to enable
            # Message's acknowledge() methods
            message._channel = channel
            message._delivery_tag = method.delivery_tag
            self._message_listener(message)

    @property
    def capacity(self):
        return self._capacity

    @capacity.setter
    def capacity(self, capacity):
        self._capacity = capacity
        self.session.channel.basic_qos(prefetch_count=capacity)

# ------------------------------------------------------------------------------

class Message(object):
    def __init__(
        self,
        body="",  # Pika expects empty string not None for no body.
        properties=None,
        content_type=None,
        content_encoding=None,
        redelivered=False,
        durable=True,
        mandatory=False,
        priority=None,
        correlation_id=None,
        reply_to=None,
        expiration=None,
        message_id=None,
        timestamp=None,
        type=None,
        user_id=None,
        app_id=None,
        cluster_id=None,
        subject=None,
    ):
        """
        Provides an abstraction for messages comprising a body, application
        properties and a set of headers used by the messaging fabric to identify
        and route messages and provide additional metadata.
        """
        self.body = body
        self.properties = properties  # Holds application property key/value pairs

        # Values below from:
        # pika.BasicProperties(delivery_mode=2)
        # https://pika.readthedocs.io/en/stable/modules/spec.html#pika.spec.BasicProperties
        # https://stackoverflow.com/questions/18403623/rabbitmq-amqp-basicproperties-builder-values
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.redelivered = redelivered
        self.durable = durable
        self.mandatory = mandatory
        self.priority = priority
        self.correlation_id = correlation_id
        self.reply_to = reply_to
        self.expiration = expiration
        self.message_id = message_id
        self.timestamp = timestamp
        self.type = type
        self.user_id = user_id
        self.app_id = app_id
        self.cluster_id = cluster_id

        if self.properties == None:
            self.properties = {}
        self.subject = subject  # Set subject *after* self.properties initialised.

    def __repr__(self):
        args = []
        for name in [
            "content_type",
            "content_encoding",
            "priority",
            "message_id",
            "type",
            "user_id",
            "app_id",
            "cluster_id",
            "reply_to",
            "correlation_id",
            "expiration",
            "timestamp",
            "redelivered",
            "durable",
            "mandatory",
            "properties",
        ]:
            value = self.__dict__[name]
            if value is not None:
                args.append("%s=%r" % (name, value))
        if self.subject:
            args.append("subject=%r" % self.subject)
        if self.body is not None:
            if args:
                args.append("body=%r" % self.body)
            else:
                args.append(repr(self.body))
        return "Message(%s)" % ", ".join(args)

    """
    Unlike AMQP 1.0 AMQP 0.9.1 doesn't have a Message subject, but it is a
    useful concept as often we wish to publish messages to a generic producer
    Node such as a topic exchange and have messages delivered based on their
    subject. Although basic_publish allows one to achieve the same result it is
    much more coupled with AMQP 0.9.1 (and AMQP 0.10) protocol details than
    if it were a property of the Message, which is also more intuitive.
    We map "subject" to a message property that is unlikely to collide with any
    application set message properties, this will be used by the Producer.send()
    but will also be available to consumers as a Message property.
    """

    @property
    def subject(self):
        return self.properties.get("x-amqp-0-9-1.subject")

    @subject.setter
    def subject(self, subject):
        if subject:
            self.properties["x-amqp-0-9-1.subject"] = subject
            # print(self.properties)

    def acknowledge(self, multiple=True, threadsafe=False):
        """
        Acknowledge this Message.

        Messages that have been received but not acknowledged may be redelivered.

        basic_ack(delivery_tag=0, multiple=False)
        If multiple is set to True, the delivery tag is treated as “up to and
        including”, so that multiple messages can be acknowledged with a single
        method. If set to False, the delivery tag refers to a single message.

        If the multiple field is True, and the delivery tag is zero, this 
        indicates acknowledgement of all outstanding messages.
        """
        if hasattr(self, "_channel"):
            def ack():
                """
                The main purpose of this nested function is to make it fairly easy
                to either directly call the underlying pika basic_ack or defer
                to connection.add_callback_threadsafe as a callback.
                """
                if multiple:
                    """
                    The multiple == True branch behaves like JMS Message.acknowledge()
                    https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#acknowledge--
                    By invoking acknowledge on a consumed message, a client
                    acknowledges all messages consumed by the session that the
                    message was delivered to. 
                    """
                    self._channel.basic_ack(delivery_tag=0, multiple=True)
                elif self._delivery_tag:
                    self._channel.basic_ack(delivery_tag=self._delivery_tag)

            if threadsafe:
                # If called from a foreign thread add ack to the event queue
                # as this is the only thread safe Pika API call.
                self._channel.connection.add_callback_threadsafe(ack)
            else:
                ack()

