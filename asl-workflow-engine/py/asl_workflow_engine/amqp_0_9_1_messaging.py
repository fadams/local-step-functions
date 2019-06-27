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
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

# Tested using Pika 1.0.1, may not work correctly with earlier versions.
import pika # sudo pip3 install pika

from asl_workflow_engine.logger import init_logging
from asl_workflow_engine.exceptions import *

class Connection(object):

    def __init__(self, url="amqp://localhost:5672"):
    #def __init__(self, url="amqp://localhost:5672", **options): # may be useful
        """
        Creates a connection. A newly created connection must be opened with the
        Connection.open() method before it can be used.

        For RabbitMQ AMQP Connection URL documentation see:
        https://pika.readthedocs.io/en/stable/modules/parameters.html
        https://pika.readthedocs.io/en/stable/examples/using_urlparameters.html
        """
        self.logger = init_logging(log_name="amqp_0_9_1_messaging")
        self.logger.info("Creating Connection with url {}".format(url))
    
        self.parameters = pika.URLParameters(url)

    def open(self, timeout=None):
        """
        Opens the connection.
        """
        if hasattr(self, "connection") and self.connection.is_open: 
            raise ConnectionError("Connection to {host}:{port} is already open".
                format(host=self.parameters.host,
                       port=self.parameters.port))

        self.logger.info("Opening Connection to {host}:{port}".
            format(host=self.parameters.host,
                   port=self.parameters.port))

        try:
            self.connection = pika.BlockingConnection(self.parameters)
        except pika.exceptions.AMQPConnectionError as e:
            raise ConnectionError(e)

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
            self.logger.info("Closing Connection to {host}:{port}".
                format(host=self.parameters.host,
                       port=self.parameters.port))
            self.connection.close()

    def session(self, name=None, transactional=False):
        """
        Creates a Session object.
        """
        # TODO add code so connection can store and retrieve sessions by name.
        self.session = Session(self, name, transactional)
        return self.session

    def set_timeout(self, callback, delay):
        """
        Executes the specified callback function after the specified ms delay.
        Intended to have the same semantics as the JavaScript setTimeout().
        NOTE: the timer callbacks are dispatched only in the scope of 
        BlockingConnection.process_data_events() and
        BlockingChannel.start_consuming() e.g. after the start() method below
        has been called.
        """
        return self.connection.call_later(delay/1000, callback)

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
        if hasattr(self, "session"):
            try:
                self.session.channel.start_consuming()
            except pika.exceptions.ConnectionClosedByBroker as e:
                raise ConnectionError(e)

#-------------------------------------------------------------------------------

class Session(object):

    def __init__(self, connection, name, transactional):
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

        connection.logger.info("Creating Session") # TODO log name
        self.connection = connection
        self.name = name # TODO do something useful with name
        # TODO maybe do something with transactional?

        """
        For RabbitMQ AMQP Channel documentation see:
        https://pika.readthedocs.io/en/stable/modules/channel.html
        """
        self.channel = connection.connection.channel()

        # Enable delivery confirmations https://www.rabbitmq.com/confirms.html
        # I don't think we need/want this to be set, this is about passing a
        # callback to be notified by the Broker when a message has been
        # confirmed as received or rejected.
        #self.channel.confirm_delivery()

    def acknowledge(self, message=None): 
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
            self.channel.basic_ack(delivery_tag=0, multiple=True)
        else:
            message.acknowledge()

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
            self.logger.info("Closing Session") # TODO log session name
            self.channel.close()

    def producer(self, target=""):
        """
        Creates a Producer used to send messages to the specified target.
        :param target: The target to which messages will be sent
        :type target: str
        """
        return Producer(self, target)

    def consumer(self, source):
        """
        Creates a Consumer used to fetch messages from the specified source.
        :param source: The source of messages
        :type source: str
        """
        return Consumer(self, source)

#-------------------------------------------------------------------------------

class Destination(object):

    def __init__(self):
        """
        For RabbitMQ AMQP Channel documentation see:
        https://pika.readthedocs.io/en/stable/modules/channel.html
        Default values taken from exchange_declare, queue_declare, queue_bind
        """
        self.declare = {"queue": "", "exchange": "", "exchange_type": "direct",
                        "passive": False, "internal": False, "durable": False,
                        "exclusive": False, "auto-delete": False,
                        "arguments": None}

        self.bindings = []
        #self.bindings = [{"queue": "", "exchange": "",
        #                  "key": None, "arguments": None}]

    def parse_address(self, address):
        """
        Parses an address string with the following format:

        <name> [ / <subject> ] [ ; <options> ]

        Where options is of the form: { <key> : <value>, ... }

        And values may be numbers, strings, maps (dictionaries) or lists

        The options map permits the following parameters:

        <name> [ / <subject> ] ; {
            create: always | sender | receiver | never,
            delete: always | sender | receiver | never,
            node: {
                type: queue | topic,
                durable: True | False,
                x-declare: { ... <declare-overrides> ... },
                x-bindings: [<binding_1>, ... <binding_n>]
            },
            link: {
                name: <link-name>,
                durable: True | False,
                reliability: unreliable | at-most-once | at-least-once | exactly-once,
                x-declare: { ... <declare-overrides> ... },
                x-bindings: [<binding_1>, ... <binding_n>],
                x-subscribe: { ... <subscribe-overrides> ... }
            }
        }

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
        """
        kv = address.split(";")
        options_string = kv[1] if len(kv) == 2 else "{}"
        # TODO parse options string into x-bindings/x-declare, etc.
        kv = kv[0].split("/")
        self.subject = kv[1] if len(kv) == 2 else ""
        self.name = kv[0]

        #print(options_string)
        #print(self.subject)
        #print(self.name)

#-------------------------------------------------------------------------------

class Producer(Destination):

    def __init__(self, session, target):
        """
        """
        super().__init__() # Call Destination constructor

        session.connection.logger.info("Creating Producer with address {}".format(target))
        self.session = session

        self.parse_address(target)
        #print(self.declare)
        #print(self.bindings)

        # TODO Declare queue, exchange, bindings as necessary

        # Check if an exchange with the name of this destination exists.
        try:
            # Use temporary channel as the channel gets closed on an exception.
            temp_channel = self.session.connection.connection.channel()
            temp_channel.exchange_declare(self.name, passive=True)
            temp_channel.close()
        except pika.exceptions.ChannelClosedByBroker as e:
            #print(e.reply_code)
            #print(e.reply_text)
            # If 404 NOT_FOUND the specified exchange doesn't exist. If no
            # exchange declare options are present then assume default direct.
            if e.reply_code == 404:
                # TODO check for exchange declare options
                #print("Assuming default direct!")
                self.subject = self.name
                self.name = ""

        #print("name = " + self.name)
        #print("subject = " + self.subject)

    def send(self, message):
        """
        For RabbitMQ AMQP Channel documentation see:
        https://pika.readthedocs.io/en/stable/modules/channel.html
        https://pika.readthedocs.io/en/stable/modules/spec.html#pika.spec.BasicProperties

        basic_publish(exchange, routing_key, body, properties=None,
                      mandatory=False)
        Delivery mode 2 makes the broker save the message to disk.
        """

        print("message.subject = " + str(message.subject))
        # If message.subject is set use that as the routing_key, otherwise use
        # the Producer target default subject parsed from address string.
        routing_key = message.subject if message.subject else self.subject

        print("exchange = " + self.name)
        print("routing_key = " + routing_key)

        properties=pika.BasicProperties(headers=message.properties,
                                        content_type=message.content_type,
                                        content_encoding=message.content_encoding,
                                        delivery_mode=2 if message.durable else 1,
                                        priority=message.priority,
                                        correlation_id=message.correlation_id,
                                        reply_to=message.reply_to,
                                        expiration=message.expiration,
                                        message_id=message.message_id,
                                        timestamp=message.timestamp,
                                        type=message.type,
                                        user_id=message.user_id,
                                        app_id=message.app_id,
                                        cluster_id=message.cluster_id)

        res = self.session.channel.basic_publish(exchange=self.name,
                                                 routing_key=routing_key,
                                                 body=message.body,
                                                 properties=properties)
        return res

#-------------------------------------------------------------------------------

class Consumer(Destination):

    def __init__(self, session, source):
        """
        """
        super().__init__() # Call Destination constructor

        session.connection.logger.info("Creating Consumer with address {}".format(source))
        self.session = session

        # Set default capacity/message prefetch to 500
        self._capacity = 500
        self.session.channel.basic_qos(prefetch_count=self._capacity)

        self.parse_address(source)
        #print(self.declare)
        #print(self.bindings)

        # Declare queue, exchange, bindings as necessary

        # exchange_declare(exchange, exchange_type='direct', passive=False, durable=False, auto_delete=False, internal=False, arguments=None, callback=None)

        self.session.channel.queue_declare(queue=self.name,
                                           passive=self.declare["passive"],
                                           durable=self.declare["durable"],
                                           exclusive=self.declare["exclusive"],
                                           auto_delete=self.declare["auto-delete"],
                                           arguments=self.declare["arguments"])
        #print("Bindings:")
        for binding in self.bindings:
            print(binding)
            if binding["exchange"] == "" : continue # Can't bind to default
            self.session.channel.queue_bind(queue=binding["queue"],
                                            exchange=binding["exchange"], 
                                            routing_key=binding["key"],
                                            arguments=binding["arguments"])

    def set_message_listener(self, message_listener):
        """
        For RabbitMQ AMQP Channel documentation see:
        https://pika.readthedocs.io/en/stable/modules/channel.html

        basic_consume(queue, on_message_callback, auto_ack=False,
                      exclusive=False, consumer_tag=None, arguments=None,
                      callback=None)
        """
        self._message_listener = message_listener
        self.session.channel.basic_consume(on_message_callback=self.message_listener,   
                                           queue=self.name)

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
            #delivery_tag = method.delivery_tag
            #print("***** Delivery tag = " + str(delivery_tag))

            message = Message(body, properties=properties.headers,
                              content_type = properties.content_type,
                              content_encoding = properties.content_encoding,
                              durable = (properties.delivery_mode == 2),
                              priority = properties.priority,
                              correlation_id = properties.correlation_id,
                              reply_to = properties.reply_to,
                              expiration = properties.expiration,
                              message_id = properties.message_id,
                              timestamp = properties.timestamp,
                              type = properties.type,
                              user_id = properties.user_id,
                              app_id = properties.app_id,
                              cluster_id = properties.cluster_id)

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

#-------------------------------------------------------------------------------

class Message(object):

    def __init__(self, body=None, properties=None, content_type = None,
                 content_encoding = None, durable = True, priority = None,
                 correlation_id = None, reply_to = None, expiration = None,
                 message_id = None, timestamp = None, type = None,
                 user_id = None, app_id = None, cluster_id = None, subject = None):
        """
        """
        self.body = body
        self.properties = properties # This holds the message header key/value pairs

        # Values below from:
        # pika.BasicProperties(delivery_mode=2)
        # https://pika.readthedocs.io/en/stable/modules/spec.html#pika.spec.BasicProperties
        # https://stackoverflow.com/questions/18403623/rabbitmq-amqp-basicproperties-builder-values
        self.content_type = content_type
        self.content_encoding = content_encoding
        #self.headers = None # same as self.properties
        #self.delivery_mode = None # Probably use bool self.durable instead
        self.durable = durable
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

        if self.properties == None: self.properties = {}
        self.subject = subject # Set subject *after* self.properties initialised.

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
            print(self.properties)

    def acknowledge(self):
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
            self._channel.basic_ack(delivery_tag=self._delivery_tag)

