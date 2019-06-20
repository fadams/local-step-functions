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
Provides a JMS-like Connection/Session/Sender/Receiver/Message abstraction for
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
        are sent and received using the Sender and Receiver objects associated
        with a Session.

        Each Sender and Receiver is created by supplying either a target or
        source address to the sender and receiver methods of the Session. 
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
        # Don't think we need/want this to be set, this is about passing a
        # callback to be notified by the Broker when a message has been
        # confirmed as received or rejected
        self.channel.confirm_delivery()

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

    def sender(self, target, **options):
        """
        Creates a Sender used to send messages to the specified target.
        :param target: The target to which messages will be sent
        :type target: str
        """
        return Sender(self, target, options)

    def receiver(self, source, **options):
        """
        Creates a receiver used to fetch messages from the specified source.
        :param source: The source of messages
        :type source: str
        """
        return Receiver(self, source, options)

#-------------------------------------------------------------------------------

class Destination(object):

    def __init__(self):
        """
        For RabbitMQ AMQP Channel documentation see:
        https://pika.readthedocs.io/en/stable/modules/channel.html
        Default values taken from exchange_declare, queue_declare, queue_bind
        """
        self.exchange = {"name": "", "exchange_type": "direct", "passive": False,
                         "durable": False, "auto_delete": False,
                         "internal": False, "arguments": None}
        self.queue = {"name": "", "passive": False, "durable": False,
                      "exclusive": False, "auto_delete": False,
                      "arguments": None}
        self.bindings = [{"queue_name": "", "exchange_name": "",
                          "routing_key": None, "arguments": None}]

    def parse_address(self, address):
        """
        """
        #print("parse_address " + address)
        # TODO do this properly
        self.queue["name"] = address

        self.bindings[0]["queue_name"] = address
        self.bindings[0]["routing_key"] = address

#-------------------------------------------------------------------------------

class Sender(Destination):

    def __init__(self, session, target, options):
        """
        """
        super().__init__() # Call Destination constructor

        session.connection.logger.info("Creating Sender with address {}".format(target))
        self.session = session

        self.parse_address(target)
        #print(self.exchange)
        #print(self.queue)
        #print(self.bindings)

        # TODO Declare queue, exchange, bindings as necessary

    def send(self, message, sync=True, timeout=None):
        """
        For RabbitMQ AMQP Channel documentation see:
        https://pika.readthedocs.io/en/stable/modules/channel.html
        https://pika.readthedocs.io/en/stable/modules/spec.html#pika.spec.BasicProperties

        basic_publish(exchange, routing_key, body, properties=None,
                      mandatory=False)
        Delivery mode 2 makes the broker save the message to disk.
        """
        #properties=pika.BasicProperties(delivery_mode=2)

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

        # TODO This needs more thought, for example if target is something
        # simple like amq.topic or something else that just specifies the
        # exchange we need to find the subject either from the address or
        # the message.subject field (if present)
        print("exchange = " + self.exchange["name"])
        print("routing_key = " + self.queue["name"])
        routing_key = self.queue["name"]

        res = self.session.channel.basic_publish(exchange=self.exchange["name"],
                                                 routing_key=routing_key,
                                                 body=message.body,
                                                 properties=properties)
        return res

#-------------------------------------------------------------------------------

class Receiver(Destination):

    def __init__(self, session, source, options):
        """
        """
        super().__init__() # Call Destination constructor

        session.connection.logger.info("Creating Receiver with address {}".format(source))
        self.session = session

        # Set default capacity/message prefetch to 500
        self._capacity = 500
        self.session.channel.basic_qos(prefetch_count=self._capacity)

        self.parse_address(source)
        #print(self.exchange)
        #print(self.queue)
        #print(self.bindings)

        # Declare queue, exchange, bindings as necessary

        # exchange_declare(exchange, exchange_type='direct', passive=False, durable=False, auto_delete=False, internal=False, arguments=None, callback=None)

        self.session.channel.queue_declare(queue=self.queue["name"],
                                           passive=self.queue["passive"],
                                           durable=self.queue["durable"],
                                           exclusive=self.queue["exclusive"],
                                           auto_delete=self.queue["auto_delete"],
                                           arguments=self.queue["arguments"])
        print("Bindings:")
        for binding in self.bindings:
            print(binding)
            if binding["exchange_name"] == "" : continue # Can't bind to default
            self.session.channel.queue_bind(queue=binding["queue_name"],
                                            exchange=binding["exchange_name"], 
                                            routing_key=binding["routing_key"],
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
                                           queue=self.queue["name"])

    def message_listener(self, channel, method, properties, body):
        """
        This is the Receiver's default message listener, its job is to create
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

            #print("----")
            #print(properties)
            #print("----")
            #print(vars(properties))
            #print("----")

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
            # Message's acknowledge() methos
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
                 user_id = None, app_id = None, cluster_id = None):
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

