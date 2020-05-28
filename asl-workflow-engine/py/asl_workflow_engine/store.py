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
This creates data stores for ASL state machines (and their associated metadata)
and for execution metadata and execution history. The stores have dictionary
semantics where the ASL store is indexed by by the ARN of the ASL State Machine.
The format of the stored ASL dict objects is the same as DescribeStateMachine.
https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeStateMachine.html.

The execution metadata is indexed by by the ARN of the execution.
The format of the stored execution dict objects is the same as DescribeExecution.
https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeExecution.html

The execution history is also indexed by by the ARN of the execution.
The format of the stored execution history list objects is the same as that
of the "events" list from GetExecutionHistory and each event is an *immutable*
dict of the form illustrated in the GetExecutionHistory documentation.
https://docs.aws.amazon.com/step-functions/latest/apireference/API_GetExecutionHistory.html
"""

import sys
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3

import json, os, threading, time, uuid, weakref
from collections import OrderedDict
from collections.abc import Mapping, MutableMapping, Sequence

from asl_workflow_engine.logger import init_logging


class JSONStore(MutableMapping):
    """
    https://stackoverflow.com/questions/3387691/how-to-perfectly-override-a-dict

    This class implements dict semantics, but uses a (very) simple JSON store.
    The approach is simple and in particular writes are inefficient as the
    approach to simply to serialise the store to JSON and write the JSON to
    the json_store file. This store is mainly intended to be used in basic
    scenarios and single instance test cases. For production scenarios it is
    recommended to use the Redis backed store, though that has more dependencies.
    """

    def __init__(self, json_store_name, *args, **kwargs):
        self.json_store = json_store_name

        self.logger = init_logging(log_name="asl_workflow_engine")
        self.logger.info("Creating JSONStore: {}".format(self.json_store))

        try:
            with open(self.json_store, "r") as fp:
                self.store = json.load(fp)
            self.logger.info(
                "JSONStore loading: {}".format(self.json_store)
            )
        except IOError as e:
            self.store = {}
        except ValueError as e:
            self.logger.warning(
                "JSONStore {} does not contain valid JSON".format(
                    self.json_store
                )
            )
            self.store = {}

        self.update(*args, **kwargs)  # use the free update to set keys

    def __str__(self):
        return str(self.store)

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        self.store[key] = value
        self._update_store()

    def __delitem__(self, key):
        del self.store[key]
        self._update_store()

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    #def update(self, iterable=tuple(), **kwargs):  # Uses "free" built-in update

    # From collections.abc.Mapping:
    def __contains__(self, key):
        return key in self.store

    def _update_store(self):
        # Serialise store to JSON and write the JSON to the json_store file.
        try:
            with open(self.json_store, "w") as fp:
                json.dump(self.store, fp)
            self.logger.info("Updating JSONStore: {}".format(self.json_store))
        except IOError as e:
            raise

    def set_ttl(self, key, ttl):
        pass

    def get_cached_view(self, key, default=None):
        return self.get(key, default)


class SimpleStore(dict):
    """
    Just a dict with trivial do nothing set_ttl and get_cached_view methods.
    """
    def set_ttl(self, key, ttl):
        pass

    def get_cached_view(self, key, default=None):
        return self.get(key, default)


class RedisStore(MutableMapping):
    """
    RedisStore is a Redis backed container implementing dict semantics.
    It is intended to be subclassed by RedisDictStore and RedisListStore as
    the use case is for storing a dict of dicts or a dict of lists.

    For a basic dict or list a Redis hash or list would suffice and the Pottery
    library gives those dict or list semantics. It is even possible to store
    a dict or list inside a Pottery RedisDict or RedisList. The issue is that
    those objects are (trasparently) JSON serialised. That is potentially not
    an issue for small and infrequently used items, but for objects that change
    a lot or could grow quite large, such as lists, the serialisation costs is
    O(2*N) *for every change* as each change to the object requires
    deserialisation->modification->serialisation.

    RedisStore basically just indexes the Redis hash or list objects (wrapped
    in Pottery RedisDict or RedisList. Redis itself actually indexes those,
    but this class applies an appropriate key prefix to disambiguate/namespace
    them and wraps the accesses in dict semantics so applications can access
    the RedisStore as if it were a dict of dicts or dict of lists, minimising
    any changes needed to the application in order to use the store.

    The reason we have RedisDictStore and RedisListStore is mainly because
    Redis does not allow empty hashes or lists, so if an application were to
    store say an empty list when it came to accessing that there would be no
    way of knowing its type without storing additional information. There are
    lots of ways to do that, but they all require additional writes and reads
    to the Redis server. As most applications are likely to want a dict of dicts
    or a dict of lists rather than a dict of dicts or lists it seems better to
    create specialisations rather than a more general form with additional cost.

    RedisStore makes use of Redis Server Assisted Client Side Caching where
    available (requires Redis > 6.0.0). This is enabled by accessing an object
    via the get_cached_view() method. The intended use case is again to mitigate
    deserialisation costs for objects accessed frequently. An example might be
    where we have JSON objects stored in a dict that we store in RedisStore.
    The get_cached_view() method will retrieve the object and deserialise it
    and store the *deserialised* object in an LRU cache. If that object is
    modified Redis will send a cache invalidation message and the object will be
    removed from the cache.
    """

    def get_connection(full_url, logger):
        """
        Class method to create or return the Redis connection. Defers imports
        of Redis libraries until we actually attempt a Redis connection.
        Use class method as we want all RedisStore instances to share connection.
        """
        try:
            return RedisStore.connection
        except AttributeError:
            """
            get_connection() supports URLs of the form:
            redis://localhost:6379?connection_attempts=20&retry_delay=10
            Redis URLs don't actually have a connection_attempts/retry_delay
            but we add them as they are convenient and consistent with AMQP URLs
            """
            split = full_url.split("?")  # Get query part
            url = split[0]  # The main URL before the ?
            # Use list comprehension to create options dict by splitting on & then =
            options = dict([] if len(split) == 1 else [
                i.split("=") for i in split[1].split("&")
            ])

            logger.info("RedisStore: Opening Connection to {}".format(url))

            # https://pypi.org/project/redis/
            # Use https://github.com/brainix/pottery for more Pythonic redis access
            from redis import Redis, ConnectionPool   # pip3 install redis
            from pottery import RedisDict, RedisList  # pip3 install pottery
            RedisStore.Redis = Redis
            RedisStore.RedisDict = RedisDict
            RedisStore.RedisList = RedisList

            # Defaults are the same defaults that Pika uses for AMQP connections.
            connection_attempts = int(options.get("connection_attempts", "1"))
            retry_delay = float(options.get("retry_delay", "2.0"))

            for i in range(connection_attempts):
                RedisStore.connection = Redis.from_url(url)
                try:
                    RedisStore.connection.ping()  # Check connection has succeeded
                    return RedisStore.connection
                except Exception as e:
                    err = e
                    logger.warning("RedisStore: {} retrying".format(e))
                    del RedisStore.connection
                time.sleep(retry_delay)

            logger.error("RedisStore: {} connection_attempts exceeded".format(err))
            sys.exit(1)

    def __init__(self, url, key="", cache_size=128, daemon=False):
        """
        key is used as a prefix/namespace in the Redis keyspace, so if we create
        multiple instances of a RedisStore then using different prefix keys for
        each instance will prevent odd result if we happen to need to store items
        with the same index keys in different stores.

        cache_size is the maximum number of items to store in the LRU cache used
        by get_cached_view(). Setting cache_size to 0 will disable the cache, so
        calls to get_cached_view() will behave exactly like get() or [].

        daemon is used to set the thread listening for cache invalidation
        messages from the Redis server as a daemon or not.
        """
        self.logger = init_logging(log_name="asl_workflow_engine")
        self.logger.info(
            "Creating {} {}: {}".format(key, self.__class__.__name__, url)
        )

        self.redis = RedisStore.get_connection(url, self.logger)

        # Get Redis version as an int, so 6.0.0 would be 600 to aid version tests.
        redis_version = self.redis.info("server").get("redis_version", "0")
        self.redis_version = int(redis_version.replace(".", ""))
        if self.redis_version < 600:
            self.logger.warning("Server assisted client side caching is not "
                                "supported in Redis version {}, read performance "
                                "might be reduced.".format(redis_version))

        self.key = key

        # Enabling the cache is deferred until first call to get_cached_view().
        self.cache = None
        self.cache_size = cache_size
        self.daemon = daemon
        self.tracker_id = None

    def __del__(self):
        self.stop()

    def _start_tracking(self):
        """
        Enable Redis server-assisted client side caching. Requires Redis >= 6.0.0
        https://redis.io/topics/client-side-caching
        https://engineering.redislabs.com/posts/redis-assisted-client-side-caching-in-python/
        """

        """
        Redis serialisation protocol 2 (RESP2) currently used in redis-py lacks
        the RESP3 "push" capability, so Redis server-assisted client side caching
        also broadcasts invalidation messages using the existing PubSub support.
        RESP2 lacks the ability to multiplex different kind of information in
        the same connection so we create a separate tracker subscriber connection
        from the same connection pool as the main Redis connection.
        """
        self.tracker = RedisStore.Redis(connection_pool=self.redis.connection_pool)

        """
        It is important to retrieve a copy of self.tracker.client_id() here
        as when we later call pubsub.subscribe() that results in a new logical
        connection, so if we were to call tracker.client_id() again it would
        have a different value. This is a subtle gotcha, as it is tempting to
        use self.tracker.client_id() in the CLIENT TRACKING ON call which
        would cause a redirect *to the wrong logical client*.
        """
        self.tracker_id = self.tracker.client_id()

        """
        In the following code the obvious way to register the handler is using:
        pubsub.subscribe(**{"__redis__:invalidate": self._cache_invalidation_handler})
        That works, but results in a circular reference with the tracker_thread.
        The circular reference means the RedisStore won't get garbage collected,
        so instead we create a weakref to self then use a lambda as the
        invalidate handler which, when called, will dereference the weakref
        and call the real _cache_invalidation_handler. Now when del is called
        on a RedisStore instance everything should be GC'd and released.
        https://stackoverflow.com/questions/2436302/when-to-use-weak-references-in-python

        Another gotcha is the pubsub.run_in_thread() call. It's easy to assume
        that, like many similar subscription APIs, the subscription thread will
        *block* (and thus consume no CPU) until a message is sent from the server.
        Unfortunately that's not the case and, as may be seen from the source,
        https://github.com/andymccurdy/redis-py/blob/master/redis/client.py#L3708
        the thread simply calls get_message in a loop. Setting a sleep_time
        trades latency for CPU, and with 10ms latency the CPU used is negligible
        but it's still a bit ugly to have a busy-wait.
        """
        weakref_to_self = weakref.ref(self)
        pubsub = self.tracker.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(**{
            "__redis__:invalidate": lambda message:
                weakref_to_self()._cache_invalidation_handler(message)
        })
        #self.tracker_thread = pubsub.run_in_thread(sleep_time=0.01, daemon=self.daemon)

        """
        Rather than use pubsub.run_in_thread() as above, the following block runs
        pubsub.listen() in a thread. It's a slightly more complex approach, but
        in this case the thread blocks rather than busy-waiting, though that
        adds additional complexity in the stop() method too. Again we use a
        weakref to the pubsub object rather than the more obvious self.pubsub
        to avoid a circular reference that would prevent garbage collection.
        """
        self.weakref_to_pubsub = weakref.ref(pubsub)
            
        def pubsub_listener():
            for message in pubsub.listen():
                """
                pubsub.listen() will block until there is a message available.
                If there is a message handler it will call that rather than
                yield. In general this means that this call will *appear* to do
                nothing and won't yield. To cleanly exit it is necessary to
                remove the message handler and do a basic subscribe, then
                publish a dummy message that will cause pubsub.listen() to
                yield. See the stop() method for how to cleanly unblock and exit.
                """
                break

        self.tracker_thread = threading.Thread(
            target=pubsub_listener,
            daemon=self.daemon,
        )
        self.tracker_thread.start()
        
        """
        Switch on tracking. We need to use execute_command() here as redis-py
        doesn't currently support it via a higher-level API call.
        """
        self.redis.execute_command(
            "CLIENT", "TRACKING", "ON", "REDIRECT", self.tracker_id
        )

    def _cache_invalidation_handler(self, message):
        """
        Handles key invalidation messages sent by the Redis server.
        """
        keys = message["data"]  # This will contain an array of invalidated keys.        
        for k in keys:
            # Keys are passed as an array of binary strings, with prefixes.
            key = self._remove_prefix(k.decode("utf-8"))
            if key in self.cache:
                del self.cache[key]

    def _remove_prefix(self, key):
        """
        Boiler plate helper method to remove the internal prefixes used in the
        Redis keys and return the keys as used by applications using RedisStore.
        """
        prefix = self.key + ":"
        if key.startswith(prefix):
            return key[len(prefix):]
        return key

    def __delitem__(self, key):
        k = self.key + ":" + key
        self.redis.delete(k)

    def __len__(self):
        """
        Warning. Because this class is storing Redis data structures (hashes and
        lists) the keys used in RedisStore are prefixed Redis database keys.
        There is no Redis API call to get a count of keys with a given prefix,
        so we use scan with match set to the prefix. This works, but be aware
        that a call to len will have O(N) time complexity and not O(1).
        This shouldn't be an issue as the use cases for RedisStore in this
        application shouldn't need to call len, but it's important to be aware.
        """
        count = 0
        cursor = "0"  # Not 0 or the while loop will immediately terminate.
        while cursor != 0:
            cursor, iterable = self.redis.scan(cursor=cursor, match=self.key + ":*")
            count = count + len(iterable)
        return count

    def __iter__(self):
        """
        Generator function that yields results from a Redis scan. The yielded
        results are converted from the binary strings returned by scan and have
        their prefixes stripped, so this call behaves semantically like iterating
        a dict yielding the dict keys.
        """
        cursor = "0"  # Not 0 or the while loop will immediately terminate.
        while cursor != 0:
            cursor, iterable = self.redis.scan(cursor=cursor, match=self.key + ":*")
            yield from (self._remove_prefix(value.decode("utf-8")) for value in iterable)

    # From collections.abc.Mapping:
    def __contains__(self, key):
        k = self.key + ":" + key
        return bool(self.redis.exists(k))

    def set_ttl(self, key, ttl):
        k = self.key + ":" + key
        self.redis.expire(k, ttl)

    def _write_to_cache(self, key, value):
        """
        The cache is *eventually consistent* and currently the only place
        _write_to_cache() is called is when a cache read fails in
        get_cached_view(). The implication of this is that even for *local*
        writes to the store the new data won't be available in the cache until
        after the server has sent the cache invalidation message.
        It would be trivial to add a _write_to_cache() call in the __setitem__
        calls of RedisDictStore and RedisListStore, which would make local
        updates available immediately in the cache for the case where
        applications might write a whole dict or list to the store, but in
        general clients might do something like the following:
        asl_store[state_machine_arn]["type"] = "EXPRESS"
        Under the hood that *actually* writes to the pottery RedisDict object.
        It would be possible to interpose a call to _write_to_cache() onto
        the pottery RedisDict and RedisList mutator methods, but on balance
        eventually consistent semantics should be good enough though it is 
        important to be aware of the subtlety.
        """
        if self.cache is None:  # Only write to cache if it is enabled.
            return value

        # Store *deserialised* dict or list objects in cache.
        if isinstance(value, RedisStore.RedisDict):
            cached_value = dict(value)
        elif isinstance(value, RedisStore.RedisList):
            cached_value = list(value)
        else:
            cached_value = value

        self.cache[key] = cached_value  # Write to cache
        # Implement LRU semantics e.g. evict the oldest item from cache
        if len(self.cache) > self.cache_size:
            oldest = next(iter(self.cache))
            del self.cache[oldest]
        return cached_value

    def get_cached_view(self, key, default=None):
        """
        We only need to enable server-assisted client side caching if we are
        actually using caching. In general caching is mainly useful here if we
        are doing many reads of infrequently updated immutable objects, so we
        can amortise the deserialisation costs in addition to minimising the
        network round-trips of Redis GET calls. If objects are read infrequently
        or the dicts or lists being accessed need to be modified then use the
        basic get() or [] operator. This method returns a *native* dict or list
        and not a RedisDict or RedisList, so any changes wil not be reflected
        in the underlying Redis store.
        """

        """
        Older Redis versions don't support RSACSC so just return uncached object
        for now. TODO we could have producers publish when they make changes,
        e.g. self.redis.publish("__redis__:invalidate", "test1")
        That should work, though will be less efficient as producers and
        consumers are decoupled, so every change might need published and the
        increased costs might well negate the benefits of caching.
        """
        if self.redis_version < 600 or self.cache_size == 0:
            return self[key]

        if self.tracker_id is None:
            self.cache = OrderedDict()
            self._start_tracking()

        try:  # Get from local cache
            value = self.cache[key]
            self.cache.move_to_end(key)
        except KeyError:  # Get from Redis server
            value = self[key]
            value = self._write_to_cache(key, value)

        return value

    def stop(self):
        """
        Tidy up resources. This will also be called by the destructor, so it
        shouldn't be necessary to call this explicitly.
        """
        if self.tracker_id is not None:
            self.redis.execute_command("CLIENT", "TRACKING", "OFF")

            try: # If subscribing is implemented using pubsub.run_in_thread()
                self.tracker_thread.stop()
            except AttributeError:
                """
                If subscribing is implemented using pubsub.listen() in a worker
                thread we remove the message handler by calling subscribe()
                again, but with no handler for the channel. We then publish to
                the channel, which will now cause pubsub.listen() to yield as
                there is no listener. Allowing the yield to succeed causes the
                worker thread to exit.
                """
                pubsub = self.weakref_to_pubsub()
                pubsub.subscribe("__redis__:invalidate")
                self.redis.publish("__redis__:invalidate", "exit")
                self.tracker_thread.join()

            self.tracker.close()
            self.tracker_id = None
            self.redis.close()


class RedisDictStore(RedisStore):
    """
    https://stackoverflow.com/questions/3387691/how-to-perfectly-override-a-dict

    This class implements dict of dicts semantics. Note that whilst the dicts
    stored in a RedisDictStore are themselves mutable, stored as Redis hashes,
    any objects that they contain are serialised and immutable, though they
    may be overwritten by a new serialised object.
    """
    def __getitem__(self, key):
        k = self.key + ":" + key
        return RedisStore.RedisDict(redis=self.redis, key=k)

    def __setitem__(self, key, value):
        k = self.key + ":" + key

        if isinstance(value, RedisStore.RedisDict) and value.key == k:
            pass  # If setting the same RedisDict instance we needn't do anything more.
        elif isinstance(value, Mapping):
            # Delete then store seems the only way to replace entire Redis hash.
            self.redis.delete(k)
            if value:
                RedisStore.RedisDict(value, redis=self.redis, key=k)
        else:
            raise TypeError("RedisDictStore can only store items of type Mapping")


class RedisListStore(RedisStore):
    """
    https://stackoverflow.com/questions/3387691/how-to-perfectly-override-a-dict

    This class implements dict of lists semantics. Note that whilst the lists
    stored in a RedisListStore are themselves mutable, stored as Redis lists,
    any objects that they contain are serialised and immutable, though they
    may be overwritten by a new serialised object.
    """
    def __getitem__(self, key):
        k = self.key + ":" + key
        return RedisStore.RedisList(redis=self.redis, key=k)

    def __setitem__(self, key, value):
        k = self.key + ":" + key
        
        if isinstance(value, RedisStore.RedisList) and value.key == k:
            pass  # If setting the same RedisList instance we needn't do anything more.
        elif isinstance(value, Sequence):
            # Delete then store seems the only way to replace entire Redis list.
            self.redis.delete(k)
            if value:
                RedisStore.RedisList(value, redis=self.redis, key=k)
        else:
            raise TypeError("RedisListStore can only store items of type Sequence")


def create_ASL_store(store_url):
    """
    The ASL store is semantically a dict of dicts indexed by by the ARN of the
    ASL State Machine and returning dicts in the format of DescribeStateMachine.
    """
    if store_url.startswith("redis://"):
        # TODO should the LRU cache size be configurable? Probably OK for now.
        return RedisDictStore(store_url, "asl_store", cache_size=1024, daemon=True)
    else:
        return JSONStore(store_url)


def create_executions_store(store_url):
    """
    The executions store is semantically a dict of dicts indexed by by the ARN
    of the execution and returning dicts in the format of DescribeExecution.
    The DISABLE_EXECUTIONS_STORE environment variable is mainly intended to be
    used during performance testing to gauge the performance impact of RedisStore.
    """
    if (store_url.startswith("redis://") and
        os.environ.get("DISABLE_EXECUTIONS_STORE", "false").lower() != "true"):
        return RedisDictStore(store_url, "executions")
    else:
        return SimpleStore()


def create_history_store(store_url):
    """
    The history store is semantically a dict of lists indexed by by the ARN of
    the execution and returning lists in the format of the "events" list from 
    GetExecutionHistory. Each event stored in the lists is an *immutable*
    dict of the form illustrated in the GetExecutionHistory documentation.
    The DISABLE_HISTORY_STORE environment variable is mainly intended to be
    used during performance testing to gauge the performance impact of RedisStore.
    """
    if (store_url.startswith("redis://") and
        os.environ.get("DISABLE_HISTORY_STORE", "false").lower() != "true"):
        return RedisListStore(store_url, "execution_history")
    else:
        return SimpleStore()

