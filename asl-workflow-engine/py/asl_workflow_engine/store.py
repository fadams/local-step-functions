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

import json, os, uuid
from collections.abc import MutableMapping

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





def create_ASL_store(store_url):
    """
    The ASL store is semantically a dict of dicts indexed by by the ARN of the
    ASL State Machine and returning dicts in the format of DescribeStateMachine.
    """
    if store_url.startswith("redis://"):
        print("Create RedisStore")
        return {}
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
        print("Create RedisStore")
        return {}
    else:
        return {}


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
        print("Create RedisStore")
        return {}
    else:
        return {}

