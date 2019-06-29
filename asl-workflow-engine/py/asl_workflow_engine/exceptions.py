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
Defines the exceptions raised by asl_workflow_engine.
TODO the usefulness and granularity of these exceptions will need to evolve
as it's difficult to foresee all of the failure scenarios until we've gone
through a few iterations.
"""

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3


class MessagingError(Exception):
    pass

class ConnectionError(MessagingError):
    pass

class SessionError(MessagingError):
    pass

class ProducerError(MessagingError):
    pass

class ConsumerError(MessagingError):
    pass

