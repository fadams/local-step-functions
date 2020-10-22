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
Defines the exceptions relating to ASL itself as defined in
https://states-language.net/spec.html.
"""

import sys
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3


class Timeout(Exception):
    pass


class TaskFailed(Exception):
    pass


class Permissions(Exception):
    pass


class ResultPathMatchFailure(Exception):
    pass


class ParameterPathFailure(Exception):
    pass


class IntrinsicFailure(Exception):
    pass


class BranchFailed(Exception):
    pass


class NoChoiceMatched(Exception):
    pass


# Not defined in the ASL spec but used in the Choice state path handling.
class PathMatchFailure(Exception):
    pass
