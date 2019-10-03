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
AWS resources are identified by Amazon Resource Names (ARNs) specified here:
http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html
This package comprises simple utility functions for creating and parsing ARNs.
"""

import sys
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3


def create_arn(
    resource="",
    arn="arn",
    partition="aws",
    service="",
    region="",
    account="",
    resource_type=None,
):
    """
    Create an ARN string from its component parts, if this function is called
    passing a dictionary we use the ** operator to perform keyword expansion.
    """
    if isinstance(resource, dict):
        return create_arn(**resource)
    if resource_type:
        resource = resource_type + ":" + resource
    return "{}:{}:{}:{}:{}:{}".format(
        arn, partition, service, region, account, resource
    )

def parse_arn(arn):
    """
    Parse an ARN into a dictionary comprising the component parts of the ARN
    """
    elements = arn.split(":", 5)
    result = {
        "arn": elements[0],
        "partition": elements[1],
        "service": elements[2],
        "region": elements[3],
        "account": elements[4],
        "resource": elements[5],
        "resource_type": None,
    }
    if "/" in result["resource"]:
        result["resource_type"], result["resource"] = result["resource"].split("/", 1)
    elif ':' in result['resource']:
        result["resource_type"], result["resource"] = result["resource"].split(":", 1)
    return result
