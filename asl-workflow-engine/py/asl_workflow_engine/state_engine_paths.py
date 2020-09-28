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
https://states-language.net/spec.html#filters

A state may want to process only a subset of its input data, and may want that
data structured differently from the way it appears in the input. Similarly, it
may want to control the format and content of the data that it passes on as
output.

Fields named “InputPath”, “Parameters”, “OutputPath”, and “ResultPath” exist to
support this. Any state except for a Fail State MAY have “InputPath” and
“OutputPath”. States which potentially generate results MAY have “ResultPath”
and “Parameters”: Pass State, Task State, and Parallel State.
"""

import sys
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3

import re

"""
ASL paths use JSONPath
https://goessner.net/articles/JsonPath/
http://www.ultimate.com/phil/python/#jsonpath
Tested using jsonpath 0.82. Note jsponpath_rw was tried but doesn't seem to
correctly support many of the test cases from the goessner link above
"""
from jsonpath import jsonpath  # sudo pip3 install jsonpath

from asl_workflow_engine.asl_exceptions import *


def apply_jsonpath(input, path="$", return_false_on_failed_match=False):
    """
    Performs the InputPath and OutputPath logic described in the ASL spec.
    https://states-language.net/spec.html#filters
    This is mostly just calling jsonpath() and applying the specified defaults.

    The return_false_on_failed_match parameter allows callers to select whether
    a failed JSONPath match will return boolean False or an empty object.
    This is because it's a little unclear what the best/most useful behaviour
    is in this circumstance. The default is currently to return an empty object
    this is because the behaviour of InputPath and OutputPath being null is to
    return an empty JSON object and a null JSONPath result is somewhat
    consistent with this. OTOH deliberately returning False on match failure
    is also useful especially in the Choice state Variable field as that allows
    us to match a BooleanEquals False Choice for the case of no JSONPath match.
    TODO need to check what AWS StepFunctions actually do with these cases
    'cause it's a bit poorly specified and I suspect that it also largely
    depends on the underlying JSONPath engine - and what to do in this case
    doesn't seem obvious from the JSONPath specification either.
    """
    if input == None or path == None:
        return {}
    if path == "$":
        return input
    result = jsonpath(input, path)

    if result == False:
        if return_false_on_failed_match:
            return False
        else:
            return {}

    """
    The following is a little subtle. Unfortunately the JSONPath specification
    is vague on a few points and some implementations, such as Python jsonpath,
    return a list of matches, but for most scenarios if a single item matches
    it is more intuitive to have that item returned rather than a list that
    contains that item. Other implementations such as the Java Jayway (which
    I think is the one used in AWS Step Functions) behave in that way. An
    exception is where the path contains an array slice operator because then
    we intuitively expect to return an array/list even if only a single item
    is matched. The code below attempts to handle that array slice edge case.
    """
    if len(result) == 1:
        path_has_slice = re.search(r"\[.*:.*\]", path)
        if not path_has_slice:
            return result[0]
    return result

def apply_path(input, context, path="$", return_false_on_failed_match=False):
    """
    https://states-language.net/spec.html#path

    A Path is a string, beginning with "$", used to identify components with a
    JSON text. The syntax is that of JSONPath.

    When a Path begins with "$$", two dollar signs, this signals that it is
    intended to identify content within the Context Object. The first dollar
    sign is stripped, and the remaining text, which begins with a dollar sign,
    is interpreted as the JSONPath applying to the Context Object.
    """
    if not path.startswith("$"):
        raise ParameterPathFailure("{} must be a JSONPath".format(path))
    if path.startswith("$$"):  # Use Context object, not input
        path = path[1:]  # Strip leading "$" from context path
        return apply_jsonpath(context, path, return_false_on_failed_match)
    else:
        return apply_jsonpath(input, path, return_false_on_failed_match)

def get_full_jsonpath(input, path):
    """
    Given an input and a JSONPath query path return the full JSONPath path of
    the query. This can be useful because JSONPath doesn't support getting
    parents of matching nodes
    """
    result = jsonpath(input, path, result_type="PATH")
    """
    #if result and len(result) == 1:
    TODO if len(result) > 1 then it means multiple states with the same name,
    which is an invalid state machine. Need to work out the best place to check
    for that situation, which is probably not here. For now always return the
    first matching result.
    """
    if result:
        return result[0]
    else:
        return ""

def apply_resultpath(input, result, path="$"):
    """
    Performs the ResultPath logic described in the ASL spec.
    https://states-language.net/spec.html#filters

    The value of “ResultPath” MUST be a Reference Path, which specifies the raw
    input’s combination with or replacement by the state’s result.

    The ResultPath field’s value is a Reference Path that specifies where to
    place the result, relative to the raw input. If the input has a field which
    matches the ResultPath value, then in the output, that field is discarded
    and overwritten by the state output. Otherwise, a new field is created in
    the state output.

    If the value of of ResultPath is null, that means that the state’s own raw
    output is discarded and its raw input becomes its result.
    """
    def update_path(target, keys, default):
        if len(keys) == 0:
            return default
        key = keys.pop(0)
        if isinstance(target, list):
            try:
                i = int(key)
                target[i] = update_path(target[i], keys, default)
            except (ValueError, IndexError) as e:
                raise ResultPathMatchFailure(e)
        elif isinstance(target, dict):
            try:
                int(key)
                raise ResultPathMatchFailure(
                    "object index {} is not a valid key string".format(key)
                )
            except ValueError:
                target[key] = update_path(target.get(key, {}), keys, default)
        else:
            raise ResultPathMatchFailure(
                "cannot use key {} to index a primitive type".format(key)
            )
        return target

    if input == None:
        input = {}
    if path == None:
        return input
    if path == "$":
        return result

    matches = re.findall(r"[^$.[\]]+", path)  # Regex to split the reference paths
    return update_path(input, matches, result)

def evaluate_payload_template(input, context, template):
    """
    https://states-language.net/spec.html#payload-template

    A state machine interpreter dispatches data as input to tasks to do useful
    work, and receives output back from them. It is frequently desired to
    reshape input data to meet the format expectations of tasks, and similarly
    to reshape the output coming back. A JSON object structure called a Payload
    Template is provided for this purpose.

    The value of "Parameters" MUST be a Payload Template which is a JSON object,
    whose input is the result of applying the InputPath to the raw input. If the
    "Parameters" field is provided, its payload, after the extraction and
    embedding, becomes the effective input.

    The value of "ResultSelector" MUST be a Payload Template, whose input is the
    result, and whose payload replaces and becomes the effective result.

    Values from the Payload Template’s input and the Context Object can be
    inserted into the payload with a combination of a field-naming convention,
    Paths and Intrinsic Functions.

    If any field within the Payload Template (however deeply nested) has a name
    ending with the characters ".$", its value is transformed and the field is
    renamed to strip the ".$" suffix.

    If the field value begins with only one "$", the value MUST be a Path. In
    this case, the Path is applied to the Payload Template’s input and is the
    new field value.

    If the field value begins with "$$", the first dollar sign is stripped and
    the remainder MUST be a Path. In this case, the Path is applied to the
    Context Object and is the new field value.

    If the field value does not begin with "$", it MUST be an Intrinsic Function.
    The interpreter invokes the Intrinsic Function and the result is the new value.

    If the path is legal but cannot be applied successfully, the interpreter
    fails the machine execution with an Error Name of "States.ParameterPathFailure".
    If the Intrinsic Function fails during evaluation, the interpreter fails the
    machine execution with an Error Name of "States.IntrinsicFailure".

    When a field name ends with “.$” and its value can be used to generate an
    Extracted Value as described above, the field is replaced within the
    Parameters value by another field whose name is the original name minus the
    “.$” suffix, and whose value is the Extracted Value.

    This implementation extends the ASL specification a little as it also
    supports replacement of JSON array values such that for [0, 1, "$.map.c.$"]
    the third item would be replaced by the result of applying the JSONpath of
    $.map.c to the input.
    """

    def evaluate(k, v=None):
        """
        Evaluate and expand fields whose name ends with “.$” as described above
        """
        if isinstance(k, str) and k.endswith(".$"):
            k = k[:-2]  # strip ".$" from end
            v_is_path = True
        else:
            v_is_path = False

        if v:
            is_tuple = True
        else:
            v = k
            is_tuple = False

        if v_is_path:
            v = apply_path(input, context, v)

        if is_tuple:
            return k, v
        else:
            return v

    def clone(template):
        """
        Recursively crawl the source JSON template creating a clone of its
        structure but evaluating and expanding fields whose name ends with “.$”
        """
        if isinstance(template, list):
            target = []
            for item in template:
                if isinstance(item, (dict, list)):
                    target.append(clone(item))
                else:
                    target.append(evaluate(item))
        elif isinstance(template, dict):
            target = {}
            for k, v in template.items():
                if isinstance(v, (dict, list)):
                    target[k] = clone(v)
                else:
                    k, v = evaluate(k, v)
                    target[k] = v
        return target

    if not template:
        return input
    return clone(template)

