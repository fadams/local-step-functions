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

import hashlib, random, re, uuid

"""
ASL paths use JSONPath.
https://goessner.net/articles/JsonPath/
http://www.ultimate.com/phil/python/#jsonpath
Tested using jsonpath 0.82. Note jsponpath_rw was tried but doesn't seem to
correctly support many of the test cases from the goessner link above.
"""
from jsonpath import jsonpath  # pip3 install jsonpath

from asl_workflow_engine.asl_exceptions import *

try:  # Attempt to use ujson if available https://pypi.org/project/ujson/
    import ujson as json
except:  # Fall back to standard library json
    import json

"""
Attempt to use pybase64 libbase64 based codec if available
pip3 install pybase64
https://github.com/mayeut/pybase64
https://github.com/aklomp/base64
"""
try:
    import pybase64 as base64
except:  # Fall back to standard library base64
    import base64


#def apply_jsonpath(input, path="$", throw_exception_on_failed_match=False):
def apply_jsonpath(input, path="$", throw_exception_on_failed_match=True):
    """
    Performs the InputPath and OutputPath logic described in the ASL spec.
    https://states-language.net/spec.html#filters
    This is mostly just calling jsonpath() and applying the specified defaults.

    The throw_exception_on_failed_match parameter allows callers to select
    whether a failed JSONPath match will throw an exception or just return {}.
    This is because it's a little unclear what the most useful behaviour is
    in this circumstance. The default was originally to return an empty object
    this is because the behaviour of InputPath and OutputPath being null is to
    return an empty JSON object and a null JSONPath result is consistent with
    this. OTOH throwing an exception on a match failure is also useful....
    AWS StepFunctions appears to fail executions in these cases, but it's a
    bit poorly specified and largely depends on the underlying JSONPath engine. 
    What to do in this case isn't obvious from the JSONPath specification either.
    https://goessner.net/articles/JsonPath/
    https://www.tbray.org/ongoing/When/201x/2017/04/14/JsonPath-Needs-Work
    """
    if input == None or path == None:
        return {}
    if path == "$":
        return input
    result = jsonpath(input, path)

    if result == False:
        if throw_exception_on_failed_match:
            raise PathMatchFailure(
                "Invalid path '{}' applied to input '{}'".format(path, input)
            )
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

#def apply_path(input, context, path="$", throw_exception_on_failed_match=False):
def apply_path(input, context, path="$", throw_exception_on_failed_match=True):
    """
    https://states-language.net/spec.html#path

    A Path is a string, beginning with "$", used to identify components with a
    JSON text. The syntax is that of JSONPath.

    When a Path begins with "$$", two dollar signs, this signals that it is
    intended to identify content within the Context Object. The first dollar
    sign is stripped, and the remaining text, which begins with a dollar sign,
    is interpreted as the JSONPath applying to the Context Object.
    """
    if path == None or not isinstance(path, str):
        return {}
    if not path.startswith("$"):
        raise ParameterPathFailure("{} must be a JSONPath".format(path))
    if path.startswith("$$"):  # Use Context object, not input
        path = path[1:]  # Strip leading "$" from context path
        return apply_jsonpath(context, path, throw_exception_on_failed_match)
    else:
        return apply_jsonpath(input, path, throw_exception_on_failed_match)

def get_full_jsonpath(input, path):
    """
    Given an input and a JSONPath query path return the full JSONPath path of
    the query. This can be useful because JSONPath doesn't support getting
    parents of matching nodes
    """
    return jsonpath(input, path, result_type="PATH")

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
            try:  # Test if key is (incorrectly) an int.
                int(key)
                raise ResultPathMatchFailure(
                    "Object index {} is not a valid key string".format(key)
                )
            except ValueError:
                target[key] = update_path(target.get(key, {}), keys, default)
        else:
            raise ResultPathMatchFailure(
                "Cannot use key {} to index a primitive type".format(key)
            )
        return target

    if input == None:
        input = {}
    if path == None:
        return input
    if path == "$":
        return result
    if path.startswith("$$"):
        """
        The value of "ResultPath" MUST NOT begin with "$$"; i.e. it may not be
        used to insert content into the Context Object.
        """
        raise ResultPathMatchFailure(
            "The value of \"ResultPath\" MUST NOT begin with \"$$\""
        )

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

    def evaluate_intrinsic_function(intrinsic):
        """
        ASL Appendix B: List of Intrinsic Functions:
        https://states-language.net/#appendix-b
        """

        def asl_intrinsic_Format(args):
            if len(args) < 1:
                raise IntrinsicFailure(
                    "States.Format failed, requires one or more arguments."
                )
            template_string = args[0]
            args = args[1:]
            try:
                return template_string.format(*args)
            except Exception as e:
                raise IntrinsicFailure(
                    "States.Format failed with {}.".format(e)
                )

        def asl_intrinsic_StringToJson(args):
            if len(args) != 1:
                raise IntrinsicFailure(
                    "States.StringToJson failed, requires a single argument."
                )
            try:
                return json.loads(args[0])
            except Exception as e:
                raise IntrinsicFailure(
                    "States.StringToJson failed with {}.".format(e)
                )

        def asl_intrinsic_JsonToString(args):
            if len(args) != 1:
                raise IntrinsicFailure(
                    "States.JsonToString failed, requires a single argument."
                )
            try:
                return json.dumps(args[0])
            except Exception as e:
                raise IntrinsicFailure(
                    "States.JsonToString failed with {}.".format(e)
                )

        def asl_intrinsic_Array(args):
            return args

        def asl_intrinsic_ArrayPartition(args):
            if len(args) != 2:
                raise IntrinsicFailure(
                    "States.ArrayPartition failed, requires two arguments."
                )
            
            input_array = args[0]
            if not isinstance(input_array, list):
                raise IntrinsicFailure(
                    "States.ArrayPartition failed, arg[0] is not an array."
                )

            n = args[1]
            if not isinstance(n, int) or n <= 0:
                raise IntrinsicFailure(
                    "States.ArrayPartition failed, arg[1] is not a non-zero, positive integer."
                )

            # Partition using list comprehension
            return [input_array[i:i + n] for i in range(0, len(input_array), n)]

        def asl_intrinsic_ArrayContains(args):
            if len(args) != 2:
                raise IntrinsicFailure(
                    "States.ArrayContains failed, requires two arguments."
                )

            input_array = args[0]
            if not isinstance(input_array, list):
                raise IntrinsicFailure(
                    "States.ArrayContains failed, arg[0] is not an array."
                )

            """
            TODO spec says You must specify a valid JSON object as the second
            argument but all examples seem to be int/string need to check
            if AWS supports JSON object/list, if so that's make this much
            more complex and computationally expensive
            """
            return args[1] in input_array

        def asl_intrinsic_ArrayRange(args):
            if len(args) != 3:
                raise IntrinsicFailure(
                    "States.ArrayRange failed, requires three arguments."
                )
            start     = args[0]
            end       = args[1]
            increment = args[2]
            if not (isinstance(start, int) and
                    isinstance(end, int) and
                    isinstance(increment, int)):
                raise IntrinsicFailure(
                    "States.ArrayRange failed, all arguments must be integers."
                )
            if increment == 0:
                raise IntrinsicFailure(
                    "States.ArrayRange failed, args[2] cannot be zero."
                )

            # Create range using list comprehension. Note end + 1 is used as
            # ASL spec specifies inclusive range but Python range is exclusive
            array = [i for i in range(start, end + 1, increment)]

            if len(array) > 1000:
                raise IntrinsicFailure(
                    "States.ArrayRange failed with > 1000 items in range."
                )
            return array

        def asl_intrinsic_ArrayGetItem(args):
            if len(args) != 2:
                raise IntrinsicFailure(
                    "States.ArrayGetItem failed, requires two arguments."
                )

            input_array = args[0]
            if not isinstance(input_array, list):
                raise IntrinsicFailure(
                    "States.ArrayGetItem failed, arg[0] is not an array."
                )

            index = args[1]
            if not isinstance(index, int) or index < 0:
                raise IntrinsicFailure(
                    "States.ArrayGetItem failed, arg[1] is not a positive integer."
                )
            if len(input_array) == 0 or index >= len(input_array):
                raise IntrinsicFailure(
                    "States.ArrayGetItem failed, index is out of bounds."
                )

            return input_array[index]

        def asl_intrinsic_ArrayLength(args):
            if len(args) != 1:
                raise IntrinsicFailure(
                    "States.ArrayLength failed, requires a single argument"
                )

            input_array = args[0]
            if not isinstance(input_array, list):
                raise IntrinsicFailure(
                    "States.ArrayLength failed, arg[0] is not an array."
                )

            return len(input_array)

        def asl_intrinsic_ArrayUnique(args):
            if len(args) != 1:
                raise IntrinsicFailure(
                    "States.ArrayUnique failed, requires a single argument"
                )

            input_array = args[0]
            if not isinstance(input_array, list):
                raise IntrinsicFailure(
                    "States.ArrayUnique failed, arg[0] is not an array."
                )

            # Use set to get unique values from input then use list to convert back
            return list(set(input_array))

        def asl_intrinsic_Base64Encode(args):
            if len(args) != 1:
                raise IntrinsicFailure(
                    "States.Base64Encode failed, requires a single argument"
                )

            if not isinstance(args[0], str):
                raise IntrinsicFailure(
                    "States.Base64Encode failed, arg[0] is not a string."
                )

            try:
                input_bytes = bytes(args[0], "utf-8")  # Get bytes from string
                return base64.b64encode(input_bytes).decode("utf-8")
            except Exception as e:
                raise IntrinsicFailure(
                    "States.Base64Encode failed with {}.".format(e)
                )

        def asl_intrinsic_Base64Decode(args):
            if len(args) != 1:
                raise IntrinsicFailure(
                    "States.Base64Decode failed, requires a single argument"
                )

            if not isinstance(args[0], str):
                raise IntrinsicFailure(
                    "States.Base64Decode failed, arg[0] is not a string."
                )

            try:
                input_bytes = bytes(args[0], "utf-8")  # Get bytes from string
                return base64.b64decode(input_bytes).decode("utf-8")
            except Exception as e:
                raise IntrinsicFailure(
                    "States.Base64Decode failed with {}.".format(e)
                )

        def asl_intrinsic_Hash(args):
            if len(args) != 2:
                raise IntrinsicFailure(
                    "States.Hash failed, requires two arguments."
                )

            data = args[0]
            algorithm = args[1]
            if not isinstance(data, str) or not isinstance(algorithm, str):
                raise IntrinsicFailure(
                    "States.Hash failed, both arguments must be strings."
                )
            
            try:
                input_bytes = bytes(data, "utf-8")  # Get bytes from string
                if algorithm == "MD5":
                    return hashlib.md5(input_bytes).hexdigest()
                elif algorithm == "SHA-1":
                    return hashlib.sha1(input_bytes).hexdigest()
                elif algorithm == "SHA-256":
                    return hashlib.sha256(input_bytes).hexdigest()
                elif algorithm == "SHA-384":
                    return hashlib.sha384(input_bytes).hexdigest()
                elif algorithm == "SHA-512":
                    return hashlib.sha512(input_bytes).hexdigest()
                else:
                    raise Exception("Invalid algorithm {}".format(algorithm))
            except Exception as e:
                raise IntrinsicFailure(
                    "States.Hash failed with {}.".format(e)
                )

        def asl_intrinsic_JsonMerge(args):
            if len(args) != 3:
                raise IntrinsicFailure(
                    "States.JsonMerge failed, requires three arguments"
                )
            if args[2] != False:
                raise IntrinsicFailure(
                    "States.JsonMerge failed, args[2] must be false as Step " +
                    "Functions currently only supports the shallow merging mode."
                )

            try:
                # Use dictionary unpacking operator ** to merge the two dictionaries.
                return {**args[0], **args[1]}
            except Exception as e:
                raise IntrinsicFailure(
                    "States.JsonMerge failed with {}.".format(e)
                )

        def asl_intrinsic_MathRandom(args):
            if len(args) < 2 or len(args) > 3:
                raise IntrinsicFailure(
                    "States.MathRandom failed, requires two or three arguments"
                )
            # The last argument controls the seed value and is optional.
            if len(args) == 3:
                # https://docs.python.org/3/library/random.html#random.seed
                random.seed(args[2])
            if not isinstance(args[0], int) or not isinstance(args[1], int):
                raise IntrinsicFailure(
                    "States.MathRandom failed, args[0] and args[1] must be integers."
                )

            # States.MathRandom has inclusive start and exclusive end number
            # https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-intrinsic-functions.html#asl-intrsc-func-math-operation
            return random.randrange(args[0], args[1])

        def asl_intrinsic_MathAdd(args):
            if len(args) != 2:
                raise IntrinsicFailure(
                    "States.MathAdd failed, requires two arguments."
                )
            if not isinstance(args[0], int) or not isinstance(args[1], int):
                raise IntrinsicFailure(
                    "States.MathAdd failed, both arguments must be integers."
                )

            return args[0] + args[1]

        def asl_intrinsic_StringSplit(args):
            if len(args) != 2:
                raise IntrinsicFailure(
                    "States.StringSplit failed, requires two arguments."
                )
            data = args[0]
            separators = args[1]
            if not isinstance(data, str) or not isinstance(separators, str):
                raise IntrinsicFailure(
                    "States.StringSplit failed, both arguments must be strings."
                )

            # States.StringSplit allows using multiple delimiting characters
            # https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-intrinsic-functions.html#asl-intrsc-func-string-operation
            # so we can't simple use Python's split() and use regex instead.
            try:
                return re.split('['+ separators + ']', data)
            except Exception as e:
                raise IntrinsicFailure(
                    "States.StringSplit failed with {}.".format(e)
                )

        def asl_intrinsic_UUID(args):
            if len(args) != 0:
                raise IntrinsicFailure(
                    "States.UUID failed, this intrinsic takes no arguments."
                )

            return str(uuid.uuid4())
     
        def asl_intrinsic_Default(args):
            raise IntrinsicFailure(
                "Intrinsic Function {} is not supported.".format(func)
            )


        # Extract intrinsic name and normalise it to asl_intrinsic_<name>
        func, args = intrinsic.split("(", 1)
        func = func.strip()
        normalised_func = func.replace("States.", "asl_intrinsic_")
        # Extract raw args string
        args = args.rsplit(")", 1)[0]

        """
        Extract the individual args from the raw string into a list. Intrinsic
        Function arguments may be strings enclosed by apostrophe (') characters,
        numbers, null, Paths, or nested Intrinsic Functions. The regex finds
        each valid argument as follows:
        \'.*?(?<!\\\\)\'        extracts apostrophe delimited string. This uses
            a negative lookbehind to match a closing ' only if not preceeded
            by a \\ in order to support escaped apostrophes in the string.
        States.*?\\)            extracts nested intrinsic

        String and nested intrinsics can contain commas so we explicitly match
        those cases, but the last part of the regex '|[^\\s*,]+' just matches
        anything except whitespace comma. We actually *want* a fairly loose
        match here so if we have an invalid number like f123.45 it would match
        but subsequent evaluation would raise an IntrinsicFailure which we want.
        """
        arglist = re.findall('\'.*?(?<!\\\\)\'|States.*?\\)|[^\\s*,]+', args)

        # Evaluate the arguments
        for i, arg in enumerate(arglist):
            if arg.startswith("'"):  # It's an apostrophe delimited string
                arglist[i] = arg.strip("'")
            elif arg.startswith("$"):  # It's a path
                arglist[i] = apply_path(input, context, arg)
            elif arg.startswith("States."):  # It's a nested intrinsic function
                arglist[i] = evaluate_intrinsic_function(arg)
            elif arg == "null":
                arglist[i] = None
            elif arg == "true":
                """
                Note that the ASL spec doesn't explicitly include booleans in
                the supported Intrinsic Function arguments, however as there
                is a States.Array intrinsic that returns a JSON array containing
                the Values of the arguments, the implication is that arguments
                could be JSON primitives. It's a little unclear.
                """
                arglist[i] = True
            elif arg == "false":
                arglist[i] = False
            else:
                try:
                    arglist[i] = int(arg)
                except ValueError:
                    try:
                        arglist[i] = float(arg)
                    except ValueError:
                        raise IntrinsicFailure(
                            "Intrinsic Function {}, Invalid argument {}.".format(func, arg)
                        )

        """
        We used
        normalised_func = func.replace("States.", "asl_intrinsic_")
        and the "asl_intrinsic_" prefix mitigates the risk of the supplied value
        executing an arbitrary function, so disable semgrep warning.
        """
        # nosemgrep
        return locals().get(
            normalised_func,
            asl_intrinsic_Default,
        )(arglist)

    def evaluate(k, v=None, is_tuple=False):
        """
        Evaluate and expand fields whose name ends with “.$” as described above
        """
        if isinstance(k, str) and k.endswith(".$"):
            k = k[:-2]  # strip ".$" from end
            v_is_path_or_intrinsic = True
        else:
            v_is_path_or_intrinsic = False

        if not is_tuple:
            v = k

        if v_is_path_or_intrinsic:
            if v == "$":  # It's a path representing the root node
                v = clone(input)  # clone to avoid potential circular reference
            elif v.startswith("$"):  # It's a path
                v = apply_path(input, context, v)
            else:  # It's an Intrinsic Function
                v = evaluate_intrinsic_function(v)

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
                    k, v = evaluate(k, v, True)
                    target[k] = v
        return target

    if template == None or template == "":
        return input
    elif template == {}:
        return {}
    else:
        return clone(template)

