#!/usr/bin/env python3
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
This module creates a J2119 validator for the StateMachine J2119 schema and
uses that to validate the supplied input, it then performs some ASL specific
semantic checks that aren't expressible in a J2119 schema.


Original Ruby version
https://github.com/awslabs/statelint
https://github.com/awslabs/statelint/tree/master/lib

Uses j2119 parser/validator
https://github.com/awslabs/j2119
https://github.com/awslabs/j2119/tree/master/lib
"""

import sys, os
assert sys.version_info >= (3, 6)  # Bomb out if not running Python3.6

import re
from statelint.j2119 import Validator
from statelint.j2119 import JSONPathChecker

class StateLint():
    def __init__(self):
        # Find StateMachine.j2119 schema file on the path
        schema_paths = [i + "/statelint/StateMachine.j2119" for i in sys.path]
        schema_path = ""
        for path in schema_paths:
            if os.path.exists(path):
                schema_path = path
                break

        if not schema_path:
            raise FileNotFoundError(
                "Unable to find StateMachine.j2119 schema file on path"
            )

        #print(schema_path)
        self.validator = Validator(schema_path)

    def validate(self, json):
        problems = self.validator.validate(json)
        checker = StateNode()
        checker.check(json, self.validator.root, problems)
        return problems

#-------------------------------------------------------------------------------

class StateNode():
    """
    Based on https://github.com/awslabs/statelint/blob/master/lib/statelint/state_node.rb
    Handles semantic validation that can't be expressed in a J2119 schema.
    """
    def __init__(self):
        """
        We push States nodes on here when we traverse them then, whenever
        we find a "Next", "Default" or "StartAt" node, we validate that the
        target node exists, and record that the target has an incoming pointer.
        """
        self.current_states_node = []
        self.current_states_incoming = []

        # We keep track of all the state names and complain about dupes
        self.all_state_names = {}

        self.intrinsic_invocation_regex = re.compile('^States\.(Format|Array|ArrayPartition|ArrayContains|ArrayRange|ArrayGetItem|ArrayLength|ArrayUnique|Base64Encode|Base64Decode|Hash|JsonMerge|JsonToString|StringToJson|MathRandom|MathAdd|StringSplit)\\s*\(.+\)$')
        self.intrinsic_uuid_invocation_regex = re.compile('^States\.UUID\\s*\(\\s*\)$')

    def check(self, node, path, problems):
        if not node or not isinstance(node, dict):
            return

        is_machine_top = "States" in node and isinstance(node["States"], dict)
        if is_machine_top:
            states = node["States"]
            self.current_states_node.append(states)
            start_at = node.get("StartAt")
            if start_at and isinstance(start_at, str):
                self.current_states_incoming.append([start_at])
                if start_at not in states:
                    problems.append(
                        f'StartAt value "{start_at}" not found in ' +
                        f'States field at {path}'
                    )
            else:
                self.current_states_incoming.append([])

            for name, child in states.items():
                if isinstance(child, dict):
                    child_path = path + ".States." + name
                    for field_name in ["Parameters", "ItemSelector", "ResultSelector"]:
                        if field_name in child:
                            self.probe_payload_template(
                                child[field_name],
                                child_path,
                                problems,
                                field_name
                            )

                    if child.get("Type") == "Choice":
                        choices = child.get("Choices")
                        if choices:
                            self.probe_choice_state(
                                choices,
                                child_path + ".Choices",
                                problems
                            )

                if name in self.all_state_names:
                    problems.append(
                        f'State "{name}", defined at {path}.States, ' +
                        f'is also defined at {self.all_state_names[name]}'
                    )
                else:
                    self.all_state_names[name] = f"{path}.States"

        self.check_for_terminal(node, path, problems)
        self.check_next(node, path, problems)
        self.check_States_ALL(node.get("Retry"), path + '.Retry', problems)
        self.check_States_ALL(node.get("Catch"), path + '.Catch', problems)

        for name, val in node.items():
            if isinstance(val, list):
                for i, element in enumerate(val):
                    self.check(element, f"{path}.{name}[{i}]", problems)
            else:
                self.check(val, f"{path}.{name}", problems)

        if is_machine_top:
            states = self.current_states_node.pop()
            incoming = self.current_states_incoming.pop()
            missing = list(set(states.keys()) - set(incoming))
            for state in missing:
                problems.append(f'No transition found to state {path}.States.{state}')


    def check_next(self, node, path, problems):
        self.add_next(node, path, "Next", problems)
        self.add_next(node, path, "Default", problems)

    def add_next(self, node, path, field, problems):
        transition_to = node.get(field)
        if transition_to and isinstance(transition_to, str):
            if len(self.current_states_node) > 0:
                if transition_to in self.current_states_node[-1]:
                    self.current_states_incoming[-1].append(transition_to)
                else:
                    problems.append(
                        f'No state found named "{transition_to}", ' +
                        f'referenced at {path}.{field}'
                    )


    def probe_choice_state(self, node, path, problems):
        if isinstance(node, dict):
            variable = node.get("Variable")
            if variable and not JSONPathChecker().is_path(variable):
                problems.append(
                    f'Field "Variable" of Choice state choice at "{path}" ' +
                    f'is not a JSONPath'
                )

            for op in ["And", "Or", "Not"]:
                if op in node:
                    self.probe_choice_state(node[op], path + "." + op, problems)

        elif isinstance(node, list):
            for i, element in enumerate(node):
                self.probe_choice_state(element, f"{path}[{i}]", problems)

    def probe_payload_template(self, node, path, problems, field_name):
        # Search through Payload Templates for object nodes and check field semantics
        if isinstance(node, dict):
            for name, val in node.items():
                if name.endswith(".$"):
                    if (not self.is_intrinsic_invocation(val) and
                        not JSONPathChecker().is_path(val)):
                        problems.append(
                            f'Field "{name}" of {field_name} at "{path}" is ' +
                            f'not a JSONPath nor intrinsic function expression'
                        )
                else:
                    self.probe_payload_template(val, f"{path}.{name}", problems, field_name)
        elif isinstance(node, list):
            for i, element in enumerate(node):
                self.probe_payload_template(element, f"{path}[{i}]", problems, field_name)

    def is_intrinsic_invocation(self, val):
        return isinstance(val, str) and (self.intrinsic_invocation_regex.match(val) or 
                                         self.intrinsic_uuid_invocation_regex.match(val))

    def check_for_terminal(self, node, path, problems):
        states = node.get("States")
        if states and isinstance(states, dict):
            terminal_found = False
            for state_node in states.values():
                if isinstance(state_node, dict):
                    if state_node.get("Type", "") in ["Succeed", "Fail"]:
                        terminal_found = True
                    elif state_node.get("End", False) == True:
                        terminal_found = True

            if not terminal_found:
                problems.append(
                    f'No terminal state found in machine at {path}.States'
                )

    def check_States_ALL(self, node, path, problems):
        if not isinstance(node, list):
            return

        for i, element in enumerate(node):
            if isinstance(element, dict):
                ee = element.get("ErrorEquals")
                if ee and isinstance(ee, list):
                    if "States.ALL" in ee:
                        if i != len(node) -1 or len(ee) != 1:
                            problems.append(
                                f'{path}[{i}]: States.ALL can only appear ' +
                                f'in the last element, and by itself'
                            )

