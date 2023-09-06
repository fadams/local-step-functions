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
This does the main parsing of IETF RFC 2119 style assertions used as input
https://www.ietf.org/rfc/rfc2119.txt
"""

import sys, os
assert sys.version_info >= (3, 6)  # Bomb out if not running Python3.6

import re
from datetime import datetime, timezone, timedelta

class Validator():
    # Based on https://github.com/awslabs/j2119/blob/master/lib/j2119.rb
    def __init__(self, assertions_source):
        try:
            with open(assertions_source, "r") as f:
                self.parser = Parser(f)
                self.root = self.parser.root

        except IOError as e:
            raise Exception(f"Unable to read file: {e}")

    def validate(self, json):
        problems = []
        validator = NodeValidator(self.parser)
        validator.validate_node(json, self.parser.root, [self.parser.root], problems)
        return problems

    # For call to str().
    def __str__(self):
        return f"J2119 validator for instances of {self.root}"

#-------------------------------------------------------------------------------

class Parser():
    # Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/parser.rb
    def __init__(self, j2119_file):
        ROOT = re.compile('This\s+document\s+specifies\s+a\s+JSON\s+object\s+called\s+an?\s+"([^"]+)"\.')
        self.root = None
        self.constraints = RoleConstraints()
        self.finder = RoleFinder()
        self.allowed_fields = AllowedFields()

        lines = j2119_file.readlines()
        for line in lines:
            root_match = ROOT.match(line)
            if root_match:
                if self.root:
                    raise Exception("Only one root declaration permitted")
                else:
                    self.root = root_match.group(1)
                    self.matcher = Matcher(self.root)
                    self.assigner = Assigner(self.constraints,
                                             self.finder,
                                             self.matcher,
                                             self.allowed_fields)
            else:
                if not self.root:
                    raise Exception("Root declaration must come first")
                else:
                    self.proc_line(line)

    def proc_line(self, line):
        #print(line)
        try:
            if self.matcher.is_constraint_line(line):
                self.assigner.assign_constraints(self.matcher.build_constraint(line))
            elif self.matcher.is_only_one_match_line(line):
                self.assigner.assign_only_one_of(self.matcher.build_only_one(line))
            elif self.matcher.is_each_of_line(line):
                eaches, trailer = self.matcher.build_each_ofs(line)
                for each in eaches:
                    self.proc_line(f"A {each} {trailer}")
            elif self.matcher.is_role_def_line(line):
                self.assigner.assign_roles(self.matcher.build_role_def(line))
            else:
                raise Exception(f"Unprocessable line: '{line}'")

        except Exception as e:
            raise Exception(f"Unprocessable line: '{line}': {e}")

    def find_more_roles(self, node, roles):
        return self.finder.find_more_roles(node, roles)

    def find_grandchild_roles(self, roles, name):
        return self.finder.find_grandchild_roles(roles, name)

    def find_child_roles(self, roles, name):
        return self.finder.find_child_roles(roles, name)

    def get_constraints(self, role):
        return self.constraints.get_constraints(role)

    def is_field_allowed(self, roles, child):
        return self.allowed_fields.is_allowed(roles, child)

    def allows_any_field(self, roles):
        return self.allowed_fields.allows_any(roles)

#-------------------------------------------------------------------------------

class NodeValidator():
    # Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/node_validator.rb
    def __init__(self, parser):
        self.parser = parser

    def validate_node(self, node, path, roles, problems):
        #print()
        #print(f"validate_node {node} {path} {roles} {problems}")

        if not node or not isinstance(node, dict):
            return

        # May have more roles based on field presence/value etc
        self.parser.find_more_roles(node, roles)

        #print(f"roles: {roles}")

        # constraints are attached per-role
        # TODO - look through the constraints and if there is a
        #  "Field should not exist" constraint, then disable
        #  type and value checking constraints
        for role in roles:
            #print(f"role: {role}")
            for constraint in self.parser.get_constraints(role):
                #print(f"{constraint} {constraint.conditions}")
                if constraint.applies(node, roles):
                    constraint.check(node, path, problems)

        # For each field
        for name, val in node.items():
            #print(f"{name} {val}")
            if not self.parser.is_field_allowed(roles, name):
                problems.append(f'Field "{name}" not allowed in {path}')

            # Only recurse into children if they have roles
            child_roles = self.parser.find_child_roles(roles, name)
            if len(child_roles):
                self.validate_node(val, f"{path}.{name}", child_roles, problems)

            # find inheritance-based roles for that field
            grandchild_roles = self.parser.find_grandchild_roles(roles, name)
            if len(grandchild_roles) and not self.parser.allows_any_field(grandchild_roles):
                # Recurse into grandchildren
                if isinstance(val, dict):
                    for child_name, child_val in val.items():
                        self.validate_node(
                            child_val,
                            f"{path}.{name}.{child_name}",
                            grandchild_roles[:],  # Need to pass a *cloned* copy 
                            problems
                        )
                elif isinstance(val, list):
                    for i, member in enumerate(val):
                        self.validate_node(
                            member,
                            f"{path}.{name}[{i}]",
                            grandchild_roles[:],  # Need to pass a *cloned* copy
                            problems
                        )

#-------------------------------------------------------------------------------

def deduce(value):
    # Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/deduce.rb
    sm = re.match('^"(.*)"$', value)
    if sm:
        return sm[1]
    if value == "true":
        return True
    if value == "false":
        return False
    if value == "null":
        return None
    if re.match('^\d+$', value):
        return int(value)
    return float(value)

#-------------------------------------------------------------------------------

class RoleConstraints():
    # Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/role_constraints.rb
    def __init__(self):
        self.constraints = {}

    def add(self, role, constraint):
        self.constraints.setdefault(role, []).append(constraint)

    def get_constraints(self, role):
        return self.constraints.get(role, [])

#-------------------------------------------------------------------------------

class JSONPathChecker():
    """
    Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/json_path_checker.rb
    Examines fields which are supposed to be JSONPath expressions or
    Reference Paths, which are JSONPaths that are singular, i.e. don't
    produce array results.

    Note that the original Ruby code failed to match on a number of useful
    valid things like hyphens in identifiers e.g. $.error-info would fail,
    so the regexes have been updated below to add common "punctuation" like
    hyphens, underscores etc. and allow them to be used anywhere in names.
    There is a very fine balance between being strict enough to usefully flag
    real errors whilst being loose enough to allow valid paths without erroring.

    If it becomes too much of a pain it may be simpler to just have is_path
    and is_reference_path just return isinstance(s, str)
    """
    def __init__(self):
        """
        The original Ruby code uses Unicode Categories e.g. \p{Lu}, \p{Ll} etc.
        https://github.com/awslabs/j2119/blob/master/lib/j2119/json_path_checker.rb
        https://www.regular-expressions.info/unicode.html
        Unfortunately Python re doesn't support \p so use explicit matches
        taken from https://difnet.com.br/opensource/unicode_hack.py.txt
        so we can use the standard library re and avoid additional dependencies
        """
        Lu = '\u0041-\u005a\u00c0-\u00d6\u00d8-\u00de\u0100\u0102\u0104\u0106\u0108\u010a\u010c\u010e\u0110\u0112\u0114\u0116\u0118\u011a\u011c\u011e\u0120\u0122\u0124\u0126\u0128\u012a\u012c\u012e\u0130\u0132\u0134\u0136\u0139\u013b\u013d\u013f\u0141\u0143\u0145\u0147\u014a\u014c\u014e\u0150\u0152\u0154\u0156\u0158\u015a\u015c\u015e\u0160\u0162\u0164\u0166\u0168\u016a\u016c\u016e\u0170\u0172\u0174\u0176\u0178\u0179\u017b\u017d\u0181\u0182\u0184\u0186\u0187\u0189-\u018b\u018e-\u0191\u0193\u0194\u0196-\u0198\u019c\u019d\u019f\u01a0\u01a2\u01a4\u01a6\u01a7\u01a9\u01ac\u01ae\u01af\u01b1-\u01b3\u01b5\u01b7\u01b8\u01bc\u01c4\u01c7\u01ca\u01cd\u01cf\u01d1\u01d3\u01d5\u01d7\u01d9\u01db\u01de\u01e0\u01e2\u01e4\u01e6\u01e8\u01ea\u01ec\u01ee\u01f1\u01f4\u01f6-\u01f8\u01fa\u01fc\u01fe\u0200\u0202\u0204\u0206\u0208\u020a\u020c\u020e\u0210\u0212\u0214\u0216\u0218\u021a\u021c\u021e\u0220\u0222\u0224\u0226\u0228\u022a\u022c\u022e\u0230\u0232\u023a\u023b\u023d\u023e\u0241\u0243-\u0246\u0248\u024a\u024c\u024e\u0386\u0388-\u038a\u038c\u038e\u038f\u0391-\u03a1\u03a3-\u03ab\u03d2-\u03d4\u03d8\u03da\u03dc\u03de\u03e0\u03e2\u03e4\u03e6\u03e8\u03ea\u03ec\u03ee\u03f4\u03f7\u03f9\u03fa\u03fd-\u042f\u0460\u0462\u0464\u0466\u0468\u046a\u046c\u046e\u0470\u0472\u0474\u0476\u0478\u047a\u047c\u047e\u0480\u048a\u048c\u048e\u0490\u0492\u0494\u0496\u0498\u049a\u049c\u049e\u04a0\u04a2\u04a4\u04a6\u04a8\u04aa\u04ac\u04ae\u04b0\u04b2\u04b4\u04b6\u04b8\u04ba\u04bc\u04be\u04c0\u04c1\u04c3\u04c5\u04c7\u04c9\u04cb\u04cd\u04d0\u04d2\u04d4\u04d6\u04d8\u04da\u04dc\u04de\u04e0\u04e2\u04e4\u04e6\u04e8\u04ea\u04ec\u04ee\u04f0\u04f2\u04f4\u04f6\u04f8\u04fa\u04fc\u04fe\u0500\u0502\u0504\u0506\u0508\u050a\u050c\u050e\u0510\u0512\u0531-\u0556\u10a0-\u10c5\u1e00\u1e02\u1e04\u1e06\u1e08\u1e0a\u1e0c\u1e0e\u1e10\u1e12\u1e14\u1e16\u1e18\u1e1a\u1e1c\u1e1e\u1e20\u1e22\u1e24\u1e26\u1e28\u1e2a\u1e2c\u1e2e\u1e30\u1e32\u1e34\u1e36\u1e38\u1e3a\u1e3c\u1e3e\u1e40\u1e42\u1e44\u1e46\u1e48\u1e4a\u1e4c\u1e4e\u1e50\u1e52\u1e54\u1e56\u1e58\u1e5a\u1e5c\u1e5e\u1e60\u1e62\u1e64\u1e66\u1e68\u1e6a\u1e6c\u1e6e\u1e70\u1e72\u1e74\u1e76\u1e78\u1e7a\u1e7c\u1e7e\u1e80\u1e82\u1e84\u1e86\u1e88\u1e8a\u1e8c\u1e8e\u1e90\u1e92\u1e94\u1ea0\u1ea2\u1ea4\u1ea6\u1ea8\u1eaa\u1eac\u1eae\u1eb0\u1eb2\u1eb4\u1eb6\u1eb8\u1eba\u1ebc\u1ebe\u1ec0\u1ec2\u1ec4\u1ec6\u1ec8\u1eca\u1ecc\u1ece\u1ed0\u1ed2\u1ed4\u1ed6\u1ed8\u1eda\u1edc\u1ede\u1ee0\u1ee2\u1ee4\u1ee6\u1ee8\u1eea\u1eec\u1eee\u1ef0\u1ef2\u1ef4\u1ef6\u1ef8\u1f08-\u1f0f\u1f18-\u1f1d\u1f28-\u1f2f\u1f38-\u1f3f\u1f48-\u1f4d\u1f59\u1f5b\u1f5d\u1f5f\u1f68-\u1f6f\u1fb8-\u1fbb\u1fc8-\u1fcb\u1fd8-\u1fdb\u1fe8-\u1fec\u1ff8-\u1ffb\u2102\u2107\u210b-\u210d\u2110-\u2112\u2115\u2119-\u211d\u2124\u2126\u2128\u212a-\u212d\u2130-\u2133\u213e\u213f\u2145\u2183\u2c00-\u2c2e\u2c60\u2c62-\u2c64\u2c67\u2c69\u2c6b\u2c75\u2c80\u2c82\u2c84\u2c86\u2c88\u2c8a\u2c8c\u2c8e\u2c90\u2c92\u2c94\u2c96\u2c98\u2c9a\u2c9c\u2c9e\u2ca0\u2ca2\u2ca4\u2ca6\u2ca8\u2caa\u2cac\u2cae\u2cb0\u2cb2\u2cb4\u2cb6\u2cb8\u2cba\u2cbc\u2cbe\u2cc0\u2cc2\u2cc4\u2cc6\u2cc8\u2cca\u2ccc\u2cce\u2cd0\u2cd2\u2cd4\u2cd6\u2cd8\u2cda\u2cdc\u2cde\u2ce0\u2ce2\uff21-\uff3a'

        Ll = '\u0061-\u007a\u00aa\u00b5\u00ba\u00df-\u00f6\u00f8-\u00ff\u0101\u0103\u0105\u0107\u0109\u010b\u010d\u010f\u0111\u0113\u0115\u0117\u0119\u011b\u011d\u011f\u0121\u0123\u0125\u0127\u0129\u012b\u012d\u012f\u0131\u0133\u0135\u0137\u0138\u013a\u013c\u013e\u0140\u0142\u0144\u0146\u0148\u0149\u014b\u014d\u014f\u0151\u0153\u0155\u0157\u0159\u015b\u015d\u015f\u0161\u0163\u0165\u0167\u0169\u016b\u016d\u016f\u0171\u0173\u0175\u0177\u017a\u017c\u017e-\u0180\u0183\u0185\u0188\u018c\u018d\u0192\u0195\u0199-\u019b\u019e\u01a1\u01a3\u01a5\u01a8\u01aa\u01ab\u01ad\u01b0\u01b4\u01b6\u01b9\u01ba\u01bd-\u01bf\u01c6\u01c9\u01cc\u01ce\u01d0\u01d2\u01d4\u01d6\u01d8\u01da\u01dc\u01dd\u01df\u01e1\u01e3\u01e5\u01e7\u01e9\u01eb\u01ed\u01ef\u01f0\u01f3\u01f5\u01f9\u01fb\u01fd\u01ff\u0201\u0203\u0205\u0207\u0209\u020b\u020d\u020f\u0211\u0213\u0215\u0217\u0219\u021b\u021d\u021f\u0221\u0223\u0225\u0227\u0229\u022b\u022d\u022f\u0231\u0233-\u0239\u023c\u023f\u0240\u0242\u0247\u0249\u024b\u024d\u024f-\u0293\u0295-\u02af\u037b-\u037d\u0390\u03ac-\u03ce\u03d0\u03d1\u03d5-\u03d7\u03d9\u03db\u03dd\u03df\u03e1\u03e3\u03e5\u03e7\u03e9\u03eb\u03ed\u03ef-\u03f3\u03f5\u03f8\u03fb\u03fc\u0430-\u045f\u0461\u0463\u0465\u0467\u0469\u046b\u046d\u046f\u0471\u0473\u0475\u0477\u0479\u047b\u047d\u047f\u0481\u048b\u048d\u048f\u0491\u0493\u0495\u0497\u0499\u049b\u049d\u049f\u04a1\u04a3\u04a5\u04a7\u04a9\u04ab\u04ad\u04af\u04b1\u04b3\u04b5\u04b7\u04b9\u04bb\u04bd\u04bf\u04c2\u04c4\u04c6\u04c8\u04ca\u04cc\u04ce\u04cf\u04d1\u04d3\u04d5\u04d7\u04d9\u04db\u04dd\u04df\u04e1\u04e3\u04e5\u04e7\u04e9\u04eb\u04ed\u04ef\u04f1\u04f3\u04f5\u04f7\u04f9\u04fb\u04fd\u04ff\u0501\u0503\u0505\u0507\u0509\u050b\u050d\u050f\u0511\u0513\u0561-\u0587\u1d00-\u1d2b\u1d62-\u1d77\u1d79-\u1d9a\u1e01\u1e03\u1e05\u1e07\u1e09\u1e0b\u1e0d\u1e0f\u1e11\u1e13\u1e15\u1e17\u1e19\u1e1b\u1e1d\u1e1f\u1e21\u1e23\u1e25\u1e27\u1e29\u1e2b\u1e2d\u1e2f\u1e31\u1e33\u1e35\u1e37\u1e39\u1e3b\u1e3d\u1e3f\u1e41\u1e43\u1e45\u1e47\u1e49\u1e4b\u1e4d\u1e4f\u1e51\u1e53\u1e55\u1e57\u1e59\u1e5b\u1e5d\u1e5f\u1e61\u1e63\u1e65\u1e67\u1e69\u1e6b\u1e6d\u1e6f\u1e71\u1e73\u1e75\u1e77\u1e79\u1e7b\u1e7d\u1e7f\u1e81\u1e83\u1e85\u1e87\u1e89\u1e8b\u1e8d\u1e8f\u1e91\u1e93\u1e95-\u1e9b\u1ea1\u1ea3\u1ea5\u1ea7\u1ea9\u1eab\u1ead\u1eaf\u1eb1\u1eb3\u1eb5\u1eb7\u1eb9\u1ebb\u1ebd\u1ebf\u1ec1\u1ec3\u1ec5\u1ec7\u1ec9\u1ecb\u1ecd\u1ecf\u1ed1\u1ed3\u1ed5\u1ed7\u1ed9\u1edb\u1edd\u1edf\u1ee1\u1ee3\u1ee5\u1ee7\u1ee9\u1eeb\u1eed\u1eef\u1ef1\u1ef3\u1ef5\u1ef7\u1ef9\u1f00-\u1f07\u1f10-\u1f15\u1f20-\u1f27\u1f30-\u1f37\u1f40-\u1f45\u1f50-\u1f57\u1f60-\u1f67\u1f70-\u1f7d\u1f80-\u1f87\u1f90-\u1f97\u1fa0-\u1fa7\u1fb0-\u1fb4\u1fb6\u1fb7\u1fbe\u1fc2-\u1fc4\u1fc6\u1fc7\u1fd0-\u1fd3\u1fd6\u1fd7\u1fe0-\u1fe7\u1ff2-\u1ff4\u1ff6\u1ff7\u2071\u207f\u210a\u210e\u210f\u2113\u212f\u2134\u2139\u213c\u213d\u2146-\u2149\u214e\u2184\u2c30-\u2c5e\u2c61\u2c65\u2c66\u2c68\u2c6a\u2c6c\u2c74\u2c76\u2c77\u2c81\u2c83\u2c85\u2c87\u2c89\u2c8b\u2c8d\u2c8f\u2c91\u2c93\u2c95\u2c97\u2c99\u2c9b\u2c9d\u2c9f\u2ca1\u2ca3\u2ca5\u2ca7\u2ca9\u2cab\u2cad\u2caf\u2cb1\u2cb3\u2cb5\u2cb7\u2cb9\u2cbb\u2cbd\u2cbf\u2cc1\u2cc3\u2cc5\u2cc7\u2cc9\u2ccb\u2ccd\u2ccf\u2cd1\u2cd3\u2cd5\u2cd7\u2cd9\u2cdb\u2cdd\u2cdf\u2ce1\u2ce3\u2ce4\u2d00-\u2d25\ufb00-\ufb06\ufb13-\ufb17\uff41-\uff5a'
    
        Lt = '\u01c5\u01c8\u01cb\u01f2\u1f88-\u1f8f\u1f98-\u1f9f\u1fa8-\u1faf\u1fbc\u1fcc\u1ffc'

        Lm = '\u02b0-\u02c1\u02c6-\u02d1\u02e0-\u02e4\u02ee\u037a\u0559\u0640\u06e5\u06e6\u07f4\u07f5\u07fa\u0e46\u0ec6\u10fc\u17d7\u1843\u1d2c-\u1d61\u1d78\u1d9b-\u1dbf\u2090-\u2094\u2d6f\u3005\u3031-\u3035\u303b\u309d\u309e\u30fc-\u30fe\ua015\ua717-\ua71a\uff70\uff9e\uff9f'
    
        Lo = '\u01bb\u01c0-\u01c3\u0294\u05d0-\u05ea\u05f0-\u05f2\u0621-\u063a\u0641-\u064a\u066e\u066f\u0671-\u06d3\u06d5\u06ee\u06ef\u06fa-\u06fc\u06ff\u0710\u0712-\u072f\u074d-\u076d\u0780-\u07a5\u07b1\u07ca-\u07ea\u0904-\u0939\u093d\u0950\u0958-\u0961\u097b-\u097f\u0985-\u098c\u098f\u0990\u0993-\u09a8\u09aa-\u09b0\u09b2\u09b6-\u09b9\u09bd\u09ce\u09dc\u09dd\u09df-\u09e1\u09f0\u09f1\u0a05-\u0a0a\u0a0f\u0a10\u0a13-\u0a28\u0a2a-\u0a30\u0a32\u0a33\u0a35\u0a36\u0a38\u0a39\u0a59-\u0a5c\u0a5e\u0a72-\u0a74\u0a85-\u0a8d\u0a8f-\u0a91\u0a93-\u0aa8\u0aaa-\u0ab0\u0ab2\u0ab3\u0ab5-\u0ab9\u0abd\u0ad0\u0ae0\u0ae1\u0b05-\u0b0c\u0b0f\u0b10\u0b13-\u0b28\u0b2a-\u0b30\u0b32\u0b33\u0b35-\u0b39\u0b3d\u0b5c\u0b5d\u0b5f-\u0b61\u0b71\u0b83\u0b85-\u0b8a\u0b8e-\u0b90\u0b92-\u0b95\u0b99\u0b9a\u0b9c\u0b9e\u0b9f\u0ba3\u0ba4\u0ba8-\u0baa\u0bae-\u0bb9\u0c05-\u0c0c\u0c0e-\u0c10\u0c12-\u0c28\u0c2a-\u0c33\u0c35-\u0c39\u0c60\u0c61\u0c85-\u0c8c\u0c8e-\u0c90\u0c92-\u0ca8\u0caa-\u0cb3\u0cb5-\u0cb9\u0cbd\u0cde\u0ce0\u0ce1\u0d05-\u0d0c\u0d0e-\u0d10\u0d12-\u0d28\u0d2a-\u0d39\u0d60\u0d61\u0d85-\u0d96\u0d9a-\u0db1\u0db3-\u0dbb\u0dbd\u0dc0-\u0dc6\u0e01-\u0e30\u0e32\u0e33\u0e40-\u0e45\u0e81\u0e82\u0e84\u0e87\u0e88\u0e8a\u0e8d\u0e94-\u0e97\u0e99-\u0e9f\u0ea1-\u0ea3\u0ea5\u0ea7\u0eaa\u0eab\u0ead-\u0eb0\u0eb2\u0eb3\u0ebd\u0ec0-\u0ec4\u0edc\u0edd\u0f00\u0f40-\u0f47\u0f49-\u0f6a\u0f88-\u0f8b\u1000-\u1021\u1023-\u1027\u1029\u102a\u1050-\u1055\u10d0-\u10fa\u1100-\u1159\u115f-\u11a2\u11a8-\u11f9\u1200-\u1248\u124a-\u124d\u1250-\u1256\u1258\u125a-\u125d\u1260-\u1288\u128a-\u128d\u1290-\u12b0\u12b2-\u12b5\u12b8-\u12be\u12c0\u12c2-\u12c5\u12c8-\u12d6\u12d8-\u1310\u1312-\u1315\u1318-\u135a\u1380-\u138f\u13a0-\u13f4\u1401-\u166c\u166f-\u1676\u1681-\u169a\u16a0-\u16ea\u1700-\u170c\u170e-\u1711\u1720-\u1731\u1740-\u1751\u1760-\u176c\u176e-\u1770\u1780-\u17b3\u17dc\u1820-\u1842\u1844-\u1877\u1880-\u18a8\u1900-\u191c\u1950-\u196d\u1970-\u1974\u1980-\u19a9\u19c1-\u19c7\u1a00-\u1a16\u1b05-\u1b33\u1b45-\u1b4b\u2135-\u2138\u2d30-\u2d65\u2d80-\u2d96\u2da0-\u2da6\u2da8-\u2dae\u2db0-\u2db6\u2db8-\u2dbe\u2dc0-\u2dc6\u2dc8-\u2dce\u2dd0-\u2dd6\u2dd8-\u2dde\u3006\u303c\u3041-\u3096\u309f\u30a1-\u30fa\u30ff\u3105-\u312c\u3131-\u318e\u31a0-\u31b7\u31f0-\u31ff\u3400\u4db5\u4e00\u9fbb\ua000-\ua014\ua016-\ua48c\ua800\ua801\ua803-\ua805\ua807-\ua80a\ua80c-\ua822\ua840-\ua873\uac00\ud7a3\uf900-\ufa2d\ufa30-\ufa6a\ufa70-\ufad9\ufb1d\ufb1f-\ufb28\ufb2a-\ufb36\ufb38-\ufb3c\ufb3e\ufb40\ufb41\ufb43\ufb44\ufb46-\ufbb1\ufbd3-\ufd3d\ufd50-\ufd8f\ufd92-\ufdc7\ufdf0-\ufdfb\ufe70-\ufe74\ufe76-\ufefc\uff66-\uff6f\uff71-\uff9d\uffa0-\uffbe\uffc2-\uffc7\uffca-\uffcf\uffd2-\uffd7\uffda-\uffdc'

        Nl = '\u16ee-\u16f0\u2160-\u2182\u3007\u3021-\u3029\u3038-\u303a'

        Mn = '\u0300-\u036f\u0483-\u0486\u0591-\u05bd\u05bf\u05c1\u05c2\u05c4\u05c5\u05c7\u0610-\u0615\u064b-\u065e\u0670\u06d6-\u06dc\u06df-\u06e4\u06e7\u06e8\u06ea-\u06ed\u0711\u0730-\u074a\u07a6-\u07b0\u07eb-\u07f3\u0901\u0902\u093c\u0941-\u0948\u094d\u0951-\u0954\u0962\u0963\u0981\u09bc\u09c1-\u09c4\u09cd\u09e2\u09e3\u0a01\u0a02\u0a3c\u0a41\u0a42\u0a47\u0a48\u0a4b-\u0a4d\u0a70\u0a71\u0a81\u0a82\u0abc\u0ac1-\u0ac5\u0ac7\u0ac8\u0acd\u0ae2\u0ae3\u0b01\u0b3c\u0b3f\u0b41-\u0b43\u0b4d\u0b56\u0b82\u0bc0\u0bcd\u0c3e-\u0c40\u0c46-\u0c48\u0c4a-\u0c4d\u0c55\u0c56\u0cbc\u0cbf\u0cc6\u0ccc\u0ccd\u0ce2\u0ce3\u0d41-\u0d43\u0d4d\u0dca\u0dd2-\u0dd4\u0dd6\u0e31\u0e34-\u0e3a\u0e47-\u0e4e\u0eb1\u0eb4-\u0eb9\u0ebb\u0ebc\u0ec8-\u0ecd\u0f18\u0f19\u0f35\u0f37\u0f39\u0f71-\u0f7e\u0f80-\u0f84\u0f86\u0f87\u0f90-\u0f97\u0f99-\u0fbc\u0fc6\u102d-\u1030\u1032\u1036\u1037\u1039\u1058\u1059\u135f\u1712-\u1714\u1732-\u1734\u1752\u1753\u1772\u1773\u17b7-\u17bd\u17c6\u17c9-\u17d3\u17dd\u180b-\u180d\u18a9\u1920-\u1922\u1927\u1928\u1932\u1939-\u193b\u1a17\u1a18\u1b00-\u1b03\u1b34\u1b36-\u1b3a\u1b3c\u1b42\u1b6b-\u1b73\u1dc0-\u1dca\u1dfe\u1dff\u20d0-\u20dc\u20e1\u20e5-\u20ef\u302a-\u302f\u3099\u309a\ua806\ua80b\ua825\ua826\ufb1e\ufe00-\ufe0f\ufe20-\ufe23'

        Mc = '\u0903\u093e-\u0940\u0949-\u094c\u0982\u0983\u09be-\u09c0\u09c7\u09c8\u09cb\u09cc\u09d7\u0a03\u0a3e-\u0a40\u0a83\u0abe-\u0ac0\u0ac9\u0acb\u0acc\u0b02\u0b03\u0b3e\u0b40\u0b47\u0b48\u0b4b\u0b4c\u0b57\u0bbe\u0bbf\u0bc1\u0bc2\u0bc6-\u0bc8\u0bca-\u0bcc\u0bd7\u0c01-\u0c03\u0c41-\u0c44\u0c82\u0c83\u0cbe\u0cc0-\u0cc4\u0cc7\u0cc8\u0cca\u0ccb\u0cd5\u0cd6\u0d02\u0d03\u0d3e-\u0d40\u0d46-\u0d48\u0d4a-\u0d4c\u0d57\u0d82\u0d83\u0dcf-\u0dd1\u0dd8-\u0ddf\u0df2\u0df3\u0f3e\u0f3f\u0f7f\u102c\u1031\u1038\u1056\u1057\u17b6\u17be-\u17c5\u17c7\u17c8\u1923-\u1926\u1929-\u192b\u1930\u1931\u1933-\u1938\u19b0-\u19c0\u19c8\u19c9\u1a19-\u1a1b\u1b04\u1b35\u1b3b\u1b3d-\u1b41\u1b43\u1b44\ua802\ua823\ua824\ua827'

        Nd = '\u0030-\u0039\u0660-\u0669\u06f0-\u06f9\u07c0-\u07c9\u0966-\u096f\u09e6-\u09ef\u0a66-\u0a6f\u0ae6-\u0aef\u0b66-\u0b6f\u0be6-\u0bef\u0c66-\u0c6f\u0ce6-\u0cef\u0d66-\u0d6f\u0e50-\u0e59\u0ed0-\u0ed9\u0f20-\u0f29\u1040-\u1049\u17e0-\u17e9\u1810-\u1819\u1946-\u194f\u19d0-\u19d9\u1b50-\u1b59\uff10-\uff19'

        Pc = '\u005f\u203f\u2040\u2054\ufe33\ufe34\ufe4d-\ufe4f\uff3f'

        Pd = '\u002d\u058a\u1806\u2010-\u2015\u2e17\u301c\u3030\u30a0\ufe31\ufe32\ufe58\ufe63\uff0d'

        #INITIAL_NAME_CLASSES = [ 'Lu', 'Ll', 'Lt', 'Lm', 'Lo', 'Nl' ]  # original
        # modified version to use explicit matches
        INITIAL_NAME_CLASSES = [Lu, Ll, Lt, Lm, Lo, Nl]

        #NON_INITIAL_NAME_CLASSES = [ 'Mn', 'Mc', 'Nd', 'Pc' ]  # original
        # Modified version to use explicit matches and also allow additional
        # punctuation to be used in the JSON names.
        NON_INITIAL_NAME_CLASSES = [Mn, Mc, Nd, Pd, Pc, "\\\\\.`¬!£$%^&+=|;:@#~/?,<>"]

        FOLLOWING_NAME_CLASSES = INITIAL_NAME_CLASSES + NON_INITIAL_NAME_CLASSES
        DOT_SEPARATOR = '\.\.?'

        def classes_to_re(classes):
            #re_classes = list(map(lambda x: f"\\p{{{x}}}", classes))  # original
            #return f"[{''.join(re_classes)}]"  # original
            return f"[{''.join(classes)}]"  # modified with explicit matches

        # Modified to only use FOLLOWING_NAME_CLASSES, as the original would
        # fail on names starting with allowed punctuation e.g. $._error
        name_re = (#classes_to_re(INITIAL_NAME_CLASSES) +
                   classes_to_re(FOLLOWING_NAME_CLASSES) + '*')
        dot_step = DOT_SEPARATOR + '((' + name_re + ')|(\*))'
        rp_dot_step = DOT_SEPARATOR + name_re
        bracket_step = '\[' + "'" + name_re + "'" + '\]'
        rp_num_index = '\[-?\d+\]'  # Original was '\[\d+\]' which didn't allow minus
        num_index = '\[\d+(, *\d+)?\]'
        star_index = '\[\*\]'
        colon_index = '\[(-?\d+)?:(-?\d+)?\]'
        # The original Ruby code had this block for matching indices
        #index = '((' + num_index + ')|(' + star_index + ')|(' + colon_index + '))'
        # However that fails to match filter/script expressions e.g. ?() or ()
        # for example $..book[(@.length-1)], $..book[?(@.isbn)], $..book[?(@.price<10)]
        # We add a script_index block to match this case.
        script_index = '\[\??\(.*\)\]'
        index = ('((' + num_index + ')|(' + star_index + ')|' + 
                  '(' + script_index + ')|(' + colon_index + '))')

        step = ('((' + dot_step + ')|(' + bracket_step + ')|' + 
                 '(' + index + '))' + '(' + index + ')?')
        # Original rp_step ends with 0 or 1 match ? which fails to match
        # $.ledgers[0][22][315].foo I think it should be a 0 or more match *
        #rp_step = '((' + rp_dot_step + ')|(' + bracket_step + '))' + '(' + rp_num_index + ')?'
        rp_step = '((' + rp_dot_step + ')|(' + bracket_step + '))' + '(' + rp_num_index + ')*'
        path = '^\$\$?' + '(' + step + ')*$'
        reference_path = '^\$' + '(' + rp_step + ')*$'

        self.path_re = re.compile(path)
        self.reference_path_re = re.compile(reference_path)

    def is_path(self, s):
        #if isinstance(s, str): print(self.path_re.match(s))
        return isinstance(s, str) and self.path_re.match(s)

    def is_reference_path(self, s):
        #if isinstance(s, str): print(self.reference_path_re.match(s))
        return isinstance(s, str) and self.reference_path_re.match(s)

#-------------------------------------------------------------------------------

class Constraint():
    """
    Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/constraints.rb#L24
    These all respond_to
    check(node, path, problem)
     - node is the JSON node being checked
     - path is the current path, for reporting practices
     - problems is a list of problem reports
    TODO: Add a "role" argument to enrich error reporting
    """
    def __init__(self):
        self.conditions = []

    def add_condition(self, condition):
        #print("Constraint add_condition")
        self.conditions.append(condition)

    def applies(self, node, role):
        #print(f"Constraint applies {node} {role} {self.conditions}")
        return (len(self.conditions) == 0 or
                any(c.constraint_applies(node, role) for c in self.conditions))

#-------------------------------------------------------------------------------

class OnlyOneOfConstraint(Constraint):
    # Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/constraints.rb#L41
    # Verify that there is only one of a selection of fields
    def __init__(self, fields):
        super().__init__()
        #print(f"OnlyOneOfConstraint {fields}")
        self.fields = fields

    def check(self, node, path, problems):
        #print(f"OnlyOneOfConstraint (check only one of {self.fields}) {node} {path} {problems}")
        if len(list(filter(lambda f: f in self.fields, node.keys()))) > 1:
            problems.append(f'{path} may have only one of {self.fields}')

#-------------------------------------------------------------------------------

class NonEmptyConstraint(Constraint):
    # Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/constraints.rb#L57
    # Verify that array field is not empty
    def __init__(self, name):
        super().__init__()
        #print(f"NonEmptyConstraint {name}")
        self.name = name

    def check(self, node, path, problems):
        #print(f"NonEmptyConstraint (check {self.name} is non empty) {node} {path} {problems}")
        value = node.get(self.name)
        if isinstance(value, list) and len(value) == 0:
            problems.append(f'{path}.{self.name} is empty, non-empty required')

#-------------------------------------------------------------------------------

class HasFieldConstraint(Constraint):
    # Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/constraints.rb#L78
    # Verify node has the named field, or one of the named fields
    def __init__(self, names):
        super().__init__()
        self.names = names if isinstance(names, list) else [names]
        #print(f"HasFieldConstraint {self.names}")

    def check(self, node, path, problems):
        #print(f"-------- HasFieldConstraint (check has {self.names} field) {node} {path} {problems}")
        # Check if any of the contraint names are present in the supplied node
        if not any(name in node for name in self.names):
            if len(self.names) == 1:
                problems.append(f'{path} does not have required field "{self.names[0]}"')
            else:
                problems.append(f'{path} does not have required field from {self.names}')

#-------------------------------------------------------------------------------

class DoesNotHaveFieldConstraint(Constraint):
    # Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/constraints.rb#L108
    # Verify node does not have the named field
    def __init__(self, name):
        super().__init__()
        #print(f"DoesNotHaveFieldConstraint {name}")
        self.name = name

    def check(self, node, path, problems):
        #print(f"DoesNotHaveFieldConstraint (check does not have field {self.name}) {node} {path} {problems}")
        if self.name in node:
            problems.append(f'{path} has forbidden field "{self.name}"')

#-------------------------------------------------------------------------------

class FieldTypeConstraint(Constraint):
    # Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/constraints.rb#L129
    # Verify type of a field in a node
    def __init__(self, name, type, is_array, is_nullable):
        super().__init__()
        #print(f"FieldTypeConstraint {name}, {type}, {is_array}, {is_nullable}")
        self.name = name
        self.type = type
        self.is_array = is_array
        self.is_nullable = is_nullable

    def check(self, node, path, problems):
        #print(f"FieldTypeConstraint (check {self.name} is {self.type}) {node} {path} {problems}")

        # type-checking is orthogonal to existence checking
        if self.name not in node:
            return

        value = node.get(self.name)
        path = f"{path}.{self.name}"

        #print(f"path: {path}")
        #print(f"value: {value}")

        if value == None:
            if not self.is_nullable:
                problems.append(f'{path} should be non-null')
            return

        if self.is_array:
            if isinstance(value, list):
                for i, element in enumerate(value):
                    self.value_check(element, "{path}[{i}]", problems)
            else:
                self.report(path, value, "an Array", problems)
        else:
            self.value_check(value, path, problems)

    def value_check(self, value, path, problems):
        #print(f"FieldTypeConstraint value_check {value} {path} {problems}")
        #print(f"self.type: {self.type}")

        if self.type == "object" and not isinstance(value, dict):
             self.report(path, value, "an Object", problems)
        elif self.type == "array" and not isinstance(value, list):
             self.report(path, value, "an Array", problems)
        elif self.type == "string" and not isinstance(value, str):
             self.report(path, value, "a String", problems)
        elif self.type == "integer" and not isinstance(value, int):
             self.report(path, value, "an Integer", problems)
        elif self.type == "float" and not isinstance(value, float):
             self.report(path, value, "a Float", problems)
        elif self.type == "boolean" and value != True and value != False:
             self.report(path, value, "a Boolean", problems)
        elif self.type == "numeric" and not (isinstance(value, int) or 
                                             isinstance(value, float)):
             self.report(path, value, "a Numeric", problems)
        elif self.type == "JSONPath" and not JSONPathChecker().is_path(value):
             self.report(path, value, "a JSONPath", problems)
        elif self.type == "referencePath"and not JSONPathChecker().is_reference_path(value):
             self.report(path, value, "a Reference Path", problems)
        elif self.type == "timestamp":
            # Preprocess RFC3339 into template strptime format
            if value[-1] == "Z":
                date = value[:-1]
            else:
                date = value[:-6]

            if "." not in date:
                date = date + ".0"

            try:
                datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f")
            except Exception as e:
                self.report(path, value, "an RFC3339 timestamp", problems)
        elif self.type == "URI" and not (isinstance(value, str) and
                                         re.match("[a-z]+:", value)):
             self.report(path, value, "a URI", problems)

    def report(self, path, value, message, problems):
        #print(f"FieldTypeConstraint report {path} {value} {message} {problems}")
        if isinstance(value, str):
            value = '"' + value + '"'
        problems.append(f'{path} is {value} but should be {message}')

#-------------------------------------------------------------------------------

class FieldValueConstraint(Constraint):
    # Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/constraints.rb#L221
    # Verify constraints on values of a named field
    def __init__(self, name, params):
        super().__init__()
        #print(f"FieldValueConstraint {name} {params}")
        self.name = name
        self.params = params

    def check(self, node, path, problems):
        #print(f"FieldValueConstraint {self.name} {self.params} check {node} {path} {problems}")

        # value-checking is orthogonal to existence checking
        if self.name not in node:
            return

        value = node.get(self.name)
        if "enum" in self.params:
            enum = self.params.get("enum")
            if value not in enum:
                problems.append(
                    f'{path}.{self.name} is "{value}", ' +
                    f'not one of the allowed values {enum}'
                )
            # if enum constraints are provided, others are ignored
            return

        if "equal" in self.params:
            equal = self.params.get("equal")
            try:
                if value != equal:
                    problems.append(
                        f'{path}.{self.name} is {value}, ' +
                        f'but required value is {equal}'
                    )
            except: # Wrong type should be reported by type constraint
                pass 

        if "floor" in self.params:
            floor = self.params.get("floor")
            try:
                if value <= floor:
                    problems.append(
                        f'{path}.{self.name} is {value}, ' +
                        f'but allowed floor is {floor}'
                    )
            except: # Wrong type should be reported by type constraint
                pass 

        if "min" in self.params:
            min = self.params.get("min")
            try:
                if value < min:
                    problems.append(
                        f'{path}.{self.name} is {value}, ' +
                        f'but allowed minimum is {min}'
                    )
            except: # Wrong type should be reported by type constraint
                pass 

        if "ceiling" in self.params:
            ceiling = self.params.get("ceiling")
            try:
                if value >= ceiling:
                    problems.append(
                        f'{path}.{self.name} is {value}, ' +
                        f'but allowed ceiling is {ceiling}')
            except: # Wrong type should be reported by type constraint
                pass 

        if "max" in self.params:
            max = self.params.get("max")
            try:
                if value > max:
                    problems.append(
                        f'{path}.{self.name} is {value}, ' +
                        f'but allowed maximum is {max}')
            except: # Wrong type should be reported by type constraint
                pass 

#-------------------------------------------------------------------------------

class RoleFinder():
    """
    Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/role_finder.rb
    This is about figuring out which roles apply to a node and
    potentially to its children in object and array valued fields
    """
    def __init__(self):
        # roles of the form: If an object with role X has field Y which
        # is an object, that object has role R
        self.child_roles = {}

        # roles of the form: If an object with role X has field Y which
        # is an object/array, the object-files/array-elements have role R
        self.grandchild_roles = {}

        # roles of the form: If an object with role X has a field Y with
        # value Z, it has role R
        # map[role][field_name][field_val] => child_role
        self.field_value_roles = {}

        # roles of the form: If an object with role X has a field Y, then
        # it has role R
        # map[role][field_name] => child_role
        self.field_presence_roles = {}

        # roles of the form: A Foo is a Bar
        self.is_a_roles = {}

    def add_is_a_role(self, role, other_role):
        self.is_a_roles.setdefault(role, []).append(other_role)

    def add_field_value_role(self, role, field_name, field_value, new_role):
        self.field_value_roles.setdefault(role, {}).setdefault(field_name, {})
        field_value = deduce(field_value)
        self.field_value_roles[role][field_name][field_value] = new_role

    def add_field_presence_role(self, role, field_name, new_role):
        self.field_presence_roles.setdefault(role, {})[field_name] = new_role

    def add_child_role(self, role, field_name, child_role):
        self.child_roles.setdefault(role, {})[field_name] = child_role

    def add_grandchild_role(self, role, field_name, child_role):
        self.grandchild_roles.setdefault(role, {})[field_name] = child_role


    def find_more_roles(self, node, roles):
        """
        Consider a node which has one or more roles. It may have more
        roles based on the presence or value of child nodes. This method
        addes any such roles to the "roles" list
        """
        # find roles depending on field values
        for role in roles:
            per_field_name = self.field_value_roles.get(role)
            if per_field_name:
                for field_name, value_roles in per_field_name.items():
                    for field_value, child_role in value_roles.items():
                        if field_value == node.get(field_name):
                            roles.append(child_role)

        # find roles depending on field presence
        for role in roles:
            per_field_name = self.field_presence_roles.get(role)
            if per_field_name:
                for field_name, child_role in per_field_name.items():
                    if field_name in node:
                        roles.append(child_role)

        # is_a roles
        for role in roles:
            other_roles = self.is_a_roles.get(role)
            if other_roles:
                roles.extend(other_roles)

    def find_child_roles(self, roles, field_name):
        """
        A node has a role, and one of its fields might be object-valued
        and that value is given a role
        """
        newroles = []
        for role in roles:
            if field_name in self.child_roles.get(role, {}):
                newroles.append(self.child_roles[role][field_name])

        return newroles

    def find_grandchild_roles(self, roles, field_name):
        """
        A node has a role, and one of its field is an object or an
        array whose fields or elements are given a role
        """
        newroles = []
        for role in roles:
            if field_name in self.grandchild_roles.get(role, {}):
                newroles.append(self.grandchild_roles[role][field_name])

        return newroles

#-------------------------------------------------------------------------------

class RoleNotPresentCondition():
    """
    Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/conditional.rb
    to be applied to a role/constraint combo, so the constraint is applied
    conditionally

    These all respond_to
    constraint_applies(node, roles)
     - node is the JSON node being checked
     - roles is the roles the node currently has
    """
    def __init__(self, exclude_roles):
        #print("RoleNotPresentCondition {exclude_roles}")
        self.excluded_roles = exclude_roles

    def constraint_applies(self, node, roles):
        #print("RoleNotPresentCondition constraint_applies")
        return not any(role in self.excluded_roles for role in roles)

#-------------------------------------------------------------------------------

class AllowedFields():
    """
    Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/allowed_fields.rb
    The States language is draconian/must-understand; no fields may appear
    which aren't explicitly blessed by a MUST/MAY clause
    """
    def __init__(self):
        self.allowed = {}
        self.any = []

    def set_allowed(self, role, child):
        self.allowed.setdefault(role, []).append(child)

    def set_any(self, role):
        self.any.append(role)

    def is_allowed(self, roles, child):
        allows_any = self.allows_any(roles)
        #print(f"AllowedFields is_allowed {roles} {child} {allows_any or any(child in self.allowed.get(role, []) for role in roles)}")
        return allows_any or any(child in self.allowed.get(role, []) for role in roles)

    def allows_any(self, roles):
        return any(role in self.any for role in roles)

#-------------------------------------------------------------------------------

class Matcher():
    """
    Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/matcher.rb
    Matches and extracts the IETF RFC 2119 style assertions using regular
    expressions with named groups for captures.
    """
    def __init__(self, root):
        self.roles = []
        self.add_role(root)

    def add_role(self, role):
        self.roles.append(role)
        self.role_matcher = "|".join(set(self.roles))  # set() gets unique values
        self.reconstruct()

    def make_type_regex(self):
        types = [
            'array', 'object', 'string', 'boolean', 'numeric', 'integer',
            'float', 'timestamp', 'JSONPath', 'referencePath', 'URI'
        ]

        # Add modified numeric types
        number_types = ['float', 'integer', 'numeric']
        number_modifiers = ['positive', 'negative', 'nonnegative']

        for number_type in number_types:
            for number_modifier in number_modifiers:
                types.append(f"{number_modifier}-{number_type}")

        # Add array types
        array_types = list(map(lambda t: f"{t}-array", types))
        types = types + array_types
        nonempty_array_types = list(map(lambda t: f"nonempty-{t}", types))
        types = types + nonempty_array_types
        nullable_types = list(map(lambda t: f"nullable-{t}", types))
        types = types + nullable_types

        self.type_regex = "|".join(types)

    def reconstruct(self):
        self.make_type_regex()

        MUST = '(?P<modal>MUST|MAY|MUST NOT)'

        RELATIONS = "|".join([
            'equal to', 'greater than', 'less than',
            'greater than or equal to', 'less than or equal to'
        ])
        RELATION = f"((?P<relation>{RELATIONS})\\s+)"

        S = '"[^"]*"' # string
        V = '\S+'     # non-string value: number, true, false, null
        RELATIONAL = f"{RELATION}(?P<target>{S}|{V})"

        CHILD_ROLE = (';\\s+((its\\s+(?P<child_type>value))|' +
            'each\\s+(?P<child_type_each>field|element))' +
            '\\s+is\\s+an?\\s+' +
            '"(?P<child_role>[^"]+)"')

        strings = Oxford().re(S, capture_name="strings")
        enums = f"one\\s+of\\s{strings}"
        predicate = f"({RELATIONAL}|{enums})"

        # Conditional clause
        excluded_roles = ('not\\s+' +
            Oxford().re(
                self.role_matcher, capture_name="excluded", use_article=True
            ) +
        '\\s+')
        conditional = "which\\s+is\\s+" + excluded_roles

        # regex for matching constraint lines
        c_start = f'^An?\\s+(?P<role>{self.role_matcher})\\s+({conditional})?{MUST}\\s+have\\s+an?\\s+'
        #print("++++++++++++++")
        #print(c_start)
        #print("++++++++++++++")

        field_list = ("one\\s+of\\s+" +
            Oxford().re('"[^"]+"', capture_name="field_list"))

        c_match = (c_start + 
            f"((?P<type>{self.type_regex})\\s+)?" +
            "field\\s+named\\s+" +
            f"((\"(?P<field_name>[^\"]+)\")|({field_list}))" +
            f"(\\s+whose\\s+value\s+MUST\\s+be\\s+{predicate})?" +
            f"({CHILD_ROLE})?" +
            "\\.")

        """
        regex for matching lines of the form:
        "An X MUST have only one of "Y", "Z", and "W".
        There's a pattern here, building a separate regex rather than
        adding more complexity to constraint_matcher. Any further additions
        should be done this way, and
        TODO: Break constraint_matcher into a bunch of smaller patterns
        like this.
        """
        oo_start = ("^An?\\s+" +
            f"(?P<role>{self.role_matcher})\\s+" +
            f"{MUST}\\s+have\\s+only\\s+")
        oo_field_list = ("one\\s+of\\s+" +
            Oxford().re('"[^"]+"', capture_name="field_list", connector="and"))
        oo_match = oo_start + oo_field_list

        # regex for matching role-def lines
        val_match = ('whose\\s+"(?P<fieldtomatch>[^"]+)"' +
            '\\s+field\'s\\s+value\\s+is\\s+' +
            '(?P<valtomatch>("[^"]*")|([^"\\s]\\S+))\\s+')
        with_a_match = 'with\\s+an?\\s+"(?P<with_a_field>[^"]+)"\\s+field\\s'
        rd_match = ('^An?\\s+' +
            f'(?P<role>{self.role_matcher})\\s+' +
            f'((?P<val_match_present>{val_match})|({with_a_match}))?' +
            'is\\s+an?\\s+' +
            '"(?P<newrole>[^"]*)"\\.\\s*$')

        # regex for matching each of lines
        eo_start = "^Each\\s+of\\s"
        eo_match = (eo_start +
            Oxford().re(
                self.role_matcher, capture_name="each_of", use_article=True, connector="and"
            ) +
        "\\s+(?P<trailer>.*)$")

        self.roledef_line = re.compile('is\\s+an?\\s+"[^"]*"\\.\\s*$')
        self.roledef_match = re.compile(rd_match)
        self.constraint_start = re.compile(c_start)
        self.constraint_match = re.compile(c_match)
        self.only_one_start = re.compile(oo_start)
        self.only_one_match = re.compile(oo_match)
        self.eachof_start = re.compile(eo_start)
        self.eachof_match = re.compile(eo_match)

    def is_constraint_line(self, line):
        return self.constraint_start.match(line)

    def is_only_one_match_line(self, line):
        return self.only_one_start.match(line)

    def is_each_of_line(self, line):
        return self.eachof_start.match(line)

    def is_role_def_line(self, line):
        #return self.roledef_line.match(line)
        return self.roledef_line.search(line)

    def build(self, re, line):
        match = re.match(line)
        if not match:
            raise Exception(f"Matcher unable to build line")

        # Use dict comprehension to return dict with only non-None values
        return {k:v for (k, v) in match.groupdict().items() if v}

    def build_constraint(self, line):
        constraint = self.build(self.constraint_match, line)
        # Original Ruby code uses duplicate child_type capture group name, which
        # re disallows, so copy the child_type_each result across if present
        if "child_type_each" in constraint:
            constraint["child_type"] = constraint["child_type_each"]

        return constraint

    def build_only_one(self, line):
        return self.build(self.only_one_match, line)

    def build_each_ofs(self, line):
        eaches_line = self.eachof_match.match(line)
        eaches = Oxford().break_role_list(self, eaches_line["each_of"])
        return eaches, eaches_line["trailer"]

    def build_role_def(self, line):
        return self.build(self.roledef_match, line)

#-------------------------------------------------------------------------------

class Oxford():
    """
    Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/oxford.rb
    We have to recognise lots of lists like so:
    X
    X or X
    X, X, or X
    Examples:
    one of "Foo", "Bar", or "Baz"
    a Token1, a Token2, or a Token3
    """
    def re(self, particle, **kwargs):
        BASIC = "(?P<CAPTURE>X((((,\\s+X)+,)?)?\\s+or\\s+X)?)"
        has_capture, inter, has_connector, last = BASIC.split("X")

        connector = kwargs.get("connector")
        if connector:
            has_connector = has_connector.replace('or', connector)

        if kwargs.get("use_article"):
            particle = f"an?\\s+({particle})"
        else:
            particle = f"({particle})"

        capture_name = kwargs.get("capture_name")
        if capture_name:
            has_capture = has_capture.replace('CAPTURE', capture_name)
        else:
            has_capture = has_capture.replace('?<CAPTURE>', '')

        return particle.join([has_capture, inter, has_connector, last])

    def break_string_list(self, list):
        pieces = []
        breaker = re.compile("^[^\"]*\"([^\"]*)\"")
        m = breaker.match(list)
        while m:
            pieces.append(m[1])
            list = list[len(m[0]):]
            m = breaker.match(list)
        return pieces

    def break_role_list(self, matcher, list):
        pieces = []
        breaker = re.compile(f"^an?\\s+({matcher.role_matcher})(,\\s+)?")
        m = breaker.match(list)
        while m:
            pieces.append(m[1])
            list = list[len(m[0]):]
            m = breaker.match(list)

        breaker = re.compile(f"^\\s*(and|or)\\s+an?\\s+({matcher.role_matcher})")
        m = breaker.match(list)
        if m:
            pieces.append(m[2])

        return pieces

#-------------------------------------------------------------------------------

class Assigner():
    """
    Based on https://github.com/awslabs/j2119/blob/master/lib/j2119/assigner.rb
    Looks at the parsed form of the J2119 lines and figures out,
    by looking at which part of the regexes match, 
    the assignments of roles to nodes and constraints to roles
    """
    def __init__(self, role_constraints, role_finder, matcher, allowed_fields):
        self.constraints = role_constraints
        self.roles = role_finder
        self.matcher = matcher
        self.allowed_fields = allowed_fields

    def assign_roles(self, assertion):
        role = assertion.get("role")
        val_match_present = assertion.get("val_match_present")
        with_a_field = assertion.get("with_a_field")
        fieldtomatch = assertion.get("fieldtomatch")
        valtomatch = assertion.get("valtomatch")
        newrole = assertion.get("newrole")

        if val_match_present:
            self.roles.add_field_value_role(
                role, fieldtomatch, valtomatch, newrole
            )
            self.matcher.add_role(newrole)
        elif with_a_field:
            self.roles.add_field_presence_role(
                role, with_a_field, newrole
            )
            self.matcher.add_role(newrole)
        else:
            self.roles.add_is_a_role(role, newrole)
            self.matcher.add_role(newrole)

    def assign_only_one_of(self, assertion):
        #print(f"assign_only_one_of: {assertion}")
        role = assertion.get("role")
        field_list_string = assertion.get("field_list", "")
        values = Oxford().break_string_list(field_list_string)
        self.add_constraint(role, OnlyOneOfConstraint(values), None)

    def assign_constraints(self, assertion):
        #print(f"assign_constraints: {assertion}")
        role = assertion.get("role")
        modal = assertion.get("modal")
        type = assertion.get("type")
        field_name = assertion.get("field_name")
        field_list_string = assertion.get("field_list")
        relation = assertion.get("relation")
        target = assertion.get("target")
        strings = assertion.get("strings")
        child_type = assertion.get("child_type")
        vals = assertion.get("vals")
        excluded = assertion.get("excluded")

        condition = None
        if excluded:
            excluded_roles = Oxford().break_role_list(self.matcher, excluded)
            condition = RoleNotPresentCondition(excluded_roles)

        if relation:
            self.add_relation_constraint(role, field_name, relation, target, condition)

        if strings:
            # of the form MUST have a <type> field named <field_name> whose value
            #  MUST be one of "a", "b", or "c"
            fields = Oxford().break_string_list(strings)
            params = {"enum": fields}
            self.add_constraint(role, FieldValueConstraint(field_name, params), condition)

        if type:
            self.add_type_constraints(role, field_name, type, condition)

        if field_list_string:
            field_list = Oxford().break_string_list(field_list_string)

        # register allowed fields
        if field_list_string:
            for field in field_list:
                self.allowed_fields.set_allowed(role, field)
        elif field_name:
            self.allowed_fields.set_allowed(role, field_name)

        if modal == "MUST":
            if field_list_string:
                # Of the form MUST have a <type>? field named one of "a", "b", or "c".
                self.add_constraint(role, HasFieldConstraint(field_list), condition)
            else:
                self.add_constraint(role, HasFieldConstraint(field_name), condition)
        elif modal == "MUST NOT":
            self.add_constraint(role, DoesNotHaveFieldConstraint(field_name), condition)

        # There can be role defs there too
        if child_type:
            self.matcher.add_role(assertion["child_role"])
            if child_type == "value":
                self.roles.add_child_role(role, field_name, assertion["child_role"])
            elif child_type == "element" or child_type == "field":
                self.roles.add_grandchild_role(role, field_name, assertion["child_role"])
        else:
            anyOrObjectOrArray = not type or type == "object" or type == "array"
            # untyped field without a defined child role
            if field_name and anyOrObjectOrArray and modal != "MUST NOT":
                self.roles.add_grandchild_role(role, field_name, field_name)
                self.allowed_fields.set_any(field_name)

    def add_constraint(self, role, constraint, condition):
        if condition:
            constraint.add_condition(condition)
        self.constraints.add(role, constraint)

    def add_relation_constraint(self, role, field, relation, target, condition):
        target = deduce(target)
        operations = {
            "equal to": "equal",
            "greater than": "floor",
            "less than": "ceiling",
            "greater than or equal to": "min",
            "less than or equal to": "max"
        }

        params = {operations[relation]: target}
        self.add_constraint(role, FieldValueConstraint(field, params), condition)

    def add_type_constraints(self, role, field, type, condition):
        is_array = "-array" in type
        is_nullable = "nullable-" in type

        types = [
            'array', 'object', 'string', 'boolean', 'numeric', 'integer',
            'float', 'timestamp', 'JSONPath', 'referencePath', 'URI'
        ]

        value_operations = {
            "positive": "floor",
            "nonnegative": "min",
            "negative": "ceiling"
        }

        for part in type.split("-"):
            if part in types:
                if part == "array" and is_array:
                    return

                self.add_constraint(
                    role,
                    FieldTypeConstraint(field, part, is_array, is_nullable),
                    condition
                )
            if part in value_operations:
                params = {value_operations[part]: 0}
                self.add_constraint(role, FieldValueConstraint(field, params), condition)
            if part == "nonempty":
                self.add_constraint(role, NonEmptyConstraint(field), condition)

