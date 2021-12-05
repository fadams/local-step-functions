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
# Run with:
# PYTHONPATH=.. python3 test_state_engine_paths.py
#

import sys
assert sys.version_info >= (3, 0) # Bomb out if not running Python3

import unittest
import json

from asl_workflow_engine.state_engine_paths import apply_jsonpath
from asl_workflow_engine.asl_exceptions import *

"""
JSON document for use by examples from https://goessner.net/articles/JsonPath/
The link above is essentially the de facto definition/specification for JSONPath
from Stefan Goessner. The tests below reflect the set of examples given at
https://goessner.net/articles/JsonPath/index.html#e3 in the document.
"""
goessner = { "store": {
    "book": [ 
      { "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      { "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      },
      { "category": "fiction",
        "author": "Herman Melville",
        "title": "Moby Dick",
        "isbn": "0-553-21311-3",
        "price": 8.99
      },
      { "category": "fiction",
        "author": "J. R. R. Tolkien",
        "title": "The Lord of the Rings",
        "isbn": "0-395-19395-8",
        "price": 22.99
      }
    ],
    "bicycle": {
      "color": "red",
      "price": 19.95
    }
  }
}

simple_array = {
    "inputs": ["index 0", "index 1"]
}

empty_array = {
    "inputs": []
}


class TestStateEnginePaths(unittest.TestCase):

    def test_goessner01(self):
        print("The authors of all books in the store.")
        result = apply_jsonpath(goessner, "$.store.book[*].author")
        print(result)
        self.assertEqual(result, ["Nigel Rees", "Evelyn Waugh", "Herman Melville", "J. R. R. Tolkien"])

    def test_goessner02(self):
        print("All authors.")
        result = apply_jsonpath(goessner, "$..author")
        print(result)
        self.assertEqual(result, ["Nigel Rees", "Evelyn Waugh", "Herman Melville", "J. R. R. Tolkien"])

    def test_goessner03(self):
        print("All things in store, which are some books and a red bicycle.")
        result = apply_jsonpath(goessner, "$.store.*")
        print(result)
        success = False
        # The "bicycle" and "book" results could be returned in either order
        # in the result list so need to check both
        if (result == [[{"category": "reference", "price": 8.95, "author": "Nigel Rees", "title": "Sayings of the Century"}, {"category": "fiction", "price": 12.99, "author": "Evelyn Waugh", "title": "Sword of Honour"}, {"category": "fiction", "isbn": "0-553-21311-3", "price": 8.99, "author": "Herman Melville", "title": "Moby Dick"}, {"category": "fiction", "isbn": "0-395-19395-8", "price": 22.99, "author": "J. R. R. Tolkien", "title": "The Lord of the Rings"}], {"price": 19.95, "color": "red"}] or
            result == [{"price": 19.95, "color": "red"}, [{"category": "reference", "price": 8.95, "author": "Nigel Rees", "title": "Sayings of the Century"}, {"category": "fiction", "price": 12.99, "author": "Evelyn Waugh", "title": "Sword of Honour"}, {"category": "fiction", "isbn": "0-553-21311-3", "price": 8.99, "author": "Herman Melville", "title": "Moby Dick"}, {"category": "fiction", "isbn": "0-395-19395-8", "price": 22.99, "author": "J. R. R. Tolkien", "title": "The Lord of the Rings"}]]):
            success = True
        self.assertEqual(success, True)

    def test_goessner04(self):
        print("The price of everything in the store.")
        result = apply_jsonpath(goessner, "$.store..price")
        print(result)
        success = False
        # The "bicycle" and "book" results could be returned in either order
        # in the result list so need to check both
        if (result == [8.95, 12.99, 8.99, 22.99, 19.95] or
            result == [19.95, 8.95, 12.99, 8.99, 22.99]):
            success = True
        self.assertEqual(success, True)

    def test_goessner05(self):
        print("The third book.")
        result = apply_jsonpath(goessner, "$..book[2]")
        print(result)
        self.assertEqual(result, {"title": "Moby Dick", "category": "fiction", "author": "Herman Melville", "price": 8.99, "isbn": "0-553-21311-3"})

    def test_goessner06(self):
        print("The last book in the order.")
        result = apply_jsonpath(goessner, "$..book[(@.length-1)]")
        print(result)
        self.assertEqual(result, {"category": "fiction", "price": 22.99, "author": "J. R. R. Tolkien", "isbn": "0-395-19395-8", "title": "The Lord of the Rings"})

        # Or alternatively using slicing. Note that in this case a single item
        # array containing the object is returned from the slice operator.
        result = apply_jsonpath(goessner, "$..book[-1:]")
        print(result)
        self.assertEqual(result, [{"category": "fiction", "price": 22.99, "author": "J. R. R. Tolkien", "isbn": "0-395-19395-8", "title": "The Lord of the Rings"}])

    def test_goessner07(self):
        print("The first two books.")
        result = apply_jsonpath(goessner, "$..book[0,1]")
        print(result)
        self.assertEqual(result, [{"title": "Sayings of the Century", "author": "Nigel Rees", "category": "reference", "price": 8.95}, {"title": "Sword of Honour", "author": "Evelyn Waugh", "category": "fiction", "price": 12.99}])
        # Or alternatively using different syntax/filter
        result = apply_jsonpath(goessner, "$..book[:2]")
        print(result)
        self.assertEqual(result, [{"title": "Sayings of the Century", "author": "Nigel Rees", "category": "reference", "price": 8.95}, {"title": "Sword of Honour", "author": "Evelyn Waugh", "category": "fiction", "price": 12.99}])

    def test_goessner08(self):
        print("Filter all books with isbn number.")
        result = apply_jsonpath(goessner, "$..book[?(@.isbn)]")
        self.assertEqual(result, [{"isbn": "0-553-21311-3", "price": 8.99, "title": "Moby Dick", "author": "Herman Melville", "category": "fiction"}, {"isbn": "0-395-19395-8", "price": 22.99, "title": "The Lord of the Rings", "author": "J. R. R. Tolkien", "category": "fiction"}])

    def test_goessner09(self):
        print("Filter all books cheaper than 10")
        result = apply_jsonpath(goessner, "$..book[?(@.price<10)]")
        print(result)
        self.assertEqual(result, [{"title": "Sayings of the Century", "author": "Nigel Rees", "price": 8.95, "category": "reference"}, {"isbn": "0-553-21311-3", "title": "Moby Dick", "author": "Herman Melville", "price": 8.99, "category": "fiction"}])

    def test_goessner10(self):
        print("All members of JSON structure.")
        result = apply_jsonpath(goessner, "$..*")
        print(result)
        """
        TODO equality test. There are lots of permutations of correct result
        due to unordered nature of dict (at least for some python versions) so
        the returned list will potentially reflect that ordering. Comparing
        correctly is therefore long and rather tedious.
        """

    def test_index_out_of_bounds(self):
        print("Apply path with index out of bounds")

        result = None
        try:
            result = apply_jsonpath(simple_array, "$.inputs[2]")
        except PathMatchFailure as e:
            print("Correctly caught exception PathMatchFailure")

        self.assertEqual(result, None)

        try:
            result = apply_jsonpath(empty_array, "$.inputs[0]")
        except PathMatchFailure as e:
            print("Correctly caught exception PathMatchFailure")

        self.assertEqual(result, None)

if __name__ == "__main__":
    unittest.main()

