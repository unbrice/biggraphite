#!/usr/bin/env python
# Copyright 2016 Criteo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import absolute_import
from __future__ import print_function

import math
import unittest

import mock

from biggraphite.drivers import _utils


class CountDownTest(unittest.TestCase):

    _COUNT = 42

    def setUp(self):
        self.on_zero = mock.Mock()
        self.count_down = _utils.CountDown(self._COUNT, self.on_zero)

    def test_on_failure(self):
        exc = Exception()
        self.count_down.on_cassandra_failure(exc)
        self.on_zero.assert_called_once()

        # Failing again should not call the callback again.
        self.count_down.on_cassandra_failure(exc)
        self.on_zero.assert_called_once()

    def test_on_result(self):
        result = "whatever this is not used"
        for _ in xrange(self._COUNT - 1):
            self.count_down.on_cassandra_result(result)
            self.on_zero.assert_not_called()
        self.count_down.on_cassandra_result(result)
        self.on_zero.assert_called_with(None)


class FloatSharderTest(unittest.TestCase):

    VALUES = range(10) + [
        _utils.ShardedFloat.MIN_ABS*2,
        0.001, 0.03,
        0.0, 0.1, 0.5,
        math.pi, 32.0,
        2.0 ** 32, 2.0 ** 64,
        _utils.ShardedFloat.MAX_ABS,
    ]
    VALUES = VALUES + [-v for v in VALUES]

    def assertEqualWithMargin(self, v, vbis):
        if v != 0:
            margin_pct = abs(v-vbis)/abs(v) * 100
        else:
            margin_pct = abs(v) * 100
        self.assertLess(margin_pct, 1, "%f became %f (%0.2f%% imprecision)" % (v, vbis, margin_pct))


    def test_box_unbox(self):
        for v in self.VALUES:
            vbis = _utils.ShardedFloat.from_float(v).simplified.as_float
            self.assertEqualWithMargin(v, vbis)

    def test_add(self):
        for v1 in self.VALUES:
            v1_sharded =  _utils.ShardedFloat.from_float(v1).simplified
            for v2 in self.VALUES:
                v2_sharded =  _utils.ShardedFloat.from_float(v2).simplified
                v = v1 + v2
                vbis = v1_sharded.approx_add(v2_sharded).simplified.as_float
                self.assertEqualWithMargin(v, vbis)


if __name__ == "__main__":
    unittest.main()
