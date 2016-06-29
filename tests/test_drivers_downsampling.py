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

import array
import math
import unittest

import mock

from biggraphite import accessor as bg_accessor
from biggraphite import test_utils as bg_test_utils
from biggraphite.drivers import _downsampling


#_RETENTION = bg_accessor.Retention.from_string("60*60s:1*3600s:1*86400s")
#_RETENTION = bg_accessor.Retention.from_string("1*10s:2*20s")


def filter_nans(a):
    res = array.array("d")
    for v in a:
        if not math.isnan(v):
            res.append(v)
    return res


class TestHelpers(unittest.TestCase):

    def test_slice_circular_buffer(self):
        b = array.array("d", range(13))
        b_twice = b+b
        for begin in xrange(len(b)):
            for size in xrange(len(b)):
                end = begin + size
                expected = b_twice[begin:end+1]
                actual = _downsampling.slice_circular_buffer(b, begin, end)
                self.assertEqual(expected, actual)
        
        actual = _downsampling.slice_circular_buffer(b, 0, 1000000)
        self.assertEqual(b, actual)
        

class TestMetricData(unittest.TestCase):

    def setUp(self):
        self.buff = _downsampling.RetentionBuffer()
        self.metric = bg_test_utils.make_metric(
            "a.b.c",
            aggregator=bg_accessor.Aggregator.total,
            retention= "60*60s:24*3600s",
        )
        self.retention = self.metric.retention
        self.first_stage = self.retention[0]

    def test_internal_split_raw(self):
        """Test that split_raw implements a circular buffer."""
        data = self.buff.get_metric_data(self.metric)

        # First data._RAW_SIZE values
        values = range(self.first_stage.points-1)
        for n in values:
            print("\n%d" % n)
            timestamp = n * self.first_stage.precision
            data.put(timestamp, n)
            previous, current = data._split_raw()
            previous = filter_nans(previous)
            current = filter_nans(current)
            self.assertFalse(previous)
            pos = max(0, n-data._RAW_SIZE)
            self.assertItemsEqual(values[pos:n+1], current)
            self.assertEqual(n, data.current_stage0_epoch)

        # One more
        n += 1
        timestamp = n * self.first_stage.precision
        data.put(timestamp, n)
        previous, current = data._split_raw()
        print("P", previous)
        print("C", current)
        values[0] = n
        self.assertItemsEqual(values, current)
        self.assertFalse(filter_nans(previous))




if __name__ == "__main__":
    unittest.main()
