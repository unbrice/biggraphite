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


class TestDownsampler(bg_test_utils.TestCaseWithTempDir):

    def make_downsampler(self):
        return _downsampling.Downsampler(self.tempdir)

    def make_metric(self, buff, name="test_metric"):
        return bg_test_utils.make_metric(
            name,
            retention=buff.retention,
            aggregator=buff.aggregator,
        )


class TestMetricView(_BaseTestCase):

    def test_raw_access(self):
        retentions_strings = (
            "1*10s",
            "2*10s",
            "60*60s",
            "1*10s:2*20s",
            "2*10s:2*20s",
            "60*60s:24*3600s",
        )
        for retention_str in retentions_strings:
            buff = self.make_buffer(retention_str)
            metric = self.make_metric(buff)
            first_stage = buff.retention[0]
            expected_current = []
            expected_previous = []
            next_reset = 0

            # Insert points while computing a view of what it expects to be kept
            for v, ts in enumerate(xrange(0, first_stage.duration* 2, first_stage.precision)):
                metric_view = buff.get_metric_view(metric)
                if ts >= next_reset:
                    expected_previous = expected_current
                    expected_current = []
                    next_reset = ts + first_stage.duration
                metric_view.put(ts, v)
                expected_current.append(v)
                if expected_previous:
                    del expected_previous[0]
                previous, current = metric_view._split_raw()
                self.assertEqual(first_stage.points, len(previous)+len(current))
                current = filter_nans(current)
                previous = filter_nans(previous)

                self.assertEqual(ts, metric_view.last_update)
                # Order is undefined except newest value is first
                self.assertItemsEqual(expected_current, current)
                self.assertItemsEqual(expected_previous, previous)
                # Check that the newest value is last in result
                self.assertEqual(expected_current[-1], current[-1])
                if previous:
                    self.assertEqual(expected_previous[-1], previous[-1])

    def test_sampled_access(self):
        retention_str = "2*1s:1*3s"
        buff = self.make_buffer(retention_str)
        metric = self.make_metric(buff)
        first_stage = buff.retention[0]
        metric_view = buff.get_metric_view(metric)
        values = array.array("d")
        
        for v, ts in enumerate(xrange(0, buff.retention.duration* 2, first_stage.precision)):
            print("== PUT", "  t+%2d  " % ts, "  EPOCH=%d  " % buff.retention.stages[-1].epoch(ts), "V=%d" % v)
            metric_view.put(ts, v)
            values.append(v)
            previous, current = metric_view._split_raw()
            print("  R_stage1", ", ".join("%f" % v for v in current))
            print("  R_stage0", ", ".join("%f" % v for v in previous))
            print("  D", ", ".join("%f" % v for v in metric_view._aggregate_all_stages_from([],[])[1:]))
            print("  S", ", ".join("%f" % v for v in metric_view.aggregate_all_stages()))
            print("  E", sum(values[-3:]))
            print("== PUT", "  t+%2d  " % ts, "  EPOCH=%d  " % buff.retention.stages[-1].epoch(ts), "V=%d"% v)
            print()

        aggregates = metric_view.aggregate_all_stages()
        print("AGGREGATES_DELTAS  ", metric_view._aggregate_all_stages_from([],[]))
        print("AGGREGATES  ", aggregates)
        for n, stage in enumerate(buff.retention.stages):
            if n == 0:
                continue
            raw_epochs = stage.precision/first_stage.precision
            print("STAGE %2d " % (n+1),
                  " RAW_EPOCHS %d " % raw_epochs,
                  " HAS %d   " % aggregates[n], "WANTS %d" % sum(values[-raw_epochs:]))
            self.assertEqual(aggregates[n], sum(values[-raw_epochs:]))


class TestRetentionBuffer(_BaseTestCase):

    def test_estimate_size(self):
        buff = _downsampling._RetentionBuffer(self.tempdir, _AGGREGATOR, _RETENTION)
        size_0 = buff.estimate_size()
        buff.get_metric_view(self.make_metric("test1"))
        size_1 = buff.estimate_size()
        buff.get_metric_view(self.make_metric("test2"))
        size_2 = buff.estimate_size()
        self.assertLess(size_0, size_1)
        self.assertLess(size_1, size_2)

    def test_update(self):
        pass#TODO

if __name__ == "__main__":
    unittest.main()
