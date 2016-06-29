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
"""Local aggregation of metrics.

Some definitions used through the module:
- A "stage" is one of the elements of the retention policy.
  For example the policy "1m for 1d:1h for 1w" has two stages.
- A "stage epoch" is a range of timestamps: [N*stage_duration, (N+1)*stage_duration[
  For example if a stage duration is 60, epochs are [0, 59], [60, 119], ...
"""
from __future__ import absolute_import
from __future__ import print_function

import array
import math

from biggraphite import accessor as bg_accessor
from biggraphite.drivers import _utils as bg_drivers_utils


_NAN = float("nan")



class Error(bg_accessor.Error):
    """Base class for all exceptions from this module."""


def slice_circular_buffer(raw, begin, end):
    """Gets a given range of values from a circular buffer.

    Args:
      raw: the buffer to slice.
      begin: first position, inclusive.
      end: last position, inclusive.

    Returns:
      An indexable of the same type as raw.
    """
    if len(raw) < end - begin:
        return raw

    begin %= len(raw)
    end %= len(raw)
    if end < begin:
        # === === === END]       [BEG === === === ...
        return raw[begin:] + raw[:end+1]
    else:
        #            [BEG === === END]            ...
        return raw[begin:end+1]


class MetricData(object):
    """Perform computation on the data of a given Metric"""

    __slots__ = (
        "__aggregator", "__expired", "__retention", "__raw", "last_update",
    )

    _RAW_SIZE = 16
    _MAX_STAGE = 8

    def __init__(self, aggregator, retention):
        """This should be called directly but through RetentionBuffer instead."""
        self.last_update = 0
        self.__aggregator = aggregator
        self.__expired = array.array("d", [_NAN]*self._MAX_STAGE)
        self.__retention = retention
        # TODO: Allocate mmap()ed ram in Buffer
        self.__raw = array.array("d", [_NAN] * self._RAW_SIZE)

    # first stage must have at least 16 points
    # stages after the first may not include

    def compute_downsampled(self, epoch):
        """Compute one value for each stage of the metric."""
        # if ts is too old, we cannot accept the point
        previous_stage0_epoch  = self.current_stage0_epoch -1
        previous_stage0_raw, current_stage0_raw = self._split_raw()
        if epoch == self.current_stage0_epoch:
            epoch_raw = current_stage0_raw
            downsampled = [_NAN] * self._RAW_SIZE
        elif epoch == previous_stage0_epoch:
            epoch_raw = previous_stage0_raw
            downsampled = self.__expired
        else:
            return None

        epoch_value = self.__aggregator.downsample(epoch_raw, newest_first=True)
        epoch_weight = len(epoch_raw)
        if not epoch_weight:
            return self.__expired

        res = self.__expired[:]
        for n, stage in enumerate(self.__retention[1:]):
            res[n] = self.__aggregator.merge(
                old=downsampled[n],
                old_weight=stage.points/epoch_weight,
                fresh=epoch_value,
            )
        return res

    @property
    def current_stage0_epoch(self):
        stage0 = self.__retention[0]
        return stage0.epoch(self.last_update)

    def _split_raw(self):
        if len(self.__retention.stages) < 2:
            return array.array("d")
        first = self.__retention[0]
        second = self.__retention[1]  # first downsampled stage

        begin_ts = second.round_down(self.last_update)
        end_ts = self.last_update

        begin_pos = first.epoch(begin_ts)
        end_pos = first.epoch(end_ts)
        print("POS", begin_pos, end_pos)
        # previous is after the split, its values correspond to older points
        previous = slice_circular_buffer(self.__raw, end_pos+1, begin_pos-1)
        # current is before the split, its values correspond to newer points
        current = slice_circular_buffer(self.__raw, begin_pos, end_pos)
        return previous, current

    def put(self, ts, value):
        """Record a value for a timestamp.

        Return:
          The new value for each stage, or None if the timestamp is too old.
        """
        ts_stage0_epoch = self.__retention[0].epoch(ts)
        previous_stage0_epoch = self.current_stage0_epoch -1
        # If ts is older than the previous period
        if ts_stage0_epoch < previous_stage0_epoch:
            return None

        # Clear __raw if needed
        clear_from = max(self.current_stage0_epoch + 1, ts_stage0_epoch - self._RAW_SIZE) % self._RAW_SIZE
        clear_to = ts_stage0_epoch % self._RAW_SIZE
        expired_raw = array.array("d")#TODO [prev:new]+[new:prev]
        pos = clear_from
        while pos != clear_to:
            expired_raw.append(self.__raw[pos])
            self.__raw[pos] = _NAN
            pos = (pos + 1) % self._RAW_SIZE
        self.__raw[pos] = value

        # Aggregate expired data
        expired_value = self.__aggregator.downsample(expired_raw, newest_first=True)
        for n, stage in enumerate(self.__retention.downsampled_stages):
            self.__expired[n] += expired_value#TODO

        # Clear __expired if it is older than the previous period
        if ts_stage0_epoch - 1 > previous_stage0_epoch:
            for n in xrange(len(self.__retention.downsampled_stages)):
                self.__expired[n] = _NAN

        if ts > self.last_update:
            self.last_update = ts
        return self.compute_downsampled(ts_stage0_epoch)



class RetentionBuffer(object):
    """In memory buffer of recent point. """
    # TODO: Make this a mmap()'d buffer instead, indexed by offsets stored
    # in a trie.

    def __init__(self):
        self.__names_to_data = {}
        self.__interned_retentions = {}

    def get_metric_data(self, metric):
        if not metric.name in self.__names_to_data:
            retention = self.__interned_retentions.setdefault(
                metric.retention, metric.retention)
            data = MetricData(aggregator=metric.aggregator, retention=retention)
            self.__names_to_data[metric.name] = data
            return data
        return self.__names_to_data[metric.name]

    def update(self, metric, ts, value):
        metric_view = self.get_metric_data(metric)
        # Reset points that should be ignored.
        return metric_view.put(ts, value)
