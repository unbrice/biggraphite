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
"""Functions currently used by the Cassandra driver but not specific to it."""
from __future__ import absolute_import
from __future__ import print_function

import threading

from biggraphite import accessor as bg_accessor


class Error(bg_accessor.Error):
    """Base class for all exceptions from this module."""

class CountDown(object):
    """Decrements a count, calls a callback when it reaches 0.

    This is used to wait for queries to complete without storing & sorting their results.
    """

    __slots__ = ("_canceled", "_count", "_lock", "_on_zero", )

    def __init__(self, count, on_zero):
        """Record parameters."""
        self._canceled = False
        self._count = count
        self._lock = threading.Lock()
        self._on_zero = on_zero

    def cancel(self, reason):
        """Call the callback now with reason as argument."""
        with self._lock:
            if self._canceled:
                return
            self._canceled = True
            self._on_zero(reason)

    def decrement(self):
        """Call the callback if count reached zero, with None as argument."""
        with self._lock:
            self._count -= 1
            if self._canceled:
                return
            elif not self._count:
                self._on_zero(None)

    def on_cassandra_result(self, unused_result):
        """Call decrement(), suitable for Cassandra's execute_async."""
        self.decrement()

    def on_cassandra_failure(self, exc):
        """Call cancel(), suitable for Cassandra's execute_async."""
        self.cancel(Error(exc))


class ShardedFloat(object):
    """Encodes floats as three 64 bit integers.

    On each integer, we only use 35 bits for digits, 1 for sign. This allows for
    2^28 additions without risking overflow.

    First int has exponent 2^93 and encodes [2^93, 2^128[
    Second int has exponent 2^58 and encodes [2^58, 2^93[
    Third int has exponent 2^23 and encodes [2^23, 2^58[
    Fourth int has exponent 2^-12 and encodes [2^-12, 2^23[
    """

    __slots__ = ("i0", "i1", "i2", "i3", )

    SHARD_WIDTH = 35
    MAX_SAFE_ADD = 2 ** (64 - 1 - SHARD_WIDTH)
    SHARD_MAX = 2 ** SHARD_WIDTH
    EXP3 = -16
    EXP2 = 19
    EXP1 = 54
    EXP0 = 89
    MIN_ABS = 2 ** EXP3
    MAX_ABS = 2 ** (EXP0 + SHARD_WIDTH) - 1
    assert EXP2 == EXP3 + SHARD_WIDTH
    assert EXP1 == EXP2 + SHARD_WIDTH
    assert EXP0 == EXP1 + SHARD_WIDTH

    # simplify() discards least significant shard values less than this
    # the rational is that unless we do more than MAX_SAFE_ADD
    # additions, they will amount to less than 0.5%
    SIMPLIFY_SHARD_IMPRECISION = 2 ** SHARD_WIDTH / 200

    def __init__(self, i0, i1, i2, i3):
        self.i0 = int(i0)
        self.i1 = int(i1)
        self.i2 = int(i2)
        self.i3 = int(i3)

    def approx_add(self, other):
        return ShardedFloat(
            self.i0+other.i0,
            self.i1+other.i1,
            self.i2+other.i2,
            self.i3+other.i3,
        )

    @classmethod
    def from_float(cls, f):
        """Build a ShardedFloat from a float."""
        sign = 1
        if f < 0:
            f = -f
            sign = -1
        if f > cls.MAX_ABS:
            f = cls.MAX_ABS
        if f < cls.MIN_ABS:
            f = cls.MIN_ABS
        i0 = int(f / 2 ** cls.EXP0)
        i1 = int(f / 2 ** cls.EXP1) % cls.SHARD_MAX
        i2 = int(f / 2 ** cls.EXP2) % cls.SHARD_MAX
        i3 = int(f * 2 ** -cls.EXP3) % cls.SHARD_MAX
        return cls(i0*sign, i1*sign, i2*sign, i3*sign)

    @property
    def simplified(self):
        """Discard least significant shards less than SIMPLIFY_SHARD_IMPRECISION."""
        i0 = self.i0
        i1 = self.i1
        i2 = self.i2
        i3 = self.i3
        if i0:
            if i1 < self.SIMPLIFY_SHARD_IMPRECISION:
                i1 = 0
            i2 = 0
            i3 = 0
        elif i1:
            if i2 < self.SIMPLIFY_SHARD_IMPRECISION:
                i2 = 0
            i3 = 0
        elif i2:
            if i3 < self.SIMPLIFY_SHARD_IMPRECISION:
                i3 = 0

        return ShardedFloat(i0, i1, i2, i3)

    @property
    def as_float(self):
        """Build a float from a ShardedFloat."""
        return (
            self.i0 * 2.0 ** self.EXP0 +
            self.i1 * 2.0 ** self.EXP1 +
            self.i2 * 2.0 ** self.EXP2 +
            self.i3 * 2.0 ** self.EXP3
        )
