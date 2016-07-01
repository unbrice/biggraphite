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
import codecs
import math
from os import path as os_path

from biggraphite import accessor as bg_accessor
from biggraphite.drivers import _utils as bg_drivers_utils


_NAN = float("nan")



class Error(bg_accessor.Error):
    """Base class for all exceptions from this module."""


class Downsampler(object):
    """Data for a given day of a given Retention.

    This is a shim based on whisper. Future versions may replace it with
    a more efficient datastructure.
    These buffers are treated as opaque memory and manipulated by the
    _MetricView instead.
    """

    def __init__(self, base_dir):
        self.base_dir = base_dir
        # The directory we actually use must be unique to the process, for that we
        # use mkdtemp. prefix and suffix are just to make it more clear
        # what the files are.
        today_str=datetime.datetime.utcnow().date().isoformat()
        self._dir = tempfile.mkdtemp(
            prefix="downsampling.%s.%d" % (today_str, os.getpid()),
            dir=base_dir,
        )

    def _fs_path_from_metric(self, metric):
        # Escape non-ascii characters
        metric_str = codecs.raw_unicode_escape_decode(metric.name)[0]
        metric_slashed = metric_str.lstrip(".").replace(".", os_path.sep)
        # include the number of dots in the path so that "a..b" and "a.b" are
        # different
        metric_dots = metric.name.count(".")
        metric_path = "%s.%d.wsp" % (metric_slashed, metric_dots)
        return os_path.join(self._dir, metric_path)

    def _create(self, fs_path, metric):
        if os.path.exists(fs_path):
            return
        whisper.create(
            path=fs_path,
            archiveList=metric.retention.as_carbon,
            xFilesFactor=0.0,
            aggregationMethod=metric.aggregation.carbon_name,
            sparse=True,
            useFallocate=False,
        )
    
    def update(self, metric, ts, value, None):
        fs_path = self._fs_path_from_metric(metric)
        self._create(fs_path, metric)
        res = []
        if not now:
            now = time.time()
        with open(path, "r+b") as file_:
            whisper.update_file(fh=file_, value=value, timestamp=ts)
            for n, stage in enumerate(metric.retention.stages):
                stage_ts = accessor.round_down(ts)
                stage_value = value
                if n:
                    stage_value = file_fetch(fh=file_, fromTime=stage_ts, untilTime=stage_ts, now=now)
                res.append(stage_ts, stage_value)
        return res

    @staticmethod
    def __is_pid_live(pid):
        """Return True iff the pid is live."""
        try:
            # Signal 0 is a no-op
            os.kill(pid, 0)
        except OSError as e:
            if e.errno == errno.ESRCH:
                # ESRCH is for "No such process"
                return False
        return True

    
    def clean_older(self):
        """Clean downsampling directories of unused metrics.

        Yields paths to the deleted directories.
        """
        dir_glob = os_path.join(self.base_dir, "downsampling.*-*-*.*")
        directories = glob.glob(dir_glob)
        for dirname in directories:
            pid_str = dirname.rsplit(".", 1)[-1]
            try:
                pid = int(pid_str)
            except ValueError:
                continue
            if self.__is_pid_live(pid):
                continue
            # We ignore errors as anothe process might be attempting
            # just the same at the same time
            shutil.rmtree(dirname, ignore_errors=True)
            yield dirname
