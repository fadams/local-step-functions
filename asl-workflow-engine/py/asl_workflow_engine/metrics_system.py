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
The "official" prometheus Python client provides metrics for Standard Exports
such as gc, cpu and memory, but aioprometheus does not provide these by default. 
This module provides equivalent system metrics for aioprometheus.
"""

import sys
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3

import gc, os, platform
from aioprometheus import Counter, Gauge

try:
    import resource
    _PAGESIZE = resource.getpagesize()
except ImportError:
    # Not Unix
    _PAGESIZE = 4096


class SystemMetrics(object):
    """
    This class provides aioprometheus with equivalent system level metrics
    to those found in the "official" prometheus Python client.
    https://github.com/prometheus/client_python/blob/master/prometheus_client/process_collector.py
    https://github.com/prometheus/client_python/blob/master/prometheus_client/platform_collector.py
    https://github.com/prometheus/client_python/blob/master/prometheus_client/gc_collector.py
    """
    def __init__(self):
        self._ticks = 100.0
        try:
            self._ticks = os.sysconf('SC_CLK_TCK')
        except (ValueError, TypeError, AttributeError):
            pass

        # This is used to test if we can access /proc.
        self._btime = 0
        try:
            self._btime = self._boot_time()
        except IOError:
            pass

        major, minor, patchlevel = platform.python_version_tuple()
        info = {
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "major": major,
            "minor": minor,
            "patchlevel": patchlevel
        }

        self.process_metrics = {
            "info": Gauge(
                "python_info",
                "Python platform information."
            ),
            "vmem": Gauge(
                "process_virtual_memory_bytes",
                "Virtual memory size in bytes."
            ),
            "rss": Gauge(
                "process_resident_memory_bytes",
                "Resident memory size in bytes."
            ),
            "start_time": Gauge(
                "process_start_time_seconds",
                "Start time of the process since unix epoch in seconds."
            ),
            "cpu": Counter(
                "process_cpu_seconds_total",
                "Total user and system CPU time spent in seconds."
            ),
            "open_fds": Gauge(
                "process_open_fds",
                "Number of open file descriptors."
            ),
            "max_fds": Gauge(
                "process_max_fds",
                "Maximum number of open file descriptors."
            )
        }

        # Only include these metrics if CPython and gc supports get_stats
        if hasattr(gc, 'get_stats') and platform.python_implementation() == 'CPython':
            self.process_metrics["collected"] = Counter(
                "python_gc_objects_collected",
                "Objects collected during gc."
            )
            self.process_metrics["uncollectable"] = Counter(
                "python_gc_objects_uncollectable",
                "Uncollectable object found during GC."
            )
            self.process_metrics["collections"] = Counter(
                "python_gc_collections",
                "Number of times this generation was collected."
            )

        self.process_metrics["info"].set(info, 1.0)

    def _boot_time(self):
        with open("/proc/stat", 'rb') as stat:
            for line in stat:
                if line.startswith(b'btime '):
                    return float(line.split()[1])

    def values(self):
        return self.process_metrics.values()

    def collect(self):
        """
        Update the metrics from the latest system info in /proc.
        Although this method uses open and read it shouldn't pose any blocking
        issues for asyncio as we are only accessing the in-memory procfs
        https://en.wikipedia.org/wiki/Procfs
        """
        if not self._btime:
            return

        try:
            with open("/proc/self/stat", 'rb') as stat:
                parts = (stat.read().split(b')')[-1].split())

            self.process_metrics["vmem"].set("", float(parts[20]))
            self.process_metrics["rss"].set("", float(parts[21]) * _PAGESIZE)
            start_time_secs = float(parts[19]) / self._ticks
            self.process_metrics["start_time"].set(
                "", start_time_secs + self._btime
            )
            utime = float(parts[11]) / self._ticks
            stime = float(parts[12]) / self._ticks
            self.process_metrics["cpu"].set("", utime + stime)
        except IOError:
            pass

        try:
            with open("/proc/self/limits", 'rb') as limits:
                for line in limits:
                    if line.startswith(b'Max open file'):
                        self.process_metrics["max_fds"].set(
                            "", float(line.split()[3])
                        )
                        break

            self.process_metrics["open_fds"].set(
                "", float(len(os.listdir("/proc/self/fd")))
            )
        except (IOError, OSError):
            pass

        # Update gc metrics if enabled.
        if "collected" in self.process_metrics:
            for generation, stat in enumerate(gc.get_stats()):
                generation = {"generation": str(generation)}
                self.process_metrics["collected"].set(
                    generation, stat["collected"]
                )
                self.process_metrics["uncollectable"].set(
                    generation, stat["uncollectable"]
                )
                self.process_metrics["collections"].set(
                    generation, stat["collections"]
                )

