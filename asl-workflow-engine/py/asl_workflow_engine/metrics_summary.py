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
With aioprometheus if you want a Summary metric the "obvious" thing to do is to
use the Summary class. The problem with that however is that it directly uses
the quantile.Estimator, which is an implementation of the Cormode, Korn,
Muthukrishnan, and Srivastava algorithm for streaming calculation of targeted
high-percentile epsilon-approximate quantiles. That Estimator retains *all*
observations in a linked list, so what happens is that over time the insert
and query time linearly degrades and gradually degrades the performance of the
application being metricated, which is far from ideal.


The "official" prometheus Python client doesn't store or expose quantiles at all.
https://github.com/prometheus/client_python#summary
This module provides a BasicSummary class that extends aioprometheus Summary to
provide a simple sum + observation count implementation as per the "official"
prometheus Python client.


The "official" prometheus Java client however does expose quantiles, but the
approach that it uses is to create a wrapper around CKMSQuantiles that maintains
a ring buffer of CKMSQuantiles to provide quantiles over a sliding time window.
https://github.com/prometheus/client_java/blob/master/simpleclient/src/main/java/io/prometheus/client/TimeWindowQuantiles.java

This module extends aioprometheus Summary with a TimeWindowSummary that
uses a custom time window Estimator.
"""

import sys
assert sys.version_info >= (3, 0)  # Bomb out if not running Python3

import time, quantile
from aioprometheus import Summary

class TimeWindowEstimator(object):
    """
    Estimator estimates quantile values from sample streams in a time- and
    memory-efficient manner subject to allowed error constraints.

    The Estimator basically follows the same API as quantile.Estimator but
    follows the pattern of the Java Summary, maintaining a ring buffer of
    quantile.Estimators to provide quantiles over a sliding windows of time.
    """
    def __init__(self, *quantiles, max_age_seconds=600, age_buckets=5):
        """Initialize an Estimator.

        Estimator is not concurrency safe.

        Args:
            quantiles: A list of floating point doubles containing the target
                quantile value and allowed error.   [(0.5, 0.01), (0.99, 0.001)]
                are the default if none are provided, signifying that the median
                will be provided at a one percent error limit and the 99th
                percentile at the a 0.1 percent error limit.
            max_age_seconds: The duration of the time window is, i.e. how long
                observations are kept before they are discarded.
                Default is 10 minutes.
            age_buckets: The number of buckets used to implement the sliding
                time window. If your time window is 10 minutes, and you have
                age_buckets=5, buckets will be switched every 2 minutes. The
                value is a trade-off between resources (memory and cpu for
                maintaining the bucket) and how smooth the time window is moved.
                Default value is 5.
        """
        self._quantiles = quantiles
        self._invariants = [quantile._Quantile(q, e) for (q, e) in quantiles]
        self._observations = 0
        self._sum = 0
        self._ring_buffer = []

        for i in range(age_buckets):
            self._ring_buffer.append(quantile.Estimator(*quantiles))

        self._current_bucket = 0
        self._last_rotate_timestamp_millis = time.time() * 1000
        self._duration_between_rotates_millis = (max_age_seconds  * 1000) / age_buckets

    def observe(self, value):
        """
        Samples an observation's value.

        Args:
            value: A numeric value signifying the value to be sampled.
        """
        self._observations += 1
        self._sum += value
        self._rotate()
        for e in self._ring_buffer:
            e.observe(float(value))

    def query(self, rank):
        """
        Retrieves the value estimate for the requested quantile rank.

        The requested quantile rank must be registered in the estimator's
        invariants a priori!

        Args:
            rank: A floating point quantile rank along the interval [0, 1].

        Returns:
            A numeric value for the quantile estimate.
        """
        current = self._rotate()
        if current._observations:
            return current.query(rank)
        else:
            """
            For the case where the current quantile.Estimator has received no
            observations, as would be the case when query is called after a
            period of no observations and an "empty" quantile.Estimator is
            rotated to the head of the ring buffer, it is unclear what the
            'best' value to return is. The Java CKMSQuantiles get() returns NaN
            https://github.com/prometheus/client_java/blob/master/simpleclient/src/main/java/io/prometheus/client/CKMSQuantiles.java
            so we do that here for consistency, however a better approach might
            be to record a reference "tail" of the ring buffer each time an
            observation is made and query that quantile.Estimator.
            """
            return float("NaN")

    def _rotate(self):
        """
        rotate the ring buffer when the time threshold has been exceeded.
        Returns:
            The quantile.Estimator from the current time window bucket.
        """
        time_since_last_rotate_millis = time.time() * 1000 - self._last_rotate_timestamp_millis

        while time_since_last_rotate_millis > self._duration_between_rotates_millis:
            self._ring_buffer[self._current_bucket] = quantile.Estimator(*self._quantiles)

            self._current_bucket += 1
            if self._current_bucket >= len(self._ring_buffer):
                self._current_bucket = 0

            time_since_last_rotate_millis -= self._duration_between_rotates_millis
            self._last_rotate_timestamp_millis += self._duration_between_rotates_millis

        return self._ring_buffer[self._current_bucket]


class TimeWindowSummary(Summary):
    """
    Extend aioprometheus Summary to use our custom time window Estimator.
    """
    def observe(self, labels, value):
        """
        Add a single observation to the summary. It is basically the same as
        aioprometheus Summary add/observe, but delegates to our custom time
        window Estimator rather than directly using quantile.Estimator.
        """
        if type(value) not in (float, int):
            raise TypeError("Summary only works with digits (int, float)")

        try:
            e = self.get_value(labels)
        except KeyError:
            # Initialize quantile estimator
            e = TimeWindowEstimator(*self.invariants)
            self.set_value(labels, e)

        e.observe(float(value))  # type: ignore


class BasicEstimator(object):
    """
    The Estimator basically follows the same API as quantile.Estimator but
    follows the pattern of the basic "official" prometheus client Summary
    to provide a simple summary comprising sum plus observation count.
    """
    def __init__(self):
        """Initialize an Estimator.
        """
        self._invariants = []  # Deliberately empty, but required by Summary.get()
        self._observations = 0
        self._sum = 0

    def observe(self, value):
        """
        Samples an observation's value.

        Args:
            value: A numeric value signifying the value to be sampled.
        """
        self._observations += 1
        self._sum += value

class BasicSummary(Summary):
    """
    Extend aioprometheus Summary to use our basic Estimator.
    """
    def observe(self, labels, value):
        """
        Add a single observation to the summary. It is basically the same as
        aioprometheus Summary add/observe, but delegates to our custom basic
        Estimator rather than directly using quantile.Estimator.
        """
        if type(value) not in (float, int):
            raise TypeError("Summary only works with digits (int, float)")

        try:
            e = self.get_value(labels)
        except KeyError:
            e = BasicEstimator()
            self.set_value(labels, e)

        e.observe(float(value))  # type: ignore

