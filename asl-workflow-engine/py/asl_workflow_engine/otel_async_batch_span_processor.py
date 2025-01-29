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
The default OpenTelemetry BatchSpanProcessor, which is the most common/useful
OpenTelemetry SpanProcessor implementation providing hooks for SDK’s Span start
and end method invocations, runs as a worker Thread which is inefficient for
asyncio applications.

This is an alternative AsyncBatchSpanProcessor where the worker runs as an
asyncio coroutine task rather than a Thread.

This implementation somewhat based on this Proof of Concept:
https://github.com/open-telemetry/opentelemetry-python/pull/3489
https://github.com/open-telemetry/opentelemetry-python/pull/3489/files

but also the threaded BatchSpanProcessor:
https://github.com/open-telemetry/opentelemetry-python/blob/main/opentelemetry-sdk/src/opentelemetry/sdk/trace/export/__init__.py

and the jaeger_client Reporter:
https://github.com/jaegertracing/jaeger-client-python/blob/master/jaeger_client/reporter.py

Hopefully in due course the official OpenTelemetry SDK will provide better
support for running asyncio workloads and we can then use those directly,
but for now some DIY seems prudent.
"""

import sys
assert sys.version_info >= (3, 9)  # Bomb out if not running Python3.9

import asyncio
import logging
import threading  # Needed for threading.Event
from typing import List, Optional
import os

from opentelemetry import context as context_api
from opentelemetry.sdk.trace import ReadableSpan, Span
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SpanExporter,
    SpanProcessor,
)

logger = logging.getLogger(__name__)

class AsyncBatchSpanProcessor(SpanProcessor):
    def __init__(
        self,
        span_exporter: SpanExporter,
        *,
        max_queue_size: Optional[int] = None,
        schedule_delay_millis: Optional[float] = None,
        max_export_batch_size: Optional[int] = None,
        export_timeout_millis: Optional[float] = None,
    ):
        if max_queue_size is None:
            max_queue_size = BatchSpanProcessor._default_max_queue_size()

        if schedule_delay_millis is None:
            schedule_delay_millis = (
                BatchSpanProcessor._default_schedule_delay_millis()
            )

        if max_export_batch_size is None:
            max_export_batch_size = (
                BatchSpanProcessor._default_max_export_batch_size()
            )

        if export_timeout_millis is None:
            export_timeout_millis = (
                BatchSpanProcessor._default_export_timeout_millis()
            )

        BatchSpanProcessor._validate_arguments(
            max_queue_size, schedule_delay_millis, max_export_batch_size
        )

        """
        This config is not in the threaded BatchSpanProcessor. Setting it to
        true directly calls self.span_exporter.export(spans) rather than using
        await asyncio.to_thread(self.span_exporter.export, spans) to launch
        via a ThreadPoolExecutor. Setting this causes AsyncBatchSpanProcessor
        to behave more like the Jaeger Client where self.agent.emitBatch(batch)
        is called directly.

        Doing this without care in an asyncio application _could_ block the
        event loop, however it is how Jaeger Client behaves and causes few/no
        issues likely as the batch size is small and the jaeger-thrift transport
        uses UDP.

        The default is false and it should only be set true if the exporter
        has been set to jaeger-thrift
        """
        sync_export = os.environ.get("OTEL_BSP_SYNC_EXPORT", "false")
        self.sync_export = str(sync_export).lower() not in ['false', '0', 'none']

        self.span_exporter = span_exporter
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        # A sentinel value that may be put on the queue to force a flush
        self.flush_marker = object()
        # Allows force_flush subroutine to block waiting for _force_flush
        # coroutine to complete.
        self.flush_event = threading.Event()

        self.worker_task: asyncio.Task[None]

        self.schedule_delay_millis = schedule_delay_millis
        self.max_export_batch_size = max_export_batch_size
        self.max_queue_size = max_queue_size
        self.export_timeout_millis = export_timeout_millis

        asyncio.get_event_loop().run_until_complete(self._start())

        #self.put_count = 0
        #self.drop_count = 0

    async def _start(self) -> None:
        self.worker_task = asyncio.get_event_loop().create_task(self._consume_queue())

    async def _enqueue(self, span: ReadableSpan) -> None:
        try:
            self.queue.put_nowait(span)
            #self.put_count+=1
            #print(f"put_count {self.put_count}")
        except asyncio.QueueFull:
            # drop the span
            #logger.info("Queue is full, dropping span!")
            #self.drop_count+=1
            #print(f"drop_count {self.drop_count}")
            pass

    async def _consume_queue(self) -> None:
        cancelled = False
        spans = []
        while not cancelled:
            while len(spans) < self.max_export_batch_size:
                try:
                    # A timeout allows for a periodic flush with smaller packets.
                    timeout = self.schedule_delay_millis/1000
                    span = await asyncio.wait_for(self.queue.get(), timeout=timeout)
                    # If self.flush_marker has been enqueued exit inner loop
                    # and export accumulated spans.
                    if span == self.flush_marker:
                        self.queue.task_done()
                        break
                    else:
                        spans.append(span)
                except asyncio.TimeoutError:
                    break
                except asyncio.CancelledError:
                    logger.info("Task _consume_queue cancelled")
                    cancelled = True
                    break

            if spans:
                await self._export_batch(spans)
                for _ in spans:
                    self.queue.task_done()
                spans = []

    async def _export_batch(self, spans: List[ReadableSpan]) -> None:
        # Send a batch of spans to the exporter set at construction time.
        if self.sync_export:
            self.span_exporter.export(spans)
        else:
            """
            Under the hood asyncio.to_thread uses loop.run_in_executor with the
            loop's default Executor. Unfortunately use of ThreadPoolExecutors
            can behave "strangely" in atexit hooks. See
            https://github.com/python/cpython/issues/86813

            One approach is to use a custom ThreadPoolExecutor in a context
            stack and do context_stack.close() in the exit handler to cleanly
            close the thread pool. A simpler approach, that lets us use
            to_thread here rather than explicitly using ThreadPoolExecutor, is
            to check if it fails with RuntimeError then simply try again using
            a direct (potentially blocking) call to span_exporter.export().
            """
            try:
                await asyncio.to_thread(self.span_exporter.export, spans)
            except RuntimeError:
                logging.warn(
                    "ThreadPoolExecutor was already shutdown, exporting "
                    "synchronously in event loop thread. This might be "
                    "running in an atexit hook."
                )
                self.span_exporter.export(spans)

    async def _shutdown(self) -> None:
        self.worker_task.cancel()

    async def _force_flush(self) -> None:
        await self.queue.put(self.flush_marker)
        await self.queue.join()  # await all enqueued items being processed
        self.flush_event.set()   # Notify/unblock force_flush() call

    # SpanProcessor API Calls

    def on_start(
        self, span: Span, parent_context: Optional[context_api.Context] = None,
    ) -> None:
        pass

    def on_end(self, span: ReadableSpan) -> None:
        asyncio.get_event_loop().create_task(self._enqueue(span))

    def shutdown(self) -> None:
        asyncio.get_event_loop().run_until_complete(self._shutdown())

    def force_flush(self, timeout_millis: int = None) -> bool:
        if timeout_millis is None:
            timeout_millis = self.export_timeout_millis

        loop = asyncio.get_event_loop()
        if not loop.is_running():
            logger.warning("Already shutdown, ignoring call to force_flush()")
            return True

        self.flush_event.clear()
        # Launch a coroutine to force any queued spans to be emitted and signal
        # self.flush_event once that has happened.
        loop.create_task(self._force_flush())
        # Wait for flush_event to be signalled by _force_flush().
        return self.flush_event.wait(timeout_millis/1000)

