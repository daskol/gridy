# Copyright 2026 Daniel Bershatsky
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

"""Execute reusable objectives in process A and manage the controller.

The objective runs in the thread executing `search`. Context checks poll
cancellation messages without blocking, while the controller in process B
handles IPC and the coordinator API in its own event loop.
"""

import asyncio
import json
import math
import multiprocessing
import socket
import traceback
from asyncio import ensure_future, gather, get_running_loop, sleep
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Protocol, Self

from gridy.client import Assignment, AsyncQueueClient
from gridy.context import Context
from gridy.controller import ClientFactory, Controller
from gridy.errors import (
    CancelledError, LeaseLostError, ObjectiveError, WorkerError)
from gridy.ipc import Channel
from gridy.messages import Acknowledged, Cancel, Error, Finished, Outcome, Stop

__all__ = ('CancelledError', 'Context', 'LeaseLostError', 'Objective',
           'ObjectiveError', 'ObjectiveFn', 'WorkerError', 'search')


class Objective(Protocol):
    """A reusable objective executing in the thread running search."""

    def __call__(self, ctx: Context,
                 params: dict[str, Any]) -> dict[str, Any]: ...


type ObjectiveFn = Callable[
    [Context, dict[str, Any]], dict[str, Any] | Awaitable[dict[str, Any]]]


@dataclass
class _Invocation:
    assignment: Assignment
    context: Context


class Worker:
    """Own the local objective, IPC, and queue process for one search."""

    def __init__(self, objective: ObjectiveFn, sweep_id: str,
                 factory: ClientFactory, poll_interval: float):
        self.objective = objective
        self.loop = get_running_loop()
        self.events: deque[Any] = deque()
        self.context: Context | None = None
        self.error: Error | None = None
        self.finished = False
        mp_context = multiprocessing.get_context('spawn')
        parent_conn, self.child_conn = socket.socketpair()
        self.connection = Channel(parent_conn)
        args = (factory, sweep_id, poll_interval, self.child_conn)
        # The spawn target must not capture this object or its objective.
        self.proc = mp_context.Process(
            target=Controller.entrypoint, args=args)

    async def __aenter__(self) -> Self:
        try:
            self._start()
        except BaseException:
            await self._shutdown()
            raise
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None,
                        exc: BaseException | None,
                        tb: TracebackType | None) -> None:
        if exc is not None:
            self._cancel('cancelled')
        await self._shutdown()

    def _start(self) -> None:
        try:
            self.proc.start()
        except Exception as e:
            raise WorkerError(f'Cannot start queue process: {e}.') from e
        self.child_conn.close()

    async def _shutdown(self) -> None:
        self._detach_context()
        self.child_conn.close()
        try:
            if self.proc.pid is not None:
                with suppress(OSError, TimeoutError):
                    async with asyncio.timeout(1):
                        await self.connection.send(Stop())
            await self._close_process()
        finally:
            self.connection.close()

    async def _join(self, timeout: float | None = None) -> bool:
        deadline = None if timeout is None else self.loop.time() + timeout
        while self.proc.is_alive():
            if deadline is not None and self.loop.time() >= deadline:
                return False
            await sleep(0.01)
        self.proc.join(0)
        return True

    async def _close_process(self) -> None:
        if self.proc.pid is not None:
            if not await self._join(1):
                # Only the network process is managed here, never the executor.
                self.proc.terminate()
            if not await self._join(1):
                self.proc.kill()
                await self._join()
        self.proc.close()

    def _cancel(self, reason: str) -> None:
        if self.context is not None:
            self.context.cancel(reason)

    def _detach_context(self) -> None:
        if self.context is not None:
            self.context._cancellation.poll = None
            self.context = None

    async def _respond(self, response: Outcome) -> None:
        self._poll()
        if self.error is not None:
            raise self.error.exception()
        try:
            await self.connection.send(response)
        except OSError as e:
            raise WorkerError('Queue process disconnected.') from e

    def _disconnected(self, error: Exception) -> None:
        self._cancel('worker_failed')
        self.error = Error(
            'WorkerError', f'Queue process exited without finishing: {error}')
        self.events.append(self.error)
        self.finished = True

    def _poll(self) -> None:
        """Poll control messages without waiting for incomplete frames."""
        while not self.finished:
            try:
                message = self.connection.recv_nowait()
            except BlockingIOError:
                return
            except (EOFError, OSError, ValueError) as e:
                self._disconnected(e)
                return
            self._dispatch(message)

    async def _receive(self) -> Any:
        while not self.events:
            try:
                message = await self.connection.recv()
            except (EOFError, OSError, ValueError) as e:
                self._disconnected(e)
                break
            self._dispatch(message)
        return self.events.popleft()

    def _dispatch(self, message: Any) -> None:
        match message:
            case Assignment():
                self._detach_context()
                self.context = Context(message.name, message.attempt_id)
                self.context._cancellation.poll = self._poll
                message = _Invocation(message, self.context)
            case Cancel() | Acknowledged():
                self._update_context(message)
                return
            case Error():
                self.error = message
                self._cancel('worker_failed')
                self.finished = True
            case Finished():
                self.finished = True
        self.events.append(message)

    def _update_context(self, message: Cancel | Acknowledged) -> None:
        context = self.context
        if context is None:
            return
        if ((context.name, context.attempt_id)
                != (message.name, message.attempt_id)):
            return
        if isinstance(message, Cancel):
            context.cancel(message.reason)
            return
        self._detach_context()

    async def _execute(self, invocation: _Invocation) -> Outcome:
        assignment, context = invocation.assignment, invocation.context
        response = Outcome(assignment.name, assignment.attempt_id)
        try:
            context.raise_if_cancelled()
            value = self.objective(context, assignment.params)
            if isinstance(value, Awaitable):
                execution = ensure_future(value)
                try:
                    value = await asyncio.shield(execution)
                except asyncio.CancelledError:
                    context.cancel('cancelled')
                    execution.cancel()
                    await gather(execution, return_exceptions=True)
                    raise
            context.raise_if_cancelled()
            if not isinstance(value, dict):
                raise ObjectiveError('Objective result must be a JSON object.')
            # Validate before IPC and snapshot mutable objective state.
            response.result = json.loads(json.dumps(value, allow_nan=False))
        except CancelledError as e:
            context.cancel(e.reason)
        except Exception:
            response.error = traceback.format_exc()
        if context.cancelled:
            response.result = None
            response.error = None
            response.cancelled = context.reason
        return response

    async def run(self) -> None:
        while True:
            match await self._receive():
                case _Invocation() as invocation:
                    try:
                        response = await self._execute(invocation)
                    finally:
                        # Async receiving resumes after invocation. Retained
                        # contexts must not start another socket reader.
                        invocation.context._cancellation.poll = None
                    await self._respond(response)
                case Error() as e:
                    raise e.exception()
                case Finished():
                    return
                case _:
                    raise WorkerError('Unexpected message from queue process.')


async def search(fn: ObjectiveFn, sweep_id: str, *,
                 client_factory: ClientFactory | None = None,
                 poll_interval: float = 5) -> None:
    """Run objectives sequentially in the thread running this coroutine.

    Objectives receive `(ctx, params)` and return a JSON object of metrics.
    Callables and their resources stay local and may be reused between trials.
    Coroutine objectives are also supported and execute in this same thread.

    One spawned background process creates and owns the queue client. Supply
    a picklable, zero-argument `client_factory` to configure it, for example
    `functools.partial(AsyncQueueClient, endpoint, token=token)`. Live clients
    and their connections must not be captured by that factory. Use a main
    guard in scripts, as with other multiprocessing programs using spawn.

    Renewal continues through completion/failure acknowledgement. Lease loss
    cancels the context with reason `lease_lost`, rejects stale results/errors,
    and raises the lease/transport error after the invocation ends.
    Cancellation is cooperative and has no bounded cleanup time. Search never
    terminates the calling process. Explicit context cancellation raises
    CancelledError without submitting a completion or failure.
    """
    if not math.isfinite(poll_interval) or poll_interval <= 0:
        raise ValueError('poll_interval must be positive and finite')
    factory = AsyncQueueClient if client_factory is None else client_factory
    async with Worker(fn, sweep_id, factory, poll_interval) as worker:
        await worker.run()
