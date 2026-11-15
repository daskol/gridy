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

"""Execute objectives in child processes while renewing leases in asyncio.

Routine `search` runs one trial at a time. Both synchronous and asynchronous
objectives execute in a fresh process started with spawn, so even CPU-bound
coroutine bodies cannot block the coordinator connection. Objectives must be
importable, picklable callables, typically module-level functions.
"""

import math
import multiprocessing
import traceback
from asyncio import (
    FIRST_COMPLETED, create_task, gather, get_running_loop, run, sleep,
    timeout_at, to_thread, wait)
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from typing import Any

from gridy.client import Assignment, AsyncQueueClient, Done, Wait

__all__ = ('LeaseLostError', 'ObjectiveError', 'ObjectiveFn', 'search')

type ObjectiveFn = Callable[[dict[str, Any]], float | Awaitable[float]]


class ObjectiveError(RuntimeError):
    """The objective raised an exception or its process exited unexpectedly."""


class LeaseLostError(RuntimeError):
    """The current lease expired without a confirmed renewal."""


async def _invoke(objective: ObjectiveFn, params: dict[str, Any]) -> float:
    value = objective(params)
    if isinstance(value, Awaitable):
        return await value
    return value


def _objective_process(objective: ObjectiveFn, params: dict[str, Any],
                       connection: Connection) -> None:
    try:
        value = run(_invoke(objective, params))
        connection.send((True, value))
    except BaseException:
        connection.send((False, traceback.format_exc()))
    finally:
        connection.close()


async def _close_process(process: BaseProcess) -> None:
    if process.pid is not None:
        # Give a finished child time to exit. Kill a cancelled or stuck one.
        await to_thread(process.join, 0.2)
        if process.is_alive():
            process.kill()
            await to_thread(process.join)
    process.close()


async def _evaluate(objective: ObjectiveFn, params: dict[str, Any]) -> float:
    context = multiprocessing.get_context('spawn')
    reader, writer = context.Pipe(duplex=False)
    process = context.Process(target=_objective_process,
                              args=(objective, params, writer))
    try:
        try:
            process.start()
        except Exception as e:
            raise ObjectiveError(f'Cannot start objective: {e}.') from e
        writer.close()
        try:
            succeeded, value = await to_thread(reader.recv)
        except EOFError as e:
            raise ObjectiveError(
                    'Objective process exited without a result.') from e
        if not succeeded:
            raise ObjectiveError(value)
        return value
    finally:
        writer.close()
        await _close_process(process)
        reader.close()


async def _renew_lease(client: AsyncQueueClient,
                       assignment: Assignment) -> None:
    expires_at = assignment.lease_expires_at
    loop = get_running_loop()
    while True:
        remaining = (expires_at - datetime.now(UTC)).total_seconds()
        if remaining <= 0:
            raise LeaseLostError(f'Lease expired for {assignment.name}.')
        deadline = loop.time() + remaining
        await sleep(remaining / 3)
        try:
            async with timeout_at(deadline):
                expires_at = await client.renew_lease(
                    assignment.name, assignment.attempt_id)
        except TimeoutError as e:
            raise LeaseLostError(
                f'Lease renewal timed out for {assignment.name}.') from e


async def _execute(client: AsyncQueueClient, objective: ObjectiveFn,
                   assignment: Assignment) -> None:
    try:
        value = await _evaluate(objective, assignment.params)
    except ObjectiveError as e:
        await client.fail(assignment.name, assignment.attempt_id, str(e))
        return
    await client.complete(assignment.name, assignment.attempt_id,
                          {'objective': value})


async def _run_assignment(client: AsyncQueueClient, objective: ObjectiveFn,
                          assignment: Assignment) -> None:
    if assignment.lease_expires_at <= datetime.now(UTC):
        raise LeaseLostError(f'Lease expired for {assignment.name}.')
    execution = create_task(_execute(client, objective, assignment))
    heartbeat = create_task(_renew_lease(client, assignment))
    try:
        await wait((execution, heartbeat),
                           return_when=FIRST_COMPLETED)
        if execution.done():
            await execution
        else:
            await heartbeat
    finally:
        execution.cancel()
        heartbeat.cancel()
        await gather(execution, heartbeat, return_exceptions=True)


async def search(fn: ObjectiveFn, sweep_id: str, *,
                 client: AsyncQueueClient | None = None,
                 poll_interval: float = 5) -> None:
    """Acquire and evaluate trials with background lease renewal.

    Results are sent as `{"objective": value}`. A heartbeat runs until
    `Complete` or `Fail` finishes, renewing after one third of the remaining
    lease time. Renewal requests are bounded by the last confirmed lease
    deadline. Cancellation and lost leases stop and reap the objective process
    before returning. Kwarg `poll_interval` controls the asynchronous pause
    after `Wait`.

    Objective exceptions are reported through `Fail` before acquiring more
    work. Transport or heartbeat failures stop the search and its active child
    process. If client is omitted, search creates and closes a queue client
    with default settings. A supplied client remains owned by the caller.
    """
    if client is not None:
        await _search(fn, sweep_id, client=client, poll_interval=poll_interval)
        return

    async with AsyncQueueClient() as cli:
        await _search(fn, sweep_id, client=cli, poll_interval=poll_interval)


async def _search(fn: ObjectiveFn, sweep_id: str, *,
                 client: AsyncQueueClient, poll_interval: float) -> None:
    if not math.isfinite(poll_interval) or poll_interval <= 0:
        raise ValueError('poll_interval must be positive and finite')
    while True:
        match await client.acquire(sweep_id):
            case Assignment() as assignment:
                await _run_assignment(client, fn, assignment)
            case Wait():
                await sleep(poll_interval)
            case Done():
                return
