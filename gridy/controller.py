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

"""Control trial acquisition, leases, and submission in process B."""

import signal
import socket
from asyncio import (
    FIRST_COMPLETED, Queue, Task, create_task, gather, get_running_loop, run,
    sleep, timeout_at, wait)
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime

from gridy.client import (
    Assignment, AsyncQueueClientProtocol, Done, ProtocolError, Wait)
from gridy.errors import CancelledError, LeaseLostError, WorkerError
from gridy.ipc import Channel
from gridy.messages import Acknowledged, Cancel, Error, Finished, Outcome, Stop

__all__ = ('ClientFactory', 'Controller')

type ClientFactory = Callable[[], AsyncQueueClientProtocol]


class Controller:
    """Own queue operations and IPC in the background process."""

    @staticmethod
    def entrypoint(factory: ClientFactory, sweep_id: str,
                   poll_interval: float, connection: socket.socket) -> None:
        """Run the controller in a spawned process."""
        # The calling process handles Ctrl-C and requests orderly shutdown.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        run(Controller.serve(factory, sweep_id, poll_interval, connection))

    def __init__(self, client: AsyncQueueClientProtocol, sweep_id: str,
                 poll_interval: float, connection: Channel):
        self.client = client
        self.sweep_id = sweep_id
        self.poll_interval = poll_interval
        self.connection = connection
        self.loop = get_running_loop()
        self.assignment: Assignment | None = None
        self.lease_deadline = 0.0
        self.lease_error: Exception | None = None
        self.outcomes: Queue[Outcome] = Queue()

    @classmethod
    async def serve(cls, factory: ClientFactory, sweep_id: str,
                    poll_interval: float, connection: socket.socket) -> None:
        channel = Channel(connection)
        try:
            async with factory() as client:
                controller = cls(client, sweep_id, poll_interval, channel)
                await controller.run()
            await channel.send(Finished())
        except BaseException as e:
            with suppress(OSError):
                await channel.send(Error.capture(e))
        finally:
            channel.close()

    async def _receive(self) -> None:
        try:
            while True:
                message = await self.connection.recv()
                if isinstance(message, Stop):
                    return
                if not isinstance(message, Outcome):
                    raise WorkerError('Unexpected message from executor.')
                self.outcomes.put_nowait(message)
        except (EOFError, OSError):
            return

    def _update_lease(self, expires_at: datetime) -> None:
        remaining = (expires_at - datetime.now(UTC)).total_seconds()
        self.lease_deadline = self.loop.time() + remaining
        self._check_lease()

    def _check_lease(self) -> None:
        if self.lease_error is not None:
            raise self.lease_error
        if self.loop.time() < self.lease_deadline:
            return
        assert self.assignment is not None
        raise LeaseLostError(f'Lease expired for {self.assignment.name}.')

    async def _renew_lease(self) -> None:
        assignment = self.assignment
        assert assignment is not None
        try:
            while True:
                self._check_lease()
                remaining = self.lease_deadline - self.loop.time()
                await sleep(remaining / 3)
                expires_at = await self._request_renewal(assignment)
                self._check_lease()
                self._update_lease(expires_at)
        except Exception as e:
            self.lease_error = e
            raise

    async def _request_renewal(self, assignment: Assignment) -> datetime:
        try:
            async with timeout_at(self.lease_deadline):
                return await self.client.renew_lease(
                    assignment.name, assignment.attempt_id)
        except TimeoutError as e:
            raise LeaseLostError(
                f'Lease renewal timed out for {assignment.name}.') from e

    async def _submit(self, response: Outcome) -> None:
        # A heartbeat may fail between scheduling this task and its first turn.
        self._check_lease()
        if response.error is not None:
            await self.client.fail(response.name, response.attempt_id,
                                   response.error)
        else:
            assert response.result is not None
            await self.client.complete(response.name, response.attempt_id,
                                       response.result)

    async def _run_assignment(self, assignment: Assignment) -> None:
        self.assignment = assignment
        self.lease_error = None
        self._update_lease(assignment.lease_expires_at)
        heartbeat = create_task(self._renew_lease())
        outcome = create_task(self.outcomes.get())
        submission: Task[None] | None = None
        connection = self.connection
        try:
            await connection.send(assignment)
            await wait((outcome, heartbeat), return_when=FIRST_COMPLETED)
            if heartbeat.done():
                cancel = Cancel(
                    assignment.name, assignment.attempt_id, 'lease_lost')
                await connection.send(cancel)
                # The invocation must end, including cleanup, before returning.
                await outcome
                await heartbeat

            response = outcome.result()
            response_key = (response.name, response.attempt_id)
            assignment_key = (assignment.name, assignment.attempt_id)
            if response_key != assignment_key:
                raise WorkerError(
                    'Result does not match the current assignment.')
            if response.cancelled is not None:
                raise CancelledError(response.cancelled)
            self._check_lease()
            submission = create_task(self._submit(response))
            await wait((submission, heartbeat), return_when=FIRST_COMPLETED)

            # A terminal acknowledgement wins over a concurrent stale renewal
            # response caused by that very completion/failure.
            if submission.done():
                await submission
            else:
                await heartbeat
            await connection.send(Acknowledged(
                assignment.name, assignment.attempt_id))
        except Exception as e:
            # Stop in-flight API retries before a cancellation notification can
            # yield on IPC backpressure.
            if submission is not None:
                submission.cancel()
                await gather(submission, return_exceptions=True)
            if self.lease_error is not None or isinstance(e, LeaseLostError):
                await connection.send(Cancel(
                    assignment.name, assignment.attempt_id, 'lease_lost'))
            raise
        finally:
            tasks = [heartbeat, outcome]
            if submission is not None:
                tasks.append(submission)
            for task in tasks:
                task.cancel()
            await gather(*tasks, return_exceptions=True)

    async def _drive(self) -> None:
        while True:
            match await self.client.acquire(self.sweep_id):
                case Assignment() as assignment:
                    await self._run_assignment(assignment)
                case Wait():
                    await sleep(self.poll_interval)
                case Done():
                    return
                case _:
                    raise ProtocolError('Unexpected acquisition outcome.')

    async def run(self) -> None:
        driver = create_task(self._drive())
        reader = create_task(self._receive())
        try:
            await wait((driver, reader), return_when=FIRST_COMPLETED)
            if reader.done():
                await reader
            if driver.done():
                await driver
        finally:
            driver.cancel()
            reader.cancel()
            await gather(driver, reader, return_exceptions=True)
