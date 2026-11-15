"""Verify cancellation ordering while IPC applies backpressure."""

import asyncio
import socket
from datetime import UTC, datetime, timedelta

import pytest

from gridy import APIError, Assignment
from gridy.controller import Controller
from gridy.ipc import Channel
from gridy.messages import Cancel, Outcome


class GatedChannel(Channel):
    """Delay cancellation notification without blocking the event loop."""

    def __init__(self, connection):
        super().__init__(connection)
        self.notifying = asyncio.Event()
        self.release = asyncio.Event()

    async def send(self, message):
        if isinstance(message, Cancel):
            self.notifying.set()
            await self.release.wait()
        await super().send(message)


class RetryingClient:
    """Keep terminal requests active until the controller cancels them."""

    def __init__(self):
        self.submitting = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.attempts = []

    async def renew_lease(self, name, attempt_id):
        await self.submitting.wait()
        raise APIError(409, 'stale attempt', code=10)

    async def _submit(self, kind):
        self.submitting.set()
        try:
            while True:
                self.attempts.append(kind)
                await asyncio.sleep(0)
        finally:
            self.cancelled.set()

    async def complete(self, name, attempt_id, result):
        await self._submit('complete')

    async def fail(self, name, attempt_id, error):
        await self._submit('fail')


@pytest.mark.parametrize('kind', ['complete', 'fail'])
def test_submission_stops_before_cancellation_notification_can_block(kind):
    async def scenario():
        local, remote = socket.socketpair()
        connection = GatedChannel(local)
        executor = Channel(remote)
        client = RetryingClient()
        controller = Controller(client, 'example', 0.01, connection)
        assignment = Assignment(
            name='sweeps/example/trials/one', attempt_id=1, params={},
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=0.3))
        execution = asyncio.create_task(
            controller._run_assignment(assignment))
        try:
            async with asyncio.timeout(2):
                received = await executor.recv()
                assert received == assignment
                outcome = Outcome(assignment.name, assignment.attempt_id)
                if kind == 'complete':
                    outcome.result = {'objective': 0.5}
                else:
                    outcome.error = 'invalid parameters'
                controller.outcomes.put_nowait(outcome)
                await connection.notifying.wait()

                # IPC notification may wait indefinitely for a reader. The
                # terminal request must already have stopped by this point.
                assert client.cancelled.is_set()
                assert client.attempts
                assert set(client.attempts) == {kind}
                attempts = len(client.attempts)
                for _ in range(3):
                    await asyncio.sleep(0)
                assert len(client.attempts) == attempts
                assert not execution.done()

                connection.release.set()
                with pytest.raises(APIError, match='stale attempt') as raised:
                    await execution
                assert raised.value.code == 10
        finally:
            connection.release.set()
            execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)
            connection.close()
            executor.close()

    asyncio.run(scenario())
