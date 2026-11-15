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

"""Aggregate client for Gridy's service APIs."""

from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from datetime import datetime
from typing import TYPE_CHECKING, Any, Self

from gridy.client.http import DEFAULT_ENDPOINT
from gridy.client.protocol import (
    AsyncQueueClientProtocol, AsyncSweepClientProtocol)
from gridy.client.queue import Assignment, AsyncQueueClient, Done, Wait
from gridy.client.sweep import AsyncSweepClient

__all__ = ('AsyncClient',)


class AsyncClient:
    """Forward service APIs to clients with common settings and one lifetime.

    Each service owns its HTTP session. Use an async context manager or
    `close()` to release all service connections. The queue and sweep clients
    are also available through their respective attributes.

        async with AsyncClient(endpoint, token=token) as client:
            outcome = await client.acquire('experiment')
            async for trial in client.iter_trials(sweep='experiment'):
                ...
    """

    def __init__(self, endpoint: str = DEFAULT_ENDPOINT, *,
                 worker_id: str | None = None, token: str | None = None,
                 timeout: float = 30, max_retries: int = 3,
                 retry_backoff: float = 0.5):
        self.queue: AsyncQueueClientProtocol = AsyncQueueClient(
            endpoint, worker_id=worker_id, token=token, timeout=timeout,
            max_retries=max_retries, retry_backoff=retry_backoff)
        self.sweep: AsyncSweepClientProtocol = AsyncSweepClient(
            endpoint, token=token, timeout=timeout,
            max_retries=max_retries, retry_backoff=retry_backoff)

        # Register exits before entry so close() also covers unused clients.
        self._stack = AsyncExitStack()
        self._stack.push_async_exit(self.queue)
        self._stack.push_async_exit(self.sweep)

    async def __aenter__(self) -> Self:
        async with self._stack as stack:
            await self.queue.__aenter__()
            await self.sweep.__aenter__()
            self._stack = stack.pop_all()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> bool | None:
        return await self._stack.__aexit__(exc_type, exc, traceback)

    async def close(self) -> None:
        """Close every service client including those not used yet."""
        await self._stack.aclose()

    @property
    def worker_id(self) -> str:
        return self.queue.worker_id

    @worker_id.setter
    def worker_id(self, value: str) -> None:
        self.queue.worker_id = value

    async def acquire(self, sweep_id: str, *,
                      request_id: str | None = None
                      ) -> Assignment | Wait | Done:
        return await self.queue.acquire(sweep_id, request_id=request_id)

    async def renew_lease(self, name: str, attempt_id: int) -> datetime:
        return await self.queue.renew_lease(name, attempt_id)

    async def complete(self, name: str, attempt_id: int,
                       result: dict[str, Any]) -> None:
        await self.queue.complete(name, attempt_id, result)

    async def fail(self, name: str, attempt_id: int, error: str) -> None:
        await self.queue.fail(name, attempt_id, error)

    def iter_trials(self, *, sweep: str | None = None
                    ) -> AsyncIterator[dict[str, Any]]:
        return self.sweep.iter_trials(sweep=sweep)


if TYPE_CHECKING:
    _client = AsyncClient()
    _queue: AsyncQueueClientProtocol = _client
    _sweep: AsyncSweepClientProtocol = _client
