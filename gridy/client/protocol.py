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

"""Structural interfaces for Gridy's asynchronous service clients."""

from collections.abc import AsyncIterator
from datetime import datetime
from types import TracebackType
from typing import Any, Protocol, Self

from gridy.client.queue import Assignment, Done, Wait

__all__ = ('AsyncQueueClientProtocol', 'AsyncSweepClientProtocol')


class _AsyncClientProtocol(Protocol):
    """Share asynchronous service client lifecycle operations."""

    async def __aenter__(self) -> Self: ...

    async def __aexit__(self, exc_type: type[BaseException] | None,
                        exc: BaseException | None,
                        traceback: TracebackType | None) -> bool | None: ...

    async def close(self) -> None: ...


class AsyncQueueClientProtocol(_AsyncClientProtocol, Protocol):
    """Acquire and finish trial attempts for one worker."""

    worker_id: str

    async def acquire(self, sweep_id: str, *,
                      request_id: str | None = None
                      ) -> Assignment | Wait | Done: ...

    async def renew_lease(self, name: str, attempt_id: int) -> datetime: ...

    async def complete(self, name: str, attempt_id: int,
                       result: dict[str, Any]) -> None: ...

    async def fail(self, name: str, attempt_id: int, error: str) -> None: ...


class AsyncSweepClientProtocol(_AsyncClientProtocol, Protocol):
    """Read trials from one sweep with pagination."""

    def iter_trials(self, *, sweep: str | None = None
                    ) -> AsyncIterator[dict[str, Any]]: ...
