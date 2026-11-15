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

"""Asynchronous HTTP/JSON client for the coordinator's QueueService.

Use one client per worker process and close it with an async context manager::

    async with AsyncClient(endpoint, token=token) as client:
        outcome = await client.acquire('experiment-1')
        if isinstance(outcome, Assignment):
            print(outcome.name, outcome.attempt_id, outcome.params)
        elif isinstance(outcome, Wait):
            ...  # Pause before acquiring again with a fresh request ID.
        elif isinstance(outcome, Done):
            ...  # The sweep has finished.

Endpoints support HTTP(S) and unix:///absolute/socket/path. A supplied token is
sent in the Authorization header as a Bearer token. Assignment fields name and
`attempt_id` identify the attempt in renew_lease, complete, and fail. The
latter two methods are alternative ways to finish an attempt.

This client implements queue requests. The worker runtime is responsible for
objective execution and background lease renewal. After an uncertain acquire
failure beyond the retry budget, reuse an explicitly supplied `request_id` to
recover the original assignment. PID, hostname, and code digest metadata are
reserved for a later iteration.
"""

import asyncio
import json
import math
import random
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Self
from urllib.parse import unquote, urlsplit

import aiohttp
from ulid import ULID

__all__ = ('APIError', 'Assignment', 'AsyncClient', 'Done', 'ProtocolError',
           'Wait')

DEFAULT_ENDPOINT = 'http://127.0.0.1:8080'
_IDENTIFIER = r'[A-Za-z0-9_-]+'
_TRIAL_NAME = re.compile(rf'sweeps/{_IDENTIFIER}/trials/{_IDENTIFIER}')
_RETRY_STATUSES = {408, 429, 500, 502, 503, 504}


class ProtocolError(RuntimeError):
    """The coordinator returned a response outside the queue contract."""


class APIError(RuntimeError):
    """An HTTP failure with the gateway's error code and details."""

    def __init__(self, status: int, message: str, *,
                 code: int | None = None, details: Any = None):
        super().__init__(f'HTTP {status}: {message}')
        self.status = status
        self.message = message
        self.code = code
        self.details = details


@dataclass(frozen=True)
class Assignment:
    """An assigned trial attempt. Preserve name and `attempt_id` verbatim."""

    name: str
    attempt_id: int
    params: dict[str, Any]
    lease_expires_at: datetime


@dataclass(frozen=True)
class Wait:
    """No assignment is available yet. Try again with a new request ID."""


@dataclass(frozen=True)
class Done:
    """The sweep has no remaining work."""


def _timestamp(value: Any) -> datetime:
    try:
        timestamp = datetime.fromisoformat(value)
        if timestamp.tzinfo is None:
            raise ValueError('missing timezone')
        return timestamp
    except (TypeError, ValueError) as error:
        raise ProtocolError('Invalid lease_expires_at timestamp') from error


def _attempt_id(value: Any) -> int:
    if isinstance(value, str) and re.fullmatch(r'[0-9]+', value):
        value = int(value)
    if type(value) is not int or not 0 < value < 2**63:
        raise ValueError('attempt_id must be a positive int64')
    return value


class AsyncClient:
    """One worker's HTTP connection pool and identity.

    Construct a client inside each worker process. Its `worker_id` is a ULID
    generated once or an explicitly supplied ULID. HTTP sessions are created
    lazily in the running event loop and owned by the client.

    Transient failures are retried at most `max_retries` times, preserving the
    serialized request. Cancellation is propagated immediately. Supply an
    explicit `request_id` to acquire when retrying beyond this client's retry
    budget. Heartbeats and polling after Wait belong to the worker runtime.
    """

    def __init__(self, endpoint: str = DEFAULT_ENDPOINT, *,
                 worker_id: str | None = None, token: str | None = None,
                 timeout: float = 30, max_retries: int = 3,
                 retry_backoff: float = 0.5):
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError('Timeout must be positive and finite.')
        if type(max_retries) is not int or max_retries < 0:
            raise ValueError('Max retries must be a nonnegative integer.')
        if not math.isfinite(retry_backoff) or retry_backoff < 0:
            raise ValueError('Retry backoff must be nonnegative and finite.')

        self._unix_path: str | None = None
        addr = urlsplit(endpoint)
        if addr.query or addr.fragment:
            raise ValueError('Endpoint must not contain a query or fragment.')
        if addr.scheme == 'unix':
            if addr.netloc or not addr.path.startswith('/'):
                raise ValueError(
                    'Unix socket endpoint needs an absolute path.')
            self._unix_path = unquote(addr.path)
            self._base_url = 'http://localhost'
        elif (addr.scheme in ('http', 'https') and addr.hostname
              and addr.username is None and addr.password is None):
            self._base_url = endpoint.rstrip('/')
        else:
            raise ValueError('Endpoint must be an HTTP URL or unix:///path.')

        self.worker_id: str
        if worker_id is None:
            self.worker_id = str(ULID())
        else:
            self.worker_id = str(ULID.from_str(worker_id))

        self._headers = {'Accept': 'application/json',
                         'Content-Type': 'application/json'}
        if token is not None:
            self._headers['Authorization'] = f'Bearer {token}'

        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff
        self._session: aiohttp.ClientSession | None = None
        self._closed = False

    def _get_session(self) -> aiohttp.ClientSession:
        if self._closed:
            raise RuntimeError('Client is closed.')
        if self._session is None:
            connector = (aiohttp.UnixConnector(path=self._unix_path)
                         if self._unix_path is not None else None)
            self._session = aiohttp.ClientSession(
                connector=connector, headers=self._headers,
                timeout=self._timeout)
        return self._session

    async def __aenter__(self) -> Self:
        self._get_session()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.close()

    async def close(self) -> None:
        """Release connections (closing an unused client is also safe)."""
        self._closed = True
        if self._session is not None:
            await self._session.close()

    async def _post(self, path: str,
                    payload: dict[str, Any]) -> dict[str, Any]:
        # Serialize once so retries cannot observe mutations of result data.
        body = json.dumps(payload, allow_nan=False)
        for attempt in range(self._max_retries):
            try:
                return await self._post_once(path, body)
            except APIError as error:
                if error.status not in _RETRY_STATUSES:
                    raise
            except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError,
                    asyncio.TimeoutError):
                pass
            delay = min(self._retry_backoff * 2**attempt, 30)
            await asyncio.sleep(random.uniform(0, delay))
        return await self._post_once(path, body)

    async def _post_once(self, path: str, body: str) -> dict[str, Any]:
        session = self._get_session()
        async with session.post(self._base_url + path, data=body,
                                allow_redirects=False) as response:
            status = response.status
            raw = await response.text()

        try:
            data = json.loads(raw)
        except ValueError:
            data = None

        if 200 <= status < 300:
            if not isinstance(data, dict):
                raise ProtocolError('Expected a JSON object')
            return data
        if not isinstance(data, dict):
            raise APIError(status, raw)
        raise APIError(status, str(data.get('message', raw)),
                       code=data.get('code'), details=data.get('details'))

    async def acquire(self, sweep_id: str, *,
                      request_id: str | None = None
                      ) -> Assignment | Wait | Done:
        """Acquire once.

        HTTP retries reuse the same `request_id` and `worker_id`. Sweep ID is
        the bare identifier (not a resource name like `sweeps/...`). A
        subsequent call after `Wait` uses a fresh `request_id` by default.
        """
        if not re.fullmatch(_IDENTIFIER, sweep_id):
            raise ValueError('Invalid sweep_id')
        if request_id is None:
            request_id = str(ULID())
        if not isinstance(request_id, str) or not request_id:
            raise ValueError('request_id must be a nonempty string')
        data = await self._post(f'/v1/sweeps/{sweep_id}/trials:acquire', {
            'workerId': self.worker_id, 'requestId': request_id,
        })
        outcomes = [key for key in ('assignment', 'wait', 'done')
                    if data.get(key) is not None]
        if len(outcomes) != 1 or not isinstance(data[outcomes[0]], dict):
            raise ProtocolError('Expected exactly one acquisition outcome.')
        outcome = outcomes[0]
        if outcome == 'wait':
            return Wait()
        if outcome == 'done':
            return Done()
        assignment = data['assignment']
        name = assignment.get('name')
        params = assignment.get('params')
        if (not isinstance(name, str) or not _TRIAL_NAME.fullmatch(name)
                or not isinstance(params, dict)):
            raise ProtocolError('Invalid assignment name or params')
        try:
            attempt_id = _attempt_id(assignment.get(
                'attemptId', assignment.get('attempt_id')))
        except ValueError as error:
            raise ProtocolError('Invalid assignment attempt_id') from error
        return Assignment(
            name=name, attempt_id=attempt_id, params=params,
            lease_expires_at=_timestamp(assignment.get(
                'leaseExpiresAt', assignment.get('lease_expires_at'))))

    def _attempt(self, name: str, attempt_id: int) -> dict[str, Any]:
        if not _TRIAL_NAME.fullmatch(name):
            raise ValueError('Invalid trial resource name')
        return {'attemptId': str(_attempt_id(attempt_id))}

    async def renew_lease(self, name: str, attempt_id: int) -> datetime:
        """Extend an attempt's lease and return its new expiration time."""
        data = await self._post(f'/v1/{name}:renewLease',
                                self._attempt(name, attempt_id))
        return _timestamp(data.get('leaseExpiresAt',
                                   data.get('lease_expires_at')))

    async def complete(self, name: str, attempt_id: int,
                       result: dict[str, Any]) -> None:
        """Record a JSON object containing the objective value and metrics."""
        if not isinstance(result, dict):
            raise ValueError('result must be a JSON object')
        payload = self._attempt(name, attempt_id)
        payload['result'] = result
        await self._post(f'/v1/{name}:complete', payload)

    async def fail(self, name: str, attempt_id: int, error: str) -> None:
        """Record an attempt's nonempty failure description."""
        if not isinstance(error, str) or not error:
            raise ValueError('error must be a nonempty string')
        payload = self._attempt(name, attempt_id)
        payload['error'] = error
        await self._post(f'/v1/{name}:fail', payload)
