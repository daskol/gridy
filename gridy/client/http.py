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

"""Shared asynchronous HTTP/JSON transport for Gridy service clients."""

import asyncio
import json
import math
import random
from typing import Any, Self
from urllib.parse import unquote, urlsplit

import aiohttp

__all__ = ('APIError', 'AsyncHTTPClient', 'ProtocolError')

DEFAULT_ENDPOINT = 'http://127.0.0.1:8080'
_RETRY_STATUSES = {408, 429, 500, 502, 503, 504}


class ProtocolError(RuntimeError):
    """The coordinator returned a response outside the API contract."""


class APIError(RuntimeError):
    """An HTTP failure with the gateway's error code and details."""

    def __init__(self, status: int, message: str, *,
                 code: int | None = None, details: Any = None):
        super().__init__(f'HTTP {status}: {message}')
        self.status = status
        self.message = message
        self.code = code
        self.details = details


class AsyncHTTPClient:
    """Own one service client's HTTP connections and request configuration.

    Sessions are created lazily in a running event loop. Use an async context
    manager or `close()` to release connections. Every instance owns its
    session, headers, timeout, and retry settings.
    """

    def __init__(self, endpoint: str = DEFAULT_ENDPOINT, *,
                 token: str | None = None, timeout: float = 30,
                 max_retries: int = 3, retry_backoff: float = 0.5):
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

        self._headers = {'Accept': 'application/json'}
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

    async def _get(self, path: str,
                   params: dict[str, str] | None = None) -> dict[str, Any]:
        return await self._request('GET', path, params=params)

    async def _post(self, path: str,
                    payload: dict[str, Any]) -> dict[str, Any]:
        # Serialize once so retries cannot observe mutations of result data.
        body = json.dumps(payload, allow_nan=False)
        return await self._request('POST', path, body=body)

    async def _request(self, method: str, path: str, *,
                       params: dict[str, str] | None = None,
                       body: str | None = None) -> dict[str, Any]:
        for attempt in range(self._max_retries):
            try:
                return await self._request_once(
                    method, path, params=params, body=body)
            except APIError as e:
                if e.status not in _RETRY_STATUSES:
                    raise
            except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError,
                    asyncio.TimeoutError):
                pass
            delay = min(self._retry_backoff * 2**attempt, 30)
            await asyncio.sleep(random.uniform(0, delay))
        return await self._request_once(method, path, params=params, body=body)

    async def _request_once(self, method: str, path: str, *,
                            params: dict[str, str] | None,
                            body: str | None) -> dict[str, Any]:
        session = self._get_session()
        headers = ({'Content-Type': 'application/json'}
                   if body is not None else None)
        async with session.request(
                method, self._base_url + path, params=params, data=body,
                headers=headers, allow_redirects=False) as response:
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
