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

"""Asynchronous HTTP/JSON client for the coordinator's SweepService."""

import re
from collections.abc import AsyncIterator
from typing import Any

from gridy.client.http import AsyncHTTPClient, ProtocolError

__all__ = ('AsyncSweepClient',)

_IDENTIFIER = re.compile(r'[A-Za-z0-9_-]+')


class AsyncSweepClient(AsyncHTTPClient):
    """Read trials page by page from an HTTP(S) or Unix socket endpoint.

    HTTP sessions are created lazily in the running event loop and owned by the
    client. Use an async context manager or `close()` to release connections.
    Iterators share the client's session and fetch pages on demand.

        async with AsyncSweepClient(endpoint) as client:
            async for trial in client.iter_trials(sweep='experiment'):
                ...
    """

    async def iter_trials(self, *, sweep: str | None = None
                          ) -> AsyncIterator[dict[str, Any]]:
        """Yield unmodified trials of all states with pagination on demand.

        If sweep is omitted, the coordinator must contain exactly one sweep.
        The client owns the HTTP session across iterator calls.
        """
        if sweep is not None and not _IDENTIFIER.fullmatch(sweep):
            raise ValueError('Invalid sweep identifier.')
        if sweep is None:
            sweep = await self._select_sweep()

        path = f'/v1/sweeps/{sweep}/trials'
        async for trial in self._items(path, 'trials'):
            yield trial

    async def _select_sweep(self) -> str:
        selected = None
        async for item in self._items('/v1/sweeps', 'sweeps'):
            if selected is not None:
                raise ValueError('Multiple sweeps found: select one sweep.')
            selected = item.get('sweep_id')
            if (not isinstance(selected, str) or
                not _IDENTIFIER.fullmatch(selected)):
                raise ProtocolError('Invalid sweep_id from the coordinator.')

        if selected is None:
            raise ValueError('No sweeps found at the coordinator.')
        return selected

    async def _items(self, path: str,
                     collection: str) -> AsyncIterator[dict[str, Any]]:
        token = ''
        seen = set[str]()
        while True:
            params = {'page_size': '1000'}
            if token:
                params['page_token'] = token

            data = await self._get(path, params)
            items = data.get(collection, [])
            if not isinstance(items, list):
                raise ProtocolError(f'Expected a list of {collection}.')
            for item in items:
                if not isinstance(item, dict):
                    raise ProtocolError(f'Expected an object in {collection}.')
                yield item

            token = data.get('next_page_token', '')
            if not isinstance(token, str):
                raise ProtocolError('Expected a string next_page_token.')
            if not token:
                return
            if token in seen:
                raise ProtocolError('The coordinator repeated a page token.')
            seen.add(token)
