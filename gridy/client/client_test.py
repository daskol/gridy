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

"""Exercise service namespaces and lifecycle of the aggregate client."""

from asyncio import run
from typing import Any
from unittest import IsolatedAsyncioTestCase, TestCase

from aiohttp import web
from aiohttp.web import Request
from ulid import ULID

from gridy import AsyncClient, Wait

TRIAL = {'trial_id': '1', 'params': {'lr': 0.001},
         'state': 'TRIAL_STATE_COMPLETED', 'result': {'objective': 0.87}}
EXPIRATION = '2026-11-15T12:00:00Z'


class TestClientConstruction(TestCase):

    def test_constructs_without_event_loop_and_closes_unused_client(self):
        client = AsyncClient()

        async def close_unused_client():
            await client.close()
            await client.close()
            with self.assertRaisesRegex(RuntimeError, 'closed'):
                await client.queue.acquire('experiment')
            with self.assertRaisesRegex(RuntimeError, 'closed'):
                await anext(client.sweep.iter_trials(sweep='experiment'))

        run(close_unused_client())


class TestAsyncClient(IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        app = web.Application()
        app.router.add_post('/{path:.*}', self.handle)
        app.router.add_get('/{path:.*}', self.handle)

        self.runner = web.AppRunner(app)
        await self.runner.setup()

        site = web.TCPSite(self.runner, '127.0.0.1', 0)
        await site.start()

        host, port = self.runner.addresses[0]
        self.endpoint = f'http://{host}:{port}'
        self.requests = []

    async def asyncTearDown(self):
        await self.runner.cleanup()

    async def handle(self, req: Request):
        payload: Any = None
        if req.method == 'POST':
            payload = await req.json()
        auth = req.headers.get('Authorization')
        self.requests.append((req.method, req.path, auth, payload))

        res: dict[str, Any]
        if req.path.endswith(':acquire'):
            res = {'wait': {}}
        elif req.path.endswith(':renewLease'):
            res = {'lease_expires_at': EXPIRATION}
        elif req.method == 'POST':
            res = {}
        else:
            res = {'trials': [TRIAL]}
        return web.json_response(res)

    async def assert_client_closed(self, client: AsyncClient) -> None:
        with self.assertRaisesRegex(RuntimeError, 'closed'):
            await client.queue.acquire('experiment')
        with self.assertRaisesRegex(RuntimeError, 'closed'):
            await anext(client.sweep.iter_trials(sweep='experiment'))

    async def test_service_clients(self):
        sweep_id = 'experiment'
        worker_id = str(ULID())
        req_id = 'request-1'
        token = 'test-token'

        async with AsyncClient(
            self.endpoint, worker_id=worker_id, token=token,
        ) as cli:
            outcome = await cli.queue.acquire(sweep_id, request_id=req_id)
            await cli.queue.close()

            trials = []
            async for trial in cli.sweep.iter_trials(sweep=sweep_id):
                trials.append(trial)

        await self.assert_client_closed(cli)
        self.assertIsInstance(outcome, Wait)
        self.assertEqual(trials, [TRIAL])

        auth = f'Bearer {token}'
        payload = {'worker_id': worker_id, 'request_id': req_id}
        self.assertEqual(self.requests, [
            ('POST', '/v1/sweeps/experiment/trials:acquire', auth, payload),
            ('GET', '/v1/sweeps/experiment/trials', auth, None),
        ])

    async def test_exception_inside_context_closes_both_service_clients(self):
        client = AsyncClient(self.endpoint)

        with self.assertRaisesRegex(ValueError, 'context failure'):
            async with client:
                raise ValueError('context failure')

        await self.assert_client_closed(client)
        self.assertEqual(self.requests, [])

    async def test_failed_context_entry_closes_both_service_clients(self):
        client = AsyncClient(self.endpoint)
        await client.sweep.close()

        with self.assertRaisesRegex(RuntimeError, 'closed'):
            async with client:
                self.fail('Entering a client with a closed service succeeded')

        await self.assert_client_closed(client)
        self.assertEqual(self.requests, [])
