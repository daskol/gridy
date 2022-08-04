import asyncio
import logging

from asyncio import AbstractEventLoop
from json import dumps
from http import HTTPStatus
from typing import Any, Callable, Dict, Optional

from aiohttp import ClientSession

__all__ = ('Worker', 'WorkerPool')


class AsyncClient:

    def __init__(self,
                 base_url: str,
                 loop: Optional[AbstractEventLoop] = None):
        self.sess = ClientSession(base_url)
        self.loop = loop or asyncio.get_event_loop()

    async def close(self):
        await self.sess.close()

    async def ping(self) -> bool:
        async with self.sess.get('/v1/ping') as res:
            if res.status != HTTPStatus.OK:
                return False
            text = await res.text()
            if text != 'Pong.\n':
                return False
            return True

    async def sample(self) -> Dict[str, Any]:
        async with self.sess.get('/v1/sample') as res:
            return await res.json()

    async def submit(self, tid: int, value: float):
        json = {'tid': tid, 'value': value}
        async with self.sess.post('/v1/submit', json=json) as res:
            if res.status != HTTPStatus.OK:
                text = await res.text()
                raise RuntimeError('Failed to submit evaluation to '
                                   'coordinator. Status code is '
                                   f'{res.status}: {text}.')


class Client:

    def __init__(self, base_url: str,
                 loop: Optional[AbstractEventLoop] = None):
        self.loop = loop or asyncio.get_event_loop()
        self.cli = AsyncClient(base_url, self.loop)

    def __del__(self):
        self.close()

    def close(self):
        self.loop.run_until_complete(self.cli.close())

    def ping(self) -> bool:
        return self.loop.run_until_complete(self.cli.ping())

    def sample(self) -> Dict[str, Any]:
        return self.loop.run_until_complete(self.cli.sample())

    def submit(self, tid: int, value: float) -> bool:
        return self.loop.run_until_complete(self.cli.submit(tid, value))


class Worker:
    """Class Worker is implements sample-eval-submit loop with sampling to and
    submitting from remote coordinator.
    """

    def __init__(self, fn: Callable[..., float]):
        self.fn = fn
        self.client = Client('http://127.0.0.1:8080')
        self.done = False

    def run(self):
        while not self.done:
            try:
                self.run_unsafe()
            except Exception:
                # TODO(@daskol): Think about better back-off.
                logging.exception('sample-eval-submit iteration failed',
                                  exc_info=True)
                raise

    def run_unsafe(self):
        logging.debug('request sample from coordinator')
        trial = self.client.sample()
        tid = trial.get('tid')
        params = trial.get('params', {})

        if tid is None:
            logging.info('no trial identifier: normal exitting')
            self.done = True
            return

        logging.info('evaluate objective function on sample #%d: %s', tid,
                     dumps(params))
        value = self.fn(**params)

        logging.info('submit evaluation on sample #%d: %e', tid, value)
        self.client.submit(trial['tid'], value)

    def stop(self, timeout: Optional[float] = None):
        raise NotImplementedError


class WorkerPool:
    """Class WorkerPool manages a group of workers.
    """

    def __init__(self, fn: Callable[..., float], noworkers: int = 1):
        self.fn = fn
        self.worker = Worker(fn)

    def run(self):
        self.worker.run()

    def stop(self, timeout: Optional[float] = None):
        self.worker.stop()


def run(fn: Callable[..., float], noworkers: int = 1):
    logging.info('run group of %d workers', noworkers)
    pool = WorkerPool(fn, noworkers)
    pool.run()
    logging.info('done')
