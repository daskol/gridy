import asyncio
import logging
from asyncio import AbstractEventLoop
from concurrent.futures import ProcessPoolExecutor, as_completed
from http import HTTPStatus
from json import dumps
from multiprocessing import Process, Queue
from typing import Any, Callable, Dict, Optional

from aiohttp import ClientSession

__all__ = ('DEFAULT_ENDPOINT', 'Worker', 'WorkerPool')

DEFAULT_ENDPOINT = 'http://127.0.0.1:8080'


class AsyncClient:

    def __init__(self,
                 base_url: str,
                 loop: Optional[AbstractEventLoop] = None):
        self.loop = loop or asyncio.get_event_loop()
        self.sess = ClientSession(base_url, loop=loop)

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

    def __init__(self, base_url: str = DEFAULT_ENDPOINT,
                 loop: Optional[AbstractEventLoop] = None):
        self.loop = loop or asyncio.get_event_loop()
        self.cli = AsyncClient(base_url, self.loop)

    def __del__(self):
        # If we was failed during object initialization, then attribute `cli`
        # can be missing.
        if hasattr(self, 'cli'):
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

    def __init__(self, fn: Callable[..., float],
                 worker_id: Optional[int] = None):
        self.fn = fn
        self.client = Client('http://127.0.0.1:8080')
        self.done = False
        self.queue = Queue()
        self.worker_id = worker_id

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
        value = self.evaluate(tid, params)

        logging.info('submit evaluation on sample #%d: %e', tid, value)
        self.client.submit(trial['tid'], value)

    def evaluate(self, tid: int, params: Dict[str, Any]) -> float:
        def fn(queue: Queue, params: Dict[str, Any]):
            value = self.fn(**params)
            queue.put(value)

        process_id = '' if self.worker_id is None else str(self.worker_id)
        process_name = 'worker' + process_id
        process = Process(target=fn,
                          name=process_name,
                          args=(self.queue, params))

        process.start()
        process.join(timeout=None)
        if process.exitcode is None:
            process.kill()

        return self.queue.get(timeout=None)

    def stop(self, timeout: Optional[float] = None):
        raise NotImplementedError


class WorkerPool:
    """Class WorkerPool manages a group of workers.
    """

    def __init__(self, fn: Callable[..., float], noworkers: int = 1):
        self.fn = fn
        self.noworkers = noworkers

    @staticmethod
    def execute(worker_id: int, fn: Callable[..., float]):
        worker = Worker(fn, worker_id=worker_id)
        worker.run()

    def run(self):
        with ProcessPoolExecutor(self.noworkers) as pool:
            tasks = []
            for worker_id in range(self.noworkers):
                task = pool.submit(WorkerPool.execute, worker_id, self.fn)
                tasks.append(task)
            for task in as_completed(tasks):
                if (e := task.exception()) is not None:
                    logging.error('worker finished with exeception: %s', e)

    def stop(self, timeout: Optional[float] = None):
        raise NotImplementedError


def run(fn: Callable[..., float], noworkers: int = 1):
    logging.info('run group of %d workers', noworkers)
    pool = WorkerPool(fn, noworkers)
    pool.run()
    logging.info('done')
