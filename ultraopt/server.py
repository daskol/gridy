import logging

from typing import Optional

from aiohttp.web import Application, Request, Response, json_response, run_app

from .sampler import PersistentSampler, Sampler
from .util import load_search_space


__all__ = ('Coordinator', )


class Handler:

    def __init__(self, sampler: Sampler):
        self.sampler = sampler

    async def get_ping(self, req: Request):
        return Response(text='Pong.\n')

    async def get_sample(self, req: Request):
        sample = self.sampler.sample()
        return json_response(sample)

    async def post_submit(self, req: Request):
        try:
            body = await req.json()
        except Exception:
            return Response(status=400, text='Expected JSON body.')

        # Validate trial identifier.
        if (tid := body.get('tid')) is None:
            return Response(status=400, text='Expected `tid` field.')

        # Validate objective value on trial.
        if (val := body.get('value')) is None:
            return Response(status=400, text='Expected `value` field.')
        elif not isinstance(val, (int, float)):
            return Response(status=400, text='Type of `value` is not number.')

        self.sampler.submit(tid, val)
        return Response()


class Coordinator:

    def __init__(self, sampler: Sampler, host='127.0.0.1', port=8080):
        self.sampler = sampler
        self.handler = Handler(self.sampler)

        self.host = host
        self.port = port

        self.app = Application()
        self.app.router.add_get('/v1/ping', self.handler.get_ping)
        self.app.router.add_get('/v1/sample', self.handler.get_sample)
        self.app.router.add_post('/v1/submit', self.handler.post_submit)

    def run(self):
        logging.info('run coordinator on %s:%d', self.host, self.port)
        run_app(self.app, host=self.host, port=self.port)


def run(host: str, port: int, seed: Optional[int] = None):
    space = load_search_space()
    sampler = PersistentSampler.open('wal.jsonl', space)
    coord = Coordinator(sampler, host=host, port=port)
    coord.run()
