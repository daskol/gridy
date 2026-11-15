#!/usr/bin/env python3

import asyncio
import os
from functools import partial
from random import uniform
from time import sleep
from typing import Any

from gridy import AsyncQueueClient, Context, search


def objective(ctx: Context, params: dict[str, Any]) -> dict[str, Any]:
    ctx.raise_if_cancelled()
    value = uniform(0, 1)
    print(value, '<-', params)
    sleep(1)
    ctx.raise_if_cancelled()
    return {'objective': value}


async def main():
    endpoint = os.getenv('GRIDY_ENDPOINT', 'http://localhost:8080')
    token = os.getenv('GRIDY_TOKEN')
    sid = 'search-space'
    factory = partial(AsyncQueueClient, endpoint, token=token)
    await search(objective, sid, client_factory=factory)


if __name__ == '__main__':
    asyncio.run(main())
