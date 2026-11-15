#!/usr/bin/env python3

import asyncio
import os
from random import uniform
from time import sleep
from typing import Any

from gridy import AsyncClient, search


def objective(params: dict[str, Any]) -> float:
    value = uniform(0, 1)
    print(value, '<-', params)
    sleep(1)
    return value


async def main():
    endpoint = os.getenv('GRIDY_ENDPOINT', 'http://localhost:8080')
    token = os.getenv('GRIDY_TOKEN')
    sid = 'search-space'
    async with AsyncClient(endpoint, token=token) as cli:
        await search(cli, objective, sid)


if __name__ == '__main__':
    asyncio.run(main())
