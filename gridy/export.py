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

"""Export completed trials from a coordinator as JSON Lines."""

import json
import re
import sys
from collections.abc import AsyncIterable, Generator
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import IO, Any

from gridy.client import AsyncSweepClient, ProtocolError

_IDENTIFIER = re.compile(r'[A-Za-z0-9_-]+')


def export_database(source: str, output: str, *,
                    sweep: str | None = None) -> None:
    """Reserve local database export for a later implementation."""
    raise NotImplementedError(
        'Export from a database file is not implemented.')


@contextmanager
def _output(path: str) -> Generator[IO[str], None, None]:
    if path == '-':
        yield sys.stdout
        return

    destination = Path(path)
    with NamedTemporaryFile(
        mode='w', encoding='utf-8', newline='\n', dir=destination.parent,
        prefix=f'.{destination.name}.', suffix='.tmp', delete=False,
    ) as stream:
        temporary = Path(stream.name)
        try:
            yield stream.file
            stream.close()
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)


def _record(trial: dict[str, Any]) -> dict[str, Any]:
    trial_id = trial.get('trial_id')
    if not isinstance(trial_id, str) or not _IDENTIFIER.fullmatch(trial_id):
        raise ProtocolError('Invalid trial_id from the coordinator.')

    result = trial.get('result')
    value: object | None = None
    if isinstance(result, dict):
        value = result.get('objective')
    if type(value) not in (int, float):
        raise ProtocolError(
            f'Trial {trial_id}: result.objective must be a number.')

    params = trial.get('params')
    if not isinstance(params, dict):
        raise ProtocolError(f'Trial {trial_id}: params must be an object.')
    return {'trial_id': trial_id, 'value': value, 'params': params}


async def _write_trials(trials: AsyncIterable[dict[str, Any]],
                        stream: IO[str]) -> None:
    """Write completed trials to a JSONL stream."""
    async for trial in trials:
        if trial.get('state') not in ('TRIAL_STATE_COMPLETED', 3):
            continue
        record = _record(trial)
        try:
            line = json.dumps(record, ensure_ascii=False, allow_nan=True)
        except ValueError as e:
            raise ProtocolError(f'Trial {record["trial_id"]}: {e}.') from e
        stream.write(line + '\n')


async def export_uri(source: str, output: str, *,
                     sweep: str | None = None) -> None:
    """Write completed trials from one sweep.

    Sources are HTTP(S) endpoints or Unix socket. When no sweep is selected,
    the coordinator must contain exactly one sweep. Output '-' writes directly
    to stdout. Files are replaced atomically after all pages have been read and
    validated.
    """
    async with AsyncSweepClient(source) as client:
        with _output(output) as stream:
            await _write_trials(client.iter_trials(sweep=sweep), stream)
