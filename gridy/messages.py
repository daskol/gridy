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

"""Messages exchanged by the worker and controller."""

from dataclasses import dataclass
from typing import Any, ClassVar, Self

from gridy.client import APIError, ProtocolError
from gridy.errors import CancelledError, LeaseLostError, WorkerError

__all__ = ('Outcome', 'Cancel', 'Acknowledged', 'Error', 'Finished', 'Stop')


@dataclass
class Outcome:
    name: str
    attempt_id: int
    result: dict[str, Any] | None = None
    error: str | None = None
    cancelled: str | None = None


@dataclass
class Cancel:
    name: str
    attempt_id: int
    reason: str


@dataclass
class Acknowledged:
    name: str
    attempt_id: int


@dataclass
class Error:
    kind: str
    message: str
    status: int | None = None
    code: int | None = None
    details: Any = None

    _TYPES: ClassVar[dict[str, type[Exception]]] = {
        'LeaseLostError': LeaseLostError,
        'WorkerError': WorkerError,
        'CancelledError': CancelledError,
        'ProtocolError': ProtocolError,
        'ValueError': ValueError,
        'OSError': OSError,
        'ConnectionError': ConnectionError,
        'RuntimeError': RuntimeError,
    }

    @classmethod
    def capture(cls, exc: BaseException) -> Self:
        if not isinstance(exc, APIError):
            return cls(type(exc).__name__, str(exc))
        return cls('APIError', exc.message, exc.status, exc.code, exc.details)

    def exception(self) -> Exception:
        if self.kind == 'APIError' and self.status is not None:
            return APIError(self.status, self.message,
                            code=self.code, details=self.details)
        error_type = self._TYPES.get(self.kind)
        if error_type is not None:
            return error_type(self.message)
        return WorkerError(f'{self.kind}: {self.message}')


@dataclass
class Finished:
    pass


@dataclass
class Stop:
    pass
