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

"""Attempt identity and cooperative cancellation state."""

from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Lock

from gridy.errors import CancelledError

__all__ = ('Context',)


@dataclass
class _CancellationState:
    reason: str | None = None
    lock: Lock = field(default_factory=Lock, repr=False)
    poll: Callable[[], None] | None = field(default=None, repr=False)


@dataclass(frozen=True, eq=False)
class Context:
    """One attempt's identity and irreversible cooperative cancellation state.

    Field `name` is the full trial resource name and `trial_id` its final
    component. The first cancellation reason wins. Contexts are never reused
    for another invocation. Cancellation checks poll IPC in the objective's
    thread without waiting for data. They do not interrupt ongoing computation.
    """

    name: str
    attempt_id: int
    _cancellation: _CancellationState = field(
        default_factory=_CancellationState, init=False, repr=False)

    @property
    def trial_id(self) -> str:
        return self.name.rsplit('/', 1)[-1]

    @property
    def cancelled(self) -> bool:
        return self.reason is not None

    @property
    def reason(self) -> str | None:
        poll = self._cancellation.poll
        if poll is not None:
            poll()
        with self._cancellation.lock:
            return self._cancellation.reason

    def cancel(self, reason: str = 'cancelled') -> None:
        """Cancel once and subsequent requests preserve the original reason."""
        if not isinstance(reason, str) or not reason:
            raise ValueError('Cancellation reason must be a nonempty string.')
        with self._cancellation.lock:
            if self._cancellation.reason is None:
                self._cancellation.reason = reason

    def raise_if_cancelled(self) -> None:
        """Raise `CancelledError` if this attempt has been cancelled."""
        reason = self.reason
        if reason is not None:
            raise CancelledError(reason)
