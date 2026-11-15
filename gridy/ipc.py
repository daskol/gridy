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

"""Exchange framed messages over a trusted local, nonblocking socket."""

import errno
import pickle
import socket
import struct
from asyncio import AbstractEventLoop, Future, Lock, get_running_loop
from typing import Any

__all__ = ('Channel',)

MAX_MESSAGE_SIZE = 64 * 1024 * 1024
_READ_CHUNK_SIZE = 64 * 1024
_POLL_READ_LIMIT = 4
_MISSING = object()


class Channel:
    """Own a socket carrying length-prefixed pickle messages.

    Peers must be trusted: decoding pickle can execute Python code. Messages
    have a four-byte network-order length and a payload of at most 64 MiB.
    One receiver may alternate between `recv` and `recv_nowait`. Concurrent
    senders are serialized. Concurrent receivers are rejected.

    Partial input remains buffered when `recv` is cancelled. A cancelled or
    failed send closes the channel because its frame may be incomplete. EOF
    raises EOFError. Malformed or oversized frames raise ValueError and close
    the channel. Decoded messages buffered before EOF are returned first.
    """

    def __init__(self, sock: socket.socket):
        self.socket = sock
        self.socket.setblocking(False)
        self._buffer = bytearray()
        self._sending = Lock()
        self._receiving = False
        self._reader: tuple[AbstractEventLoop, int, Future[None]] | None = None

    def close(self) -> None:
        """Close the socket and wake a receive operation awaiting readiness."""
        reader = self._reader
        if reader is not None:
            loop, descriptor, ready = reader
            loop.remove_reader(descriptor)
            self._reader = None
            if not ready.done():
                ready.set_result(None)
        self.socket.close()

    async def send(self, message: Any) -> None:
        """Send one message, closing the channel if writing is interrupted."""
        payload = pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)
        if len(payload) > MAX_MESSAGE_SIZE:
            raise ValueError('IPC message exceeds the maximum size.')
        async with self._sending:
            if self.socket.fileno() < 0:
                raise ConnectionError('IPC channel is closed.')
            loop = get_running_loop()
            try:
                await loop.sock_sendall(
                    self.socket, struct.pack('!I', len(payload)))
                await loop.sock_sendall(self.socket, payload)
            except BaseException:
                self.close()
                raise

    def _take_message(self) -> Any:
        if len(self._buffer) < 4:
            return _MISSING
        size, = struct.unpack_from('!I', self._buffer)
        if not 0 < size <= MAX_MESSAGE_SIZE:
            self.close()
            raise ValueError('Invalid IPC frame size.')
        if len(self._buffer) < size + 4:
            return _MISSING
        payload = self._buffer[4:size + 4]
        del self._buffer[:size + 4]
        try:
            return pickle.loads(payload)
        except Exception as e:
            self.close()
            raise ValueError('Invalid IPC message payload.') from e

    def _read_ready(self) -> Any:
        if self.socket.fileno() < 0:
            raise EOFError('IPC channel is closed.')
        # Limit each poll so a continuously readable socket cannot monopolize
        # an objective's cancellation check or the controller's event loop.
        for _ in range(_POLL_READ_LIMIT):
            message = self._take_message()
            if message is not _MISSING:
                return message
            chunk = self.socket.recv(_READ_CHUNK_SIZE)
            if not chunk:
                truncated = bool(self._buffer)
                self.close()
                if truncated:
                    raise EOFError('Peer closed with a truncated IPC frame.')
                raise EOFError('IPC peer closed the channel.')
            self._buffer.extend(chunk)
        message = self._take_message()
        if message is not _MISSING:
            return message
        raise BlockingIOError(errno.EWOULDBLOCK, 'No complete IPC message.')

    def recv_nowait(self) -> Any:
        """Return a complete message or raise BlockingIOError immediately."""
        if self._receiving:
            raise RuntimeError('Only one IPC receiver may run at a time.')
        return self._read_ready()

    async def _wait_readable(self) -> None:
        loop = get_running_loop()
        ready: Future[None] = loop.create_future()
        descriptor = self.socket.fileno()
        reader = (loop, descriptor, ready)

        def readable() -> None:
            if not ready.done():
                ready.set_result(None)

        loop.add_reader(descriptor, readable)
        self._reader = reader
        try:
            # Readiness consumes no bytes. Even cancellation after the ready
            # callback fires cannot discard data received by the socket.
            await ready
        finally:
            if self._reader is reader:
                loop.remove_reader(descriptor)
                self._reader = None

    async def recv(self) -> Any:
        """Await one message, preserving all buffered input on cancellation."""
        if self._receiving:
            raise RuntimeError('Only one IPC receiver may run at a time.')
        self._receiving = True
        try:
            while True:
                try:
                    return self._read_ready()
                except BlockingIOError:
                    await self._wait_readable()
        finally:
            self._receiving = False
