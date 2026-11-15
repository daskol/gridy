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

"""Exercise framed IPC using real nonblocking socket pairs."""

import asyncio
import pickle
import socket
import struct

import pytest

from gridy.ipc import MAX_MESSAGE_SIZE, Channel


def frame(message):
    payload = pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)
    return struct.pack('!I', len(payload)) + payload


@pytest.fixture
def pair():
    left, right = socket.socketpair()
    channel = Channel(left)
    right.setblocking(False)
    try:
        yield channel, right
    finally:
        channel.close()
        right.close()


def test_poll_buffers_fragmented_header_and_payload_without_blocking(pair):
    channel, peer = pair
    wire = frame({'trial': 1, 'params': {'lr': 0.01}})
    chunks = [wire[:1], wire[1:3], wire[3:5], wire[5:-1], wire[-1:]]
    for chunk in chunks[:-1]:
        peer.sendall(chunk)
        with pytest.raises(BlockingIOError):
            channel.recv_nowait()
    peer.sendall(chunks[-1])
    assert channel.recv_nowait() == {'trial': 1, 'params': {'lr': 0.01}}
    with pytest.raises(BlockingIOError):
        channel.recv_nowait()


def test_poll_returns_coalesced_messages_in_order_before_eof(pair):
    channel, peer = pair
    messages = [None, {'trial': 1}, 'stop', [1, 2, 3]]
    peer.sendall(b''.join(frame(message) for message in messages))
    peer.close()
    assert [channel.recv_nowait() for _ in messages] == messages
    with pytest.raises(EOFError, match='peer closed'):
        channel.recv_nowait()


@pytest.mark.parametrize('split', [1, 3, 5, 12])
def test_cancelled_receive_preserves_partial_header_and_body(pair, split):
    channel, peer = pair
    message = {'trial': 4, 'result': {'objective': 0.9}}
    wire = frame(message)

    async def run():
        peer.sendall(wire[:split])
        receiving = asyncio.create_task(channel.recv())
        await asyncio.sleep(0)
        receiving.cancel()
        with pytest.raises(asyncio.CancelledError):
            await receiving
        peer.sendall(wire[split:])
        assert await channel.recv() == message

    asyncio.run(run())


def test_cancellation_after_readiness_does_not_consume_unobserved_bytes(
        pair, monkeypatch):
    channel, peer = pair
    message = {'trial': 7, 'attempt_id': 2}
    callbacks = []

    async def run():
        loop = asyncio.get_running_loop()
        add_reader = loop.add_reader
        receiving = None

        def register(descriptor, callback, *args):
            def ready():
                callback(*args)
                callbacks.append('ready')
                receiving.cancel()

            add_reader(descriptor, ready)

        monkeypatch.setattr(loop, 'add_reader', register)
        receiving = asyncio.create_task(channel.recv())
        await asyncio.sleep(0)
        peer.sendall(frame(message))
        with pytest.raises(asyncio.CancelledError):
            await receiving
        monkeypatch.setattr(loop, 'add_reader', add_reader)
        assert channel.recv_nowait() == message

    asyncio.run(run())
    assert callbacks == ['ready']


def test_async_and_polled_receives_share_partial_input(pair):
    channel, peer = pair
    message = {'control': 'cancel', 'reason': 'lease_lost'}
    wire = frame(message)
    peer.sendall(wire[:6])
    with pytest.raises(BlockingIOError):
        channel.recv_nowait()

    async def run():
        receiving = asyncio.create_task(channel.recv())
        await asyncio.sleep(0)
        peer.sendall(wire[6:])
        assert await receiving == message
        peer.sendall(frame('finished'))
        assert channel.recv_nowait() == 'finished'

    asyncio.run(run())


def test_large_send_yields_to_heartbeat_under_backpressure(pair):
    sender, peer = pair
    receiver = Channel(peer)
    sender.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
    payload = b'x' * (4 * 1024 * 1024)
    ticks = []

    async def run():
        sending = asyncio.create_task(sender.send(payload))
        for tick in range(5):
            await asyncio.sleep(0.005)
            assert not sending.done()
            ticks.append(tick)
        assert await receiver.recv() == payload
        await sending

    asyncio.run(run())
    assert ticks == list(range(5))


def test_concurrent_sends_preserve_frame_boundaries(pair):
    sender, peer = pair
    receiver = Channel(peer)
    messages = [{'index': index, 'data': bytes([index]) * 100_000}
                for index in range(12)]

    async def run():
        sends = asyncio.gather(*(sender.send(message) for message in messages))
        received = [await receiver.recv() for _ in messages]
        await sends
        assert received == messages

    asyncio.run(run())


def test_cancelling_partial_send_closes_channel_and_prevents_next_frame(pair):
    sender, peer = pair
    receiver = Channel(peer)
    sender.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)

    async def run():
        sending = asyncio.create_task(sender.send(b'x' * (4 * 1024 * 1024)))
        await asyncio.sleep(0)
        assert not sending.done()
        sending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await sending
        assert sender.socket.fileno() == -1
        with pytest.raises(ConnectionError, match='closed'):
            await sender.send('next')
        with pytest.raises(EOFError, match='truncated'):
            await receiver.recv()

    asyncio.run(run())


def test_cancelling_send_waiting_for_lock_keeps_channel_usable(pair):
    sender, peer = pair
    receiver = Channel(peer)
    sender.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
    payload = b'x' * (1024 * 1024)

    async def run():
        first = asyncio.create_task(sender.send(payload))
        await asyncio.sleep(0)
        waiting = asyncio.create_task(sender.send('cancelled'))
        await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        assert sender.socket.fileno() >= 0
        assert await receiver.recv() == payload
        await first
        await sender.send('next')
        assert await receiver.recv() == 'next'

    asyncio.run(run())


@pytest.mark.parametrize('wire', [b'\0', b'\0\0\0', frame('trial')[:-1]])
def test_truncated_frame_reports_eof_and_closes_socket(pair, wire):
    channel, peer = pair
    peer.sendall(wire)
    peer.close()
    with pytest.raises(EOFError, match='truncated'):
        asyncio.run(channel.recv())
    assert channel.socket.fileno() == -1


@pytest.mark.parametrize('wire', [
    struct.pack('!I', MAX_MESSAGE_SIZE + 1),
    struct.pack('!I', 0),
    struct.pack('!I', 1) + b'x'])
def test_invalid_frame_is_rejected_and_channel_closed(pair, wire):
    channel, peer = pair
    peer.sendall(wire)
    with pytest.raises(ValueError, match='IPC'):
        channel.recv_nowait()
    assert channel.socket.fileno() == -1


def test_close_wakes_pending_receive(pair):
    channel, _ = pair

    async def run():
        receiving = asyncio.create_task(channel.recv())
        await asyncio.sleep(0)
        channel.close()
        with pytest.raises(EOFError, match='closed'):
            await receiving

    asyncio.run(run())


def test_concurrent_receivers_are_rejected(pair):
    channel, peer = pair

    async def run():
        receiving = asyncio.create_task(channel.recv())
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match='one IPC receiver'):
            await channel.recv()
        with pytest.raises(RuntimeError, match='one IPC receiver'):
            channel.recv_nowait()
        peer.sendall(frame('trial'))
        assert await receiving == 'trial'

    asyncio.run(run())
