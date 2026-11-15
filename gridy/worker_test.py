"""Exercise execution and cancellation across a real process boundary."""

import asyncio
import json
import multiprocessing
import os
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from time import monotonic

import pytest

import gridy
from gridy import APIError, Assignment, Done, LeaseLostError, Wait, search

SWEEP_ID = 'experiment-1'
TRIAL_NAME = f'sweeps/{SWEEP_ID}/trials/trial-1'


def read_events(root):
    journal = Path(root) / 'journal.jsonl'
    if not journal.exists():
        return []
    return [json.loads(line) for line in journal.read_text().splitlines()]


def event_names(root):
    return [event['event'] for event in read_events(root)]


def busy_wait(predicate, *, timeout=8):
    """Keep the calling thread busy while the worker makes progress."""
    deadline = monotonic() + timeout
    while not predicate():
        sum(index * index for index in range(10000))
        if monotonic() >= deadline:
            raise AssertionError('The background worker did not make progress')


@dataclass
class ClientFactory:
    """Spawn-safe configuration used to construct a child-owned client."""

    root: str
    trials: int = 1
    lease_seconds: float = 0.3
    renewal: str = 'ok'
    submit_renewals: int = 0
    acquire_error: bool = False
    submit_error: bool = False
    wait_first: bool = False
    assignment_bytes: int = 0

    def __call__(self):
        return RecordingClient(self)


class RecordingClient:
    """A process-local client with observable requests and failures."""

    def __init__(self, config):
        self.config = config
        self.root = Path(config.root)
        self.worker_id = 'test-worker'
        self.acquisitions = 0
        self.submitting = False
        self.submit_renewals = 0
        self.submitted = asyncio.Event()
        self.record('constructed')

    def record(self, event, **fields):
        record = {'event': event, 'pid': os.getpid(),
                  'threads': [thread.name for thread in threading.enumerate()],
                  **fields}
        with (self.root / 'journal.jsonl').open('a') as output:
            output.write(json.dumps(record) + '\n')

    async def __aenter__(self):
        self.record('entered')
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.record('exited')
        await self.close()

    async def close(self):
        self.record('closed')

    async def acquire(self, sweep_id, *, request_id=None):
        self.record('acquire', sweep_id=sweep_id)
        if self.config.acquire_error:
            raise APIError(503, 'coordinator unavailable', code=14,
                           details=[{'source': 'test'}])
        self.acquisitions += 1
        if self.config.wait_first and self.acquisitions == 1:
            return Wait()
        index = self.acquisitions - int(self.config.wait_first)
        if index > self.config.trials:
            return Done()
        params = {'root': self.config.root, 'index': index}
        if self.config.assignment_bytes:
            params['payload'] = 'x' * self.config.assignment_bytes
        expires_at = \
            datetime.now(UTC) + timedelta(seconds=self.config.lease_seconds)
        return Assignment(
            name=f'sweeps/{sweep_id}/trials/trial-{index}',
            attempt_id=6 + index,
            params=params,
            lease_expires_at=expires_at,
        )

    async def renew_lease(self, name, attempt_id):
        self.record('renew', name=name, attempt_id=attempt_id)
        if (self.root / 'started').exists():
            if self.config.renewal == 'stale':
                self.record('lease_lost')
                raise APIError(409, 'stale attempt', code=10)
            if self.config.renewal == 'stall':
                try:
                    await asyncio.Event().wait()
                finally:
                    self.record('renewal_cancelled')
            if self.config.renewal == 'crash':
                os._exit(7)
            if self.config.renewal == 'expired':
                return datetime.now(UTC) - timedelta(seconds=1)
        if self.submitting:
            self.submit_renewals += 1
            if self.submit_renewals >= self.config.submit_renewals:
                self.submitted.set()
                if self.config.renewal == 'ack_stale':
                    self.record('stale_after_acknowledgement')
                    raise APIError(409, 'attempt already completed', code=10)
        seconds = max(self.config.lease_seconds, 0.3)
        return datetime.now(UTC) + timedelta(seconds=seconds)

    async def submit(self, kind, **fields):
        self.record(kind, **fields)
        if self.config.submit_error:
            raise APIError(503, 'submission unavailable', code=14)
        self.submitting = True
        self.submit_renewals = 0
        self.submitted.clear()
        if self.config.submit_renewals:
            await self.submitted.wait()
        self.record(kind + '_acknowledged')
        self.submitting = False

    async def complete(self, name, attempt_id, result):
        await self.submit('complete', name=name, attempt_id=attempt_id,
                          result=result)

    async def fail(self, name, attempt_id, error):
        await self.submit('fail', name=name, attempt_id=attempt_id,
                          error=error)


@pytest.fixture(autouse=True)
def no_worker_leaks():
    before = {process.pid for process in multiprocessing.active_children()}
    threads = set(threading.enumerate())
    yield
    leaked = [process for process in multiprocessing.active_children()
              if process.pid not in before]
    for process in leaked:
        process.kill()
        process.join()
    assert not leaked, 'search left a background worker running'
    leaked_threads = [thread for thread in threading.enumerate()
                      if thread not in threads
                      and thread.name == 'gridy-control']
    assert not leaked_threads, 'search left a control thread running'


def run_search(objective, root, **options):
    poll_interval = options.pop('poll_interval', 0.01)
    factory = ClientFactory(str(root), **options)
    asyncio.run(search(objective, SWEEP_ID, client_factory=factory,
                       poll_interval=poll_interval))


def result_objective(ctx, params):
    return {'objective': 0.25, 'accuracy': 0.9}


def failing_objective(ctx, params):
    raise ValueError('invalid parameters')


def test_closure_runs_in_callers_main_thread_and_reuses_state(tmp_path):
    calls = []
    resource = object()

    def objective(ctx, params):
        assert os.getpid() == parent_pid
        assert threading.current_thread() is threading.main_thread()
        assert resource is saved_resource
        calls.append((ctx, params))
        return {'objective': len(calls), 'pid': os.getpid()}

    parent_pid = os.getpid()
    saved_resource = resource
    run_search(objective, tmp_path, trials=2)

    assert len(calls) == 2
    first, second = [ctx for ctx, _ in calls]
    assert first is not second
    assert (first.name, first.trial_id, first.attempt_id) == (
        TRIAL_NAME, 'trial-1', 7)
    assert (second.trial_id, second.attempt_id) == ('trial-2', 8)
    assert not first.cancelled
    assert not second.cancelled
    first.cancel('later')
    assert first.cancelled
    assert not second.cancelled
    records = read_events(tmp_path)
    child_pids = {record['pid'] for record in records}
    assert len(child_pids) == 1
    assert parent_pid not in child_pids
    assert event_names(tmp_path).count('constructed') == 1
    assert event_names(tmp_path).count('entered') == 1
    assert event_names(tmp_path).count('closed') == 1
    submissions = [record for record in records
                   if record['event'] == 'complete']
    assert [record['result'] for record in submissions] == [
        {'objective': 1, 'pid': parent_pid},
        {'objective': 2, 'pid': parent_pid}]
    names = event_names(tmp_path)
    assert names.index('complete_acknowledged') < names.index('acquire', 4)


def test_callable_object_keeps_resources_between_invocations(tmp_path):
    class Objective:
        def __init__(self):
            self.prepared = object()
            self.calls = 0

        def __call__(self, ctx, params):
            self.calls += 1
            return {'objective': self.calls}

    objective = Objective()
    prepared = objective.prepared
    run_search(objective, tmp_path, trials=3)
    assert objective.calls == 3
    assert objective.prepared is prepared


def test_context_cancellation_is_sticky_and_distinct(tmp_path):
    seen = []

    def objective(ctx, params):
        seen.append(ctx)
        assert not ctx.cancelled
        assert ctx.reason is None
        ctx.cancel('requested')
        ctx.cancel('later_reason')
        assert ctx.cancelled
        assert ctx.reason == 'requested'
        with pytest.raises(gridy.CancelledError) as raised:
            ctx.raise_if_cancelled()
        assert raised.value.reason == 'requested'
        return {'objective': 0}

    with pytest.raises(gridy.CancelledError) as raised:
        run_search(objective, tmp_path)
    assert raised.value.reason == 'requested'
    assert seen[0].cancelled
    assert 'complete' not in event_names(tmp_path)
    assert 'fail' not in event_names(tmp_path)


def test_cpu_bound_objective_does_not_block_lease_renewals(tmp_path):
    def objective(ctx, params):
        (tmp_path / 'started').touch()
        busy_wait(lambda: event_names(tmp_path).count('renew') >= 3)
        ctx.raise_if_cancelled()
        return {'objective': 0.9}

    run_search(objective, tmp_path)
    assert event_names(tmp_path).count('renew') >= 3
    assert event_names(tmp_path).count('complete_acknowledged') == 1


@pytest.mark.parametrize('outcome', ['raise', 'return', 'fail'])
def test_lease_loss_cancels_and_discards_stale_outcome(tmp_path, outcome):
    contexts = []

    def objective(ctx, params):
        contexts.append(ctx)
        (tmp_path / 'started').touch()
        busy_wait(lambda: ctx.cancelled)
        assert ctx.reason == 'lease_lost'
        try:
            if outcome == 'raise':
                ctx.raise_if_cancelled()
            if outcome == 'fail':
                raise ValueError('failure after cancellation')
            return {'objective': 0.9}
        finally:
            deadline = monotonic() + 0.1
            busy_wait(lambda: monotonic() >= deadline)
            assert event_names(tmp_path).count('acquire') == 1
            assert 'complete' not in event_names(tmp_path)
            assert 'fail' not in event_names(tmp_path)
            (tmp_path / 'cleaned_up').touch()

    with pytest.raises(APIError) as raised:
        run_search(objective, tmp_path, renewal='stale', trials=2)

    assert raised.value.status == 409
    assert raised.value.code == 10
    assert (tmp_path / 'cleaned_up').exists()
    assert contexts[0].cancelled
    contexts[0].cancel('other')
    assert contexts[0].reason == 'lease_lost'
    names = event_names(tmp_path)
    assert names.count('acquire') == 1
    assert 'renew' not in names[names.index('lease_lost') + 1:]
    assert 'complete' not in names
    assert 'fail' not in names
    assert names[-1] == 'closed'


@pytest.mark.parametrize('outcome', ['return', 'fail'])
def test_stale_outcome_is_discarded_without_objective_polling(
        tmp_path, outcome):
    contexts = []

    def objective(ctx, params):
        contexts.append(ctx)
        (tmp_path / 'started').touch()
        # Observe the external service, without inspecting the context.
        busy_wait(lambda: 'lease_lost' in event_names(tmp_path))
        if outcome == 'fail':
            raise ValueError('late failure without polling')
        return {'objective': 0.5}

    with pytest.raises(APIError) as raised:
        run_search(objective, tmp_path, renewal='stale', trials=2)

    assert raised.value.status == 409
    assert contexts[0].cancelled
    assert contexts[0].reason == 'lease_lost'
    names = event_names(tmp_path)
    assert names.count('acquire') == 1
    assert 'complete' not in names
    assert 'fail' not in names
    assert names[-1] == 'closed'


@pytest.mark.parametrize('renewal', ['stall', 'expired'])
def test_unconfirmed_renewal_cannot_outlive_lease(tmp_path, renewal):
    contexts = []

    def objective(ctx, params):
        contexts.append(ctx)
        (tmp_path / 'started').touch()
        busy_wait(lambda: ctx.cancelled)
        ctx.raise_if_cancelled()

    started = monotonic()
    with pytest.raises(LeaseLostError):
        run_search(objective, tmp_path, renewal=renewal)
    assert monotonic() - started < 8
    assert contexts[0].reason == 'lease_lost'
    names = event_names(tmp_path)
    if renewal == 'stall':
        assert 'renewal_cancelled' in names
    assert 'complete' not in names
    assert 'fail' not in names


@pytest.mark.parametrize('objective,kind', [
    (result_objective, 'complete'), (failing_objective, 'fail')])
def test_renewal_continues_until_submission_acknowledged(
        tmp_path, objective, kind):
    run_search(objective, tmp_path, submit_renewals=2, trials=2)

    records = read_events(tmp_path)
    names = [record['event'] for record in records]
    first_submit = names.index(kind)
    first_ack = names.index(kind + '_acknowledged')
    assert names[first_submit:first_ack].count('renew') >= 2
    assert 'acquire' not in names[first_submit:first_ack]
    submissions = [record for record in records if record['event'] == kind]
    assert len(submissions) == 2
    assert (submissions[0]['name'], submissions[0]['attempt_id']) == (
        TRIAL_NAME, 7)
    if kind == 'complete':
        assert submissions[0]['result'] == {'objective': 0.25, 'accuracy': 0.9}
        assert 'fail' not in names
    else:
        assert 'ValueError: invalid parameters' in submissions[0]['error']
        assert 'complete' not in names


def test_submission_error_is_not_reported_as_objective_failure(tmp_path):
    with pytest.raises(APIError) as raised:
        run_search(result_objective, tmp_path, submit_error=True, trials=2)
    assert raised.value.status == 503
    assert raised.value.code == 14
    names = event_names(tmp_path)
    assert names.count('complete') == 1
    assert names.count('acquire') == 1
    assert 'fail' not in names
    assert names[-1] == 'closed'


def test_acquisition_error_preserves_api_details_and_closes_client(tmp_path):
    with pytest.raises(APIError) as raised:
        run_search(result_objective, tmp_path, acquire_error=True)
    assert raised.value.status == 503
    assert raised.value.message == 'coordinator unavailable'
    assert raised.value.code == 14
    assert raised.value.details == [{'source': 'test'}]
    assert event_names(tmp_path) == [
        'constructed', 'entered', 'acquire', 'exited', 'closed']


def test_expired_assignment_is_never_executed(tmp_path):
    calls = []

    def objective(ctx, params):
        calls.append(ctx)
        return {'objective': 0}

    with pytest.raises(LeaseLostError):
        run_search(objective, tmp_path, lease_seconds=-1)
    assert calls == []
    names = event_names(tmp_path)
    assert 'renew' not in names
    assert 'complete' not in names
    assert 'fail' not in names


def test_wait_does_not_block_calling_event_loop(tmp_path):
    ticks = []

    async def ticker():
        for index in range(20):
            ticks.append(index)
            await asyncio.sleep(0.01)

    async def run():
        factory = ClientFactory(str(tmp_path), trials=0, wait_first=True)
        search_task = search(result_objective, SWEEP_ID,
                             client_factory=factory, poll_interval=0.1)
        await asyncio.gather(search_task, ticker())

    asyncio.run(run())
    assert len(ticks) == 20
    assert event_names(tmp_path).count('acquire') == 2


def test_background_crash_cancels_the_running_objective(tmp_path):
    contexts = []

    def objective(ctx, params):
        contexts.append(ctx)
        (tmp_path / 'started').touch()
        busy_wait(lambda: ctx.cancelled)
        (tmp_path / 'cleaned_up').touch()
        return {'objective': 0}

    with pytest.raises(gridy.WorkerError):
        run_search(objective, tmp_path, renewal='crash')
    assert contexts[0].cancelled
    assert (tmp_path / 'cleaned_up').exists()
    assert 'complete' not in event_names(tmp_path)


@pytest.mark.parametrize('interval', [0, -1, float('nan'), float('inf')])
def test_rejects_invalid_poll_interval(tmp_path, interval):
    with pytest.raises(ValueError):
        run_search(result_objective, tmp_path, poll_interval=interval)
    assert read_events(tmp_path) == []


def test_async_objective_runs_in_callers_thread(tmp_path):
    calls = []

    async def objective(ctx, params):
        await asyncio.sleep(0)
        assert threading.current_thread() is threading.main_thread()
        calls.append(os.getpid())
        return {'objective': 0.5}

    run_search(objective, tmp_path)
    assert calls == [os.getpid()]
    assert event_names(tmp_path).count('complete') == 1


def test_async_objective_polls_cancellation_after_yielding(tmp_path):
    contexts = []
    cleaned_up = []

    async def objective(ctx, params):
        contexts.append(ctx)
        (tmp_path / 'started').touch()
        try:
            async with asyncio.timeout(8):
                while True:
                    await asyncio.sleep(0.01)
                    ctx.raise_if_cancelled()
        finally:
            cleaned_up.append(ctx.reason)

    with pytest.raises(APIError) as raised:
        run_search(objective, tmp_path, renewal='stale')

    assert raised.value.status == 409
    assert contexts[0].cancelled
    assert cleaned_up == ['lease_lost']
    assert 'complete' not in event_names(tmp_path)
    assert 'fail' not in event_names(tmp_path)


def test_retained_context_does_not_consume_later_attempt_cancellation(
        tmp_path):
    contexts = []

    def objective(ctx, params):
        contexts.append(ctx)
        if params['index'] == 1:
            return {'objective': 0.5}
        previous = contexts[0]
        (tmp_path / 'started').touch()
        busy_wait(lambda: 'lease_lost' in event_names(tmp_path))
        for _ in range(10):
            assert not previous.cancelled
            assert previous.reason is None
            previous.raise_if_cancelled()
        busy_wait(lambda: ctx.cancelled)
        ctx.raise_if_cancelled()

    with pytest.raises(APIError) as raised:
        run_search(objective, tmp_path, trials=2, renewal='stale')

    assert raised.value.status == 409
    assert len(contexts) == 2
    assert not contexts[0].cancelled
    assert contexts[1].reason == 'lease_lost'
    records = read_events(tmp_path)
    completions = [record for record in records
                   if record['event'] == 'complete']
    assert [record['attempt_id'] for record in completions] == [7]
    assert 'fail' not in event_names(tmp_path)


@pytest.mark.parametrize('asynchronous', [False, True])
def test_search_runs_objectives_in_the_calling_background_thread(
        tmp_path, asynchronous):
    calls = []

    def objective(ctx, params):
        calls.append((os.getpid(), threading.current_thread()))
        return {'objective': len(calls)}

    async def async_objective(ctx, params):
        await asyncio.sleep(0)
        return objective(ctx, params)

    fn = async_objective if asynchronous else objective
    with ThreadPoolExecutor(max_workers=1) as executor:
        caller_thread = executor.submit(threading.current_thread).result()
        future = executor.submit(run_search, fn, tmp_path, trials=2)
        future.result(timeout=10)

    assert caller_thread is not threading.main_thread()
    assert calls == [(os.getpid(), caller_thread)] * 2
    names = event_names(tmp_path)
    assert names.count('complete_acknowledged') == 2
    assert names.count('closed') == 1


def test_search_task_cancellation_cleans_up_objective_and_worker(tmp_path):
    contexts = []
    cleaned_up = []

    async def run():
        started = asyncio.Event()

        async def objective(ctx, params):
            contexts.append(ctx)
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                assert ctx.cancelled
                assert ctx.reason == 'cancelled'
                cleaned_up.append(True)

        task = asyncio.create_task(search(
            objective, SWEEP_ID, client_factory=ClientFactory(str(tmp_path))))
        await asyncio.wait_for(started.wait(), timeout=8)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert cleaned_up == [True]
    assert contexts[0].cancelled
    names = event_names(tmp_path)
    assert 'complete' not in names
    assert 'fail' not in names
    assert names[-1] == 'closed'


def test_lease_loss_during_submission_cancels_it_without_acquiring_more(
        tmp_path):
    contexts = []

    def objective(ctx, params):
        contexts.append(ctx)
        (tmp_path / 'started').touch()
        return {'objective': 0.5}

    with pytest.raises(APIError) as raised:
        run_search(objective, tmp_path, renewal='stale', submit_renewals=1,
                   trials=2)
    assert raised.value.code == 10
    assert contexts[0].cancelled
    assert contexts[0].reason == 'lease_lost'
    names = event_names(tmp_path)
    assert names.count('complete') == 1
    assert 'complete_acknowledged' not in names
    assert names.count('acquire') == 1
    assert 'fail' not in names


@pytest.mark.parametrize('result', [
    0.25, None, {'objective': float('nan')}, {'objective': object()}])
def test_invalid_objective_result_is_reported_as_failure(tmp_path, result):
    def objective(ctx, params):
        return result

    run_search(objective, tmp_path)
    records = read_events(tmp_path)
    failures = [record for record in records if record['event'] == 'fail']
    assert len(failures) == 1
    assert failures[0]['error']
    assert 'complete' not in event_names(tmp_path)
    assert event_names(tmp_path).count('acquire') == 2


def test_unpicklable_factory_fails_to_start_without_leaking_worker(
        tmp_path, monkeypatch):
    socketpair = socket.socketpair
    connections = []

    def record_socketpair(*args, **kwargs):
        pair = socketpair(*args, **kwargs)
        connections.extend(pair)
        return pair

    monkeypatch.setattr(socket, 'socketpair', record_socketpair)

    def factory():
        return RecordingClient(ClientFactory(str(tmp_path)))

    with pytest.raises(gridy.WorkerError, match='Cannot start queue process'):
        asyncio.run(search(result_objective, SWEEP_ID, client_factory=factory))
    assert read_events(tmp_path) == []
    assert connections
    assert all(connection.fileno() == -1 for connection in connections)


@dataclass
class HTTPClientFactory:
    endpoint: str
    root: str

    def __call__(self):
        (Path(self.root) / 'client_pid').write_text(str(os.getpid()))
        return gridy.AsyncQueueClient(
            self.endpoint, worker_id='01ARZ3NDEKTSV4RRFFQ69G5FAV',
            token='worker-token', max_retries=0, timeout=2)


def test_queue_http_integration_with_main_thread_objective(tmp_path):
    requests = []
    lock = threading.Lock()
    acquisitions = 0

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def do_POST(self):
            nonlocal acquisitions
            payload = json.loads(self.rfile.read(
                int(self.headers['Content-Length'])))
            with lock:
                requests.append((self.path, self.headers['Authorization'],
                                 payload))
            expires_at = (datetime.now(UTC) + timedelta(
                seconds=0.3)).isoformat()
            if self.path.endswith(':acquire'):
                acquisitions += 1
                if acquisitions == 1:
                    response = {'assignment': {
                        'name': TRIAL_NAME, 'attempt_id': '7',
                        'params': {'lr': 0.01},
                        'lease_expires_at': expires_at}}
                else:
                    response = {'done': {}}
            elif self.path.endswith(':renewLease'):
                response = {'lease_expires_at': expires_at}
            else:
                response = {}
            body = json.dumps(response).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def renewals():
        with lock:
            return sum(path.endswith(':renewLease')
                       for path, _, _ in requests)

    def objective(ctx, params):
        assert threading.current_thread() is threading.main_thread()
        assert params == {'lr': 0.01}
        busy_wait(lambda: renewals() >= 2)
        ctx.raise_if_cancelled()
        return {'objective': 0.9, 'steps': 2}

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever,
                              kwargs={'poll_interval': 0.01}, daemon=True)
    thread.start()
    endpoint = f'http://127.0.0.1:{server.server_port}'
    try:
        asyncio.run(search(
            objective, SWEEP_ID,
            client_factory=HTTPClientFactory(endpoint, str(tmp_path))))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert not thread.is_alive()
    assert int((tmp_path / 'client_pid').read_text()) != os.getpid()
    assert renewals() >= 2
    acquire = [payload for path, _, payload in requests
               if path.endswith(':acquire')]
    assert len(acquire) == 2
    assert {payload['worker_id'] for payload in acquire} == {
        '01ARZ3NDEKTSV4RRFFQ69G5FAV'}
    assert acquire[0]['request_id'] != acquire[1]['request_id']
    assert all(auth == 'Bearer worker-token' for _, auth, _ in requests)
    complete = [payload for path, _, payload in requests
                if path.endswith(':complete')]
    assert complete == [{'attempt_id': '7',
                         'result': {'objective': 0.9, 'steps': 2}}]
    assert not any(path.endswith(':fail') for path, _, _ in requests)


def test_context_retains_first_cancellation_reason():
    ctx = gridy.Context(TRIAL_NAME, 7)
    ctx.raise_if_cancelled()
    with pytest.raises(AttributeError):
        ctx.name = 'other-trial'
    with pytest.raises(AttributeError):
        ctx.attempt_id = 8
    assert not ctx.cancelled
    assert ctx.reason is None
    ctx.cancel('lease_lost')
    ctx.cancel('shutdown')
    assert ctx.cancelled
    assert ctx.reason == 'lease_lost'
    with pytest.raises(gridy.CancelledError) as raised:
        ctx.raise_if_cancelled()
    assert raised.value.reason == 'lease_lost'


@pytest.mark.parametrize('kind', ['complete', 'fail'])
def test_terminal_acknowledgement_wins_over_concurrent_stale_renewal(
        tmp_path, kind):
    contexts = []

    def objective(ctx, params):
        contexts.append(ctx)
        if kind == 'fail':
            raise ValueError('invalid parameters')
        return {'objective': 0.5}

    run_search(objective, tmp_path, renewal='ack_stale', submit_renewals=1)
    assert len(contexts) == 1
    assert not contexts[0].cancelled
    assert contexts[0].reason is None
    names = event_names(tmp_path)
    assert 'stale_after_acknowledgement' in names
    assert kind + '_acknowledged' in names
    assert names.count('acquire') == 2


def test_search_uses_no_ipc_or_shutdown_threads(tmp_path):
    before = set(threading.enumerate())

    def objective(ctx, params):
        busy_wait(lambda: event_names(tmp_path).count('renew') >= 2)
        assert set(threading.enumerate()) == before
        ctx.raise_if_cancelled()
        return {'objective': 0.5}

    async def run():
        await search(objective, SWEEP_ID,
                     client_factory=ClientFactory(str(tmp_path)))
        # Inspect before asyncio.run shuts down a possible thread executor.
        assert set(threading.enumerate()) == before

    asyncio.run(run())
    records = read_events(tmp_path)
    assert records
    assert {record['event'] for record in records} >= {
        'constructed', 'acquire', 'renew', 'complete', 'closed'}
    assert all(record['threads'] == ['MainThread'] for record in records)


def test_large_assignment_and_result_cross_ipc_with_live_lease(tmp_path):
    payload_size = 4 * 1024 * 1024

    def objective(ctx, params):
        assert len(params['payload']) == payload_size
        assert params['payload'] == 'x' * payload_size
        ctx.raise_if_cancelled()
        return {'objective': 0.5, 'payload': params['payload']}

    run_search(objective, tmp_path, assignment_bytes=payload_size,
               lease_seconds=1, submit_renewals=2)
    records = read_events(tmp_path)
    completions = [record for record in records
                   if record['event'] == 'complete']
    assert len(completions) == 1
    assert completions[0]['result'] == {
        'objective': 0.5, 'payload': 'x' * payload_size}
    names = event_names(tmp_path)
    submitting = names.index('complete')
    acknowledged = names.index('complete_acknowledged')
    assert names[submitting:acknowledged].count('renew') >= 2
    assert 'fail' not in names


def test_retained_context_can_be_read_while_waiting_for_acknowledgement(
        tmp_path):
    contexts = []
    observations = []

    def objective(ctx, params):
        contexts.append(ctx)
        return {'objective': 0.5}

    async def run():
        task = asyncio.create_task(search(
            objective, SWEEP_ID,
            client_factory=ClientFactory(str(tmp_path), submit_renewals=2)))
        try:
            async with asyncio.timeout(8):
                while not task.done():
                    if contexts and 'complete' in event_names(tmp_path):
                        observations.append(
                            (contexts[0].cancelled, contexts[0].reason))
                    await asyncio.sleep(0.005)
                await task
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(run())
    assert observations
    assert all(state == (False, None) for state in observations)
    assert event_names(tmp_path).count('complete_acknowledged') == 1
