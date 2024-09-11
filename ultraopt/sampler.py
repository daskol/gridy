import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from itertools import count
from operator import attrgetter
from os import PathLike
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Type

import numpy as np
from hyperopt.base import JOB_STATE_DONE, Domain, Trials
from hyperopt.tpe import suggest

from .storage import WAL, Add, Eval, State, Update


class Sampler:
    """Class Sampler is a base class and interface. Any sampler implementation
    should conform to.
    """

    def sample(self) -> Dict[str, Any]:
        """Sample hyperparameters from search space.

        :return: Dictionary of sample identifier (tid) and hyperparameters
                 itself.

        >>> sampler: Sampler = ...
        >>> sampler.sample()
        {
            'tid': 42,
            'params': {'alpha': 0.186, 'gamma': 0.320, 'seed': 3407},
        }
        """

    def submit(self, tid: int, value: float,
               params: Optional[Dict[str, Any]] = None):
        """Update sampler with evaluated hyperparameters. If optional params
        argument is specified then it tries to set objective value and upsert
        hyperparameters in advance. The params argument is primary for
        restoring state of sampler.

        :param tid: Trial identifier.
        :param value: Value of objective function.
        :param params: Optional hyperparameter point to insert.

        >>> sampler: Sampler = ...
        >>> sample = sampler.sample()
        >>> value = objective(**sample['params'])
        >>> sampler.submit(sample['tid'], value)

        >>> x, y = 1.0, 2.0
        >>> value = objective(x=x, y=y)
        >>> sampler: Sampler = ...
        >>> sampler.submit(0, value, {'x': x, 'y': y})
        >>> sample = sampler.sample()
        """


class HyperOptSampler(Sampler):
    """Class HyperOptSampler is a wrapper around hyperparameter optimization
    algorithms provided by hyperopt library.

    It is based on implementation of hyperopt 0.2.7.

    :param space: Search space.
    :param seed: Random seed.
    """

    def __init__(self, space, num_warmup_samples: int = 10,
                 seed: Optional[int] = None):
        self.num_warmup_samples = num_warmup_samples
        self.rng = np.random.default_rng(seed)
        self.trials = Trials()
        self.domain = Domain(fn=lambda: RuntimeError('unexpected'),
                             expr=space,
                             pass_expr_memo_ctrl=False)

    def sample(self):
        # Based on implementation of hyperopt.fmin.FMinIter.run() method.
        tids = self.trials.new_trial_ids(1)
        seed = self.rng.integers(2**31 - 1)
        trials = suggest(tids, self.domain, self.trials, seed,
                         n_startup_jobs=self.num_warmup_samples, verbose=True)

        # TODO: List `trials` can be empty. Why?
        if len(trials) != 1:
            raise RuntimeError('No trials!')
        self.trials.insert_trial_docs(trials)
        self.trials.refresh()

        trial = trials[0]
        params = {k: v[0] if len(v) else None
                  for k, v in trial['misc']['vals'].items()}
        return {'tid': trial['tid'], 'params': params}

    def submit(self, tid: int, value: float,
               params: Optional[Dict[str, Any]] = None):
        # Based on implementation of hyperopt.fmin.FMinIter.serial_evaluate()
        # method.

        def find_trial(tid):
            for trial in self.trials._dynamic_trials:
                if trial['tid'] == tid:
                    return trial

        # Find trial object and prepare update.
        trial = find_trial(tid)
        trial_update = {'result': {'loss': value}, 'state': JOB_STATE_DONE}

        # There is no trial with such tid and there is not hyperparameter.
        if trial is None and params is None:
            return

        # If params argument is given then try to upsert (insert if not exist
        # or update). In case of existing trial, it should be updated with
        # evaluation result.
        if trial is None and params is not None:
            # Create trial from scrtatch and insert it.
            trial = {'tid': tid,
                     'misc': {'tid': tid,
                              'vals': {k: [v] for k, v in params.items()},
                              'idxs': {k: [tid] for k, v in params.items()}}}
            trial.update(trial_update)
            self.trials._dynamic_trials.append(trial)
        else:
            trial.update(trial_update)  # In-place update.

        # Finally, update trials index.
        self.trials.refresh()


@dataclass
class Trial:
    """Class Trial helps to track dependencies of hyperparameters sample on
    determenistic paramters.
    """

    params: Dict[str, Any]

    scheduled: Set[int] = field(default_factory=set)

    finished: List[int] = field(default_factory=list)

    values: List[float] = field(default_factory=list)

    @property
    def done(self) -> bool:
        """Whether all determenistic search space is exhausted.
        """
        return self.scheduled == set(self.finished)

    @property
    def value(self) -> float:
        """Objective value averaged over tasks (determenistic samples).
        """
        if not self.values:
            return float('nan')
        return sum(self.values) / len(self.values)


class PersistentSampler(Sampler):
    """Class PersistentSampler implements Sampler interface which wraps a basic
    sampler with persistence layer based on write-ahead log (WAL) approach.
    """

    def __init__(self, space, parent: Sampler, wal: WAL,
                 timeout: Optional[int] = None):
        super().__init__()
        self.sampler = parent
        self.wal = wal
        self.next_task_id = count()  # Task ID sequence.

        # Queues to track state of tasks.
        self.scheduled = deque()
        self.running = {}
        self.finished = {}

        # Index of all trials. It maps tid's (Trial ID) to trial object (result
        # of Sampler.sample method).
        self.trials: Dict[int, Trial] = {}

        # Policy to manage stale tasks.
        if timeout:
            self.timeout = timedelta(seconds=timeout)
        else:
            self.timeout = None

        # Restore sampler state from WAL.
        self.replay()

        # TODO: Move to determenistic paramters out of sampler.
        self.seeds = (3407, 42, 0x3aa7d859)

    def __str__(self) -> str:
        args = ', '.join([
            f'scheduled={len(self.scheduled)}',
            f'running={len(self.running)}',
            f'finished={len(self.finished)}',
        ])
        return f'{self.__class__.__name__}({args})'

    @classmethod
    def open(cls, path: PathLike, space,
             sampler_cls: Type[Sampler] = HyperOptSampler):
        """Create sampler and restore its state from WAL on filesystem.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        wal = WAL.open(path)
        sampler = sampler_cls(space)
        return cls(space, sampler, wal)

    def replay(self):
        # Play event by event.
        while rec := self.wal.read():
            self.play(rec)

        # Restrore state of task id sequence.
        task_id = None
        if self.scheduled:
            task_id = max(self.scheduled, key=attrgetter('id')).id
        for container in (self.running, self.finished):
            if container:
                curr_task_id = max(list(container))
                if task_id is None:
                    task_id = curr_task_id
                else:
                    task_id = max(task_id, curr_task_id)

        if task_id is not None:
            self.next_task_id = count(task_id + 1)

    def play(self, rec):
        if isinstance(rec, Add):
            # Append to scheduled queue.
            self.scheduled.append(rec)

            # Add rec to trial dependencies.
            if not isinstance(tid := rec.opts.get('tid'), int):
                raise RuntimeError(f'Failed to play event: {rec}.')

            if tid not in self.trials:
                tasks = Trial(rec.stoch_params)
                tasks.scheduled.add(rec.id)
                self.trials[tid] = tasks
            else:
                self.trials[tid].scheduled.add(rec.id)
        elif isinstance(rec, Eval):
            # Find dependant trial.
            if (task := self.finished.get(rec.id)) is None:
                raise RuntimeError(f'Failed to play event: {rec}. '
                                   f'Task #{rec.id} it not in finished queue.')
            if not isinstance(tid := task.opts.get('tid'), int):
                raise RuntimeError(f'Failed to play event: {rec}.')
            if tid not in self.trials:
                raise RuntimeError(f'Missing trial: {rec}.')

            # Add evaluation results.
            tasks = self.trials[tid]
            tasks.finished.append(rec.id)
            tasks.values.append(rec.value)

            # Submit to sampler reduced objective value.
            if tasks.done:
                self.sampler.submit(tid, tasks.value, tasks.params)
        elif isinstance(rec, Update):
            # We just move items from one queue to another queue. There is no
            # validation and we fully rely on our implementation constraints.
            if rec.state == State.SCHEDULED:
                task = self.running.pop(rec.id)
                self.scheduled.append(task)
            elif rec.state == State.RUNNING:
                task = self.scheduled.popleft()
                self.running[task.id] = task
            elif rec.state == State.FINISHED:
                task = self.running.pop(rec.id)
                self.finished[task.id] = task
            else:
                raise RuntimeError(f'Unknown task state: {rec}.')
        else:
            logging.warning('unexpected record verb: %s', rec.verb)

    def sample(self):
        # If there is not tasks in scheduled queue then try to move stale tasks
        # from running queue to scheduled queue.
        if not self.scheduled and self.timeout is not None:
            # Find ids of stale tasks.
            threshold = datetime.now() - self.timeout
            tids = []
            for tid, task in self.running.items():
                if task.timestamp <= threshold:
                    tids.append(tid)
            # Change state of stale tasks.
            for tid in tids:
                task = self.running.pop(tid)
                self.scheduled.append(task)
                self.wal.write(Update(task.id, State.SCHEDULED))

        # If there is no tasks in scheduled queue then get the next trial with
        # sampler and enqueue task for the trial.
        if not self.scheduled:
            trial = self.sampler.sample()
            tasks = Trial(trial['params'])
            for i, seed in enumerate(self.seeds):
                # Enqueue task to scheduled queue and record enqueue event in
                # WAL. Task ID is generated sequentially. Trial ID is saved in
                # event attributes as well.
                task = Add(id=next(self.next_task_id),
                           stoch_params=trial['params'],
                           det_params={'seed': seed},
                           opts={'tid': trial['tid']})
                self.scheduled.append(task)
                self.wal.write(task)
                # Record relations between parent trial and child task.
                tasks.scheduled.add(task.id)
            # Add an entry to index of tasks for specific trial.
            self.trials[trial['tid']] = tasks

        # With non-empty scheduled queue, we dequeue the oldest item and place
        # to running queue then record event of changing task state in WAL.
        task: Add = self.scheduled.popleft()
        task.timestamp = datetime.now()
        self.running[task.id] = task
        self.wal.write(Update(task.id, State.RUNNING))

        # Prepare output to conform to Sampler interface.
        params = {**task.stoch_params, **task.det_params}
        return {'tid': task.id, 'params': params}

    def submit(self, tid: int, value: float):
        # There is an abuse of notation. Here, tid means task id rather than
        # trial id. Nevertheless, we can get trial id by task id and submit
        # reduced (averaged) objective value to sampler.
        if (task := self.running.pop(tid)) is None:
            raise RuntimeError(f'Failed to find task #{tid} in running queue.')

        # Change state of the task and record state changing event and
        # evaluation event.
        self.finished[task.id] = task
        self.wal.write(Update(tid, State.FINISHED))
        self.wal.write(Eval(tid, value))

        # Find parent trial, reduce (averaged) task values, and update sampler
        # with reduced value.
        if not isinstance(tid := task.opts.get('tid'), int):
            raise RuntimeError('Failed to find a trial by task: there is '
                               'no valid trial id (tid) in task '
                               f'#{task.id} options.')
        elif tid not in self.trials:
            raise RuntimeError('Failed to find a trial by task: missing trial '
                               f'with trial id {tid}.')

        # Partially resolve trial dependencies on tasks.
        tasks: Trial = self.trials.get(tid)
        tasks.finished.append(task.id)
        tasks.values.append(value)

        # If all dependencies are resolved (all tasks are finished) then we
        # reduced task values and submit an update to sampler.
        if tasks.done:
            self.sampler.submit(tid, tasks.value)
