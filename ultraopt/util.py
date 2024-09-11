from dataclasses import dataclass
from datetime import datetime
from os import PathLike
from typing import Any, Dict, List, Optional

from hyperopt import hp

try:
    from pandas import DataFrame
except ImportError:
    DataFrame = None

from .sampler import PersistentSampler

SPACE = {
    'alpha': hp.loguniform('alpha', -3, 0),
    'mu': 1.0,
    'gamma': hp.loguniform('gamma', -2, 0),
}


def load_objective_func():
    def objective(*, alpha, gamma):
        return alpha ** 2 + gamma ** 2
    return objective


def load_search_space():
    return SPACE


@dataclass
class Trial:

    trial_id: int

    task_id: int

    value: Optional[float]

    params: Dict[str, Any]

    state: str

    timestamp: datetime


@dataclass(repr=False)
class History:

    trials: List[Trial]

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(length={len(self.trials)})'

    @classmethod
    def open(cls, path: PathLike) -> 'History':
        space = SPACE  # TODO(@daskol): Restore state of sampler w/o sampler.
        sampler = PersistentSampler.open(path, space)
        trials = []

        # Add scheduled tasks to history first.
        for task in sampler.scheduled:
            trial = Trial(trial_id=task.opts['tid'],
                          task_id=task.id,
                          value=None,
                          params={**task.stoch_params, **task.det_params},
                          state='scheduled',
                          timestamp=task.timestamp)
            trials.append(trial)

        # Then add running and finished tasks to history object.
        for state, queue in [('running', sampler.running),
                             ('finished', sampler.finished)]:
            for task in queue.values():
                value = None
                if state == 'finished':
                    deps = sampler.trials.get(task.opts['tid'])
                    index = deps.finished.index(task.id)
                    value = deps.values[index]

                trial = Trial(trial_id=task.opts['tid'],
                              task_id=task.id,
                              value=value,
                              params={
                                  **task.stoch_params,
                                  **task.det_params
                              },
                              state=state,
                              timestamp=task.timestamp)
                trials.append(trial)

        return History(trials)

    def to_pandas(self) -> DataFrame:
        if DataFrame is None:
            raise RuntimeError('Package pandas is missing: run '
                               '"pip install pandas" '
                               'to install missing dependency.')

        df = DataFrame(self.trials) \
            .set_index(['trial_id', 'task_id']) \
            .sort_index()
        df['state'] = df.state.astype('category')
        return df
