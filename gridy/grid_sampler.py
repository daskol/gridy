from itertools import product
from typing import Any

from ultraopt import Sampler


class ExhaustedSpaceError(Exception):
    pass


class GridSampler(Sampler):

    def __init__(self, space: dict[str, Any]):
        axes, subspaces = zip(*space.items())
        self.space = space
        self.axes = axes
        self.enumerated_points = enumerate(product(*subspaces))
        self.trials: dict[int, dict[str, Any]] = {}
        self.values: dict[int, dict[str, Any]] = {}

    def sample(self) -> dict[str, Any]:
        try:
            tid, point = next(self.enumerated_points)
        except StopIteration as e:
            raise ExhaustedSpaceError from e
        params = {k: v for k, v in zip(self.axes, point)}
        if tid in self.trials:
            raise KeyError(
                f'Trials index already has a trial with key tid={tid}.')
        trial = {'tid': tid, 'params': params}
        self.trials[tid] = trial
        return trial

    def submit(self, tid: int, value: float,
               params: dict[str, Any] | None = None):
        # TODO(@daskol): Failover? Submittion has no influence in general on
        # grid enumeration but maybe we can optimize sampling path?
        trial = {'tid': tid, 'value': value}
        if params is not None:
            trial['params'] = params
        self.trials[tid] = trial
