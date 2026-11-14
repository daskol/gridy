from typing import Any

import pytest

try:
    from gridy.grid_sampler import ExhaustedSpaceError, GridSampler, Sampler
except ImportError:
    pass


def exhaust(sampler: Sampler, limit=1024) -> list[Any]:
    samples = []
    for _ in range(limit):
        try:
            samples += [sampler.sample()]
        except ExhaustedSpaceError:
            break
    return samples


@pytest.mark.skip(reason='deprecation')
class TestGridSampler:

    def test_sample(self):
        k1 = '["optimizer", "batch_size"]'
        k2 = '["optimizer", "lr"]'
        space = {k1: [2, 4], k2: [0.001, 1e-05]}
        sampler = GridSampler(space)
        samples = exhaust(sampler)
        assert len(samples) == 4

        sample = samples[0]
        assert 'tid' in sample
        assert sample['tid'] == 0
        assert 'params' in sample
        assert isinstance(sample['params'], dict)

        assert sample['params'][k1] in space[k1]
        assert sample['params'][k2] in space[k2]
