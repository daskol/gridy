import numpy as np

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from hyperopt import Trials, fmin, hp, tpe

from ultraopt.sampler import HyperOptSampler, PersistentSampler
from ultraopt.storage import WAL


class TestHyperOptSampler(TestCase):

    SPACE = {
        'alpha': hp.loguniform('alpha', -3, 0),
        'gamma': hp.loguniform('gamma', -2, 0),
    }

    def test_sample(self):
        # Obtain trail from vanily hyperopt sampler.
        trial = fmin(fn=lambda x: 0.0,
                     space=TestHyperOptSampler.SPACE,
                     algo=tpe.suggest,
                     max_evals=1,
                     rstate=np.random.default_rng(42),
                     show_progressbar=False,
                     verbose=False)

        # Sample from wrapped hyperopt sampler.
        sampler = HyperOptSampler(seed=42)
        sample = sampler.sample()

        # Compare samples.
        self.assertDictEqual(trial, sample['params'])

    def test_submit(self):
        # Sample hyperparameters with hyperopt public API.
        cache = Trials()
        fmin(fn=lambda x: 0.0,
             space=TestHyperOptSampler.SPACE,
             algo=tpe.suggest,
             trials=cache,
             max_evals=2,
             rstate=np.random.default_rng(42),
             show_progressbar=False,
             verbose=False)
        reference = [{k: v[0] for k, v in el['misc']['vals'].items()}
                     for el in cache.trials]

        # Sample hyperparameters with wrapped hyperopt sampler.
        trials = []
        sampler = HyperOptSampler(seed=42)
        trial = sampler.sample()
        trials.append(trial)
        sampler.submit(trial['tid'], 0.0)
        trial = sampler.sample()
        trials.append(trial)
        params = [trial['params'] for trial in trials]

        # Verify identity.
        self.assertEqual(len(reference), len(params))
        for i, (ref, item) in enumerate(zip(reference, params)):
            self.assertDictEqual(ref, item, f'Difference in sample #{i}.')


class TestPersistentSampler(TestCase):

    SPACE = {
        'alpha': hp.loguniform('alpha', -3, 0),
        'gamma': hp.loguniform('gamma', -2, 0),
    }

    def test_sanity(self):
        with TemporaryDirectory() as tmpdir:
            tmpdir = '/tmp'
            with WAL.open(Path(tmpdir) / 'wal.jsonl') as wal:
                sampler = PersistentSampler(self.SPACE, HyperOptSampler(), wal)
                sampler.sample()
                sampler.sample()
                print(sampler.scheduled)
                print(sampler.running)
                print(sampler.finished)
