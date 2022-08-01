from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from ultraopt.storage import WAL, Add, Record, State


class TestWAL(TestCase):

    def test_write(self):
        with TemporaryDirectory() as tmpdir:
            recs = [Add(0, {'gamma': 0.1}), Add(1, {'gamma': 1.0})]
            with WAL.open(Path(tmpdir) / 'wal.jsonl') as wal:
                for rec in recs:
                    wal.write(rec)

    def test_read_write(self):
        record = Record(verb='none',
                        timestamp=datetime.now(),
                        id=42,
                        state=State.RUNNING)

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / 'wal.jsonl'

            with WAL.open(path) as wal:
                wal.write(record)

            with WAL.open(path) as wal:
                rec1 = wal.read()
                rec2 = wal.read()

        self.assertIsNotNone(rec1)
        self.assertEqual(rec1, record)
        self.assertIsNone(rec2)
