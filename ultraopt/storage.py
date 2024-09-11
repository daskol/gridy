"""Module storage contains domain specific definitions of task lifecycle and
its logging on filesystem for better durability and persistence.
"""

from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from json import dump, loads
from os import SEEK_END, SEEK_SET, PathLike, fsync
from typing import Any, Dict, Optional, TextIO

__all__ = ('Add', 'Eval', 'Record', 'State', 'Update', 'WAL')


def to_json(obj):
    if isinstance(obj, State):
        return obj.value
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return str(obj)


class State(Enum):
    """Enum State encodes feasible states for any task tracked with WAL.
    """

    SCHEDULED = 'scheduled'

    RUNNING = 'running'

    FINISHED = 'finished'


@dataclass
class Record:
    """Class Record defines common set of fields used in each record.
    """

    verb: str

    timestamp: datetime

    id: int

    state: State

    @staticmethod
    def from_dict(obj) -> 'Record':
        if (verb := obj.get('verb')) is None:
            raise RuntimeError
        elif not isinstance(verb, str):
            raise RuntimeError

        if (value := obj.get('timestamp')) is None:
            raise RuntimeError
        timestamp = datetime.fromisoformat(value)

        if (id := obj.get('id')) is None:
            raise RuntimeError
        elif not isinstance(id, int):
            raise RuntimeError

        if (value := obj.get('state')) is None:
            raise RuntimeError
        state = State(value)

        return Record(verb, timestamp, id, state)


@dataclass(init=False)
class Add(Record):

    stoch_params: Dict[str, Any]

    det_params: Dict[str, Any]

    opts: Dict[str, Any]

    def __init__(self, id: int, stoch_params: Optional[Dict[str, Any]] = None,
                 det_params: Optional[Dict[str, Any]] = None,
                 opts: Optional[Dict[str, Any]] = None,
                 timestamp: Optional[datetime] = None):
        timestamp = timestamp or datetime.now()
        super().__init__('add', timestamp, id, State.SCHEDULED)
        self.stoch_params = stoch_params or {}
        self.det_params = det_params or {}
        self.opts = opts or {}

    @staticmethod
    def from_dict(obj) -> 'Add':
        rec = Record.from_dict(obj)
        if rec.verb != 'add':
            raise RuntimeError
        if rec.state != State.SCHEDULED:
            raise RuntimeError

        if not isinstance(stoch_params := obj.get('stoch_params'), dict):
            raise RuntimeError
        if not isinstance(det_params := obj.get('det_params'), dict):
            raise RuntimeError
        if not isinstance(opts := obj.get('opts'), dict):
            raise RuntimeError

        return Add(rec.id, stoch_params, det_params, opts, rec.timestamp)


@dataclass
class Eval(Record):

    value: float

    metrics: Dict[str, Any]

    def __init__(self, id: int, value: float,
                 metrics: Optional[Dict[str, Any]] = None,
                 timestamp: Optional[datetime] = None):
        timestamp = timestamp or datetime.now()
        super().__init__('eval', timestamp, id, State.FINISHED)
        self.value = value
        self.metrics = metrics or {}

    @staticmethod
    def from_dict(obj) -> 'Eval':
        rec = Record.from_dict(obj)
        if rec.verb != 'eval':
            raise RuntimeError
        if rec.state != State.FINISHED:
            raise RuntimeError

        if not isinstance(value := obj.get('value'), (int, float)):
            raise RuntimeError
        if not isinstance(metrics := obj.get('metrics'), dict):
            raise RuntimeError

        return Eval(rec.id, float(value), metrics, rec.timestamp)


@dataclass
class Update(Record):
    """Class Update corresponds to changing state of a task.
    """

    reason: Optional[str] = None

    def __init__(self, id: int, state: State, reason: Optional[str] = None,
                 timestamp: Optional[datetime] = None):
        timestamp = timestamp or datetime.now()
        super().__init__('update', timestamp, id, state)
        self.reason = reason

    @staticmethod
    def from_dict(obj) -> 'Update':
        rec = Record.from_dict(obj)
        if rec.verb != 'update':
            raise RuntimeError

        if (reason := obj.get('reason')) is None:
            pass
        elif not isinstance(reason, str):
            raise RuntimeError('Expected type of reason field is str.')

        return Update(rec.id, rec.state, reason, rec.timestamp)


class WAL:
    """Class WAL implements a Write-Ahead Log pattern for tracking changes
    persistently. An instance of WAL should not be directly constucted. Instead
    of that, a suitable static method should be used.

    >>> with WAL.open('wal.jsonl') as wal:
    >>>     while (rec := wal.read()):
    >>>         ...
    >>>     rec = Eval(42, 3.1415926, {'elapsed': 228.0})
    >>>     wal.write(rec)
    """

    def __init__(self, fobj: TextIO, fsync: bool):
        self.fobj = fobj
        self.fobj.seek(0, SEEK_SET)
        self.fsync = fsync
        self.writing = False

    def __del__(self):
        self.close()

    def __enter__(self) -> 'WAL':
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    @classmethod
    def open(cls, path: PathLike, fsync: bool = False) -> 'WAL':
        """Class method open opens a WAL.

        :param path: Path to WAL on filesystem.
        :param fsync: Whether fsync is called after each write.
        :return: An instance of WAL.
        """
        return cls(open(path, 'a+'), fsync)

    def close(self):
        """Method close safely closes underlying file with forced syncing its
        content to disk.
        """
        if self.fobj:
            self.sync()
            self.fobj.close()
            self.fobj = None

    def read(self) -> Optional[Record]:
        """Method read reads a single record from WAL on filesystem. WAL is
        available for reading only until invocation of :py:meth:`write` method.
        As soon as method :py:meth:`write` is called, this method will return
        None.

        :return: Either an instance of Record-derived type or None on EOF and
                 in writing state.
        """
        if (line := self.fobj.readline()) == '':
            return

        obj = loads(line)

        if (verb := obj.get('verb')) is None:
            raise RuntimeError
        elif verb == 'add':
            return Add.from_dict(obj)
        elif verb == 'eval':
            try:
                return Eval.from_dict(obj)
            except Exception:
                print(obj)
                raise
        elif verb == 'update':
            return Update.from_dict(obj)
        else:
            return Record.from_dict(obj)

    def sync(self):
        self.fobj.flush()
        if self.fsync:
            fsync(self.fobj.fileno())

    def write(self, record: Record):
        """Method write serialized an instance of Record-derived type to
        filesystem. The first invocation of the method invalidates consequent
        calls of :py:meth:`read` method.

        :param record: A record to write to log.
        """
        if not self.writing:
            self.fobj.seek(0, SEEK_END)
            self.writing = True

        dump(obj=asdict(record),
             fp=self.fobj,
             ensure_ascii=False,
             indent=None,
             default=to_json)
        self.fobj.write('\n')
        self.sync()
