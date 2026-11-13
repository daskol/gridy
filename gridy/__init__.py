from importlib import import_module
from typing import TYPE_CHECKING

from gridy.client import (
    APIError, Assignment, AsyncClient, Done, ProtocolError, Wait)

if TYPE_CHECKING:
    from gridy.grid_sampler import GridSampler
    from ultraopt.worker import Client as RemoteSampler

__all__ = ('APIError', 'Assignment', 'AsyncClient', 'Done', 'GridSampler',
           'ProtocolError', 'RemoteSampler', 'Wait')

_LEGACY_EXPORTS = {
    'GridSampler': ('gridy.grid_sampler', 'GridSampler'),
    'RemoteSampler': ('ultraopt.worker', 'Client'),
    **{name: ('ultraopt', name) for name in (
        'Coordinator', 'History', 'HyperOptSampler', 'PersistentSampler',
        'Sampler', 'WAL', 'Worker', 'WorkerPool')},
}


def __getattr__(name):
    # The HTTP client can be imported without loading the legacy samplers.
    if name not in _LEGACY_EXPORTS:
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
    module, attribute = _LEGACY_EXPORTS[name]
    value = getattr(import_module(module), attribute)
    globals()[name] = value
    return value
