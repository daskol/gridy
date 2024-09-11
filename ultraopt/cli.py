"""
"""

import inspect
import logging
from argparse import ArgumentParser, FileType
from pathlib import Path
from sys import stderr

from . import server, worker
from .util import load_objective_func

LOG_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warn': logging.WARN,
    'error': logging.ERROR,
}


def help_():
    parser.print_help()


def serve(listen: str):
    host, port = listen.split(':', 1)
    port = int(port)
    server.run(host, port)


def show(data_dir: Path):
    print(data_dir, data_dir.absolute())


def submit():
    pass


def version_():
    print('ultraopt version {version}')  # TODO


def work(pool_size: int):
    worker.run(load_objective_func, pool_size)


def main():
    # Parse command line arguments. If no subcommand were run then show usage
    # and exit. We assume that only main parser (super command) has valid value
    # in func attribute.
    args = parser.parse_args()
    if args.func is None:
        parser.print_usage()
        return

    # Set up basic logging configuration.
    if (stream := args.log_output) is None:
        stream = stderr
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=LOG_LEVELS[args.log_level],
                        stream=stream)

    # Find CLI option or argument by parameter name of handling function.
    kwargs = {}
    spec = inspect.getfullargspec(args.func)
    for name in spec.args:
        kwargs[name] = getattr(args, name)

    # Invoke CLI command handler.
    args.func(**kwargs)


parser = ArgumentParser(description=__doc__)
parser.set_defaults(func=None)
parser.add_argument('--log-level', default='info', choices=sorted(LOG_LEVELS.keys()), help='set logger verbosity level')  # noqa: E501
parser.add_argument('--log-output', default=stderr, metavar='FILENAME', type=FileType('w'), help='set output file or stderr (-) for logging')  # noqa: E501

subparsers = parser.add_subparsers()

help_p = subparsers.add_parser('help', add_help=False, help='Show this message and exit.')  # noqa: E501
help_p.set_defaults(func=help_)

serve_p = subparsers.add_parser('serve', help='Run server to coordinate sampling and evaluation.')  # noqa: E501
serve_p.set_defaults(func=serve)
serve_p.add_argument('-d', '--data-dir', default='.')
serve_p.add_argument('-l', '--listen', default='0.0.0.0:8080', metavar='HOST:PORT')  # noqa: E501
serve_p.add_argument('-t', '--timeout')
serve_p.add_argument('-s', '--seed', nargs='*')

show_p = subparsers.add_parser('show', help='Show experiment progress.')  # noqa: E501
show_p.set_defaults(func=show)
show_p.add_argument('data_dir', default=Path(), type=Path, nargs='?')

submit_p = subparsers.add_parser('submit', help='Submit evaluations manually.')  # noqa: E501
submit_p.set_defaults(func=submit)
submit_p.add_argument('submission')

version_p = subparsers.add_parser('version', add_help=False, help='Show version information.')  # noqa: E501
version_p.set_defaults(func=version_)

work_p = subparsers.add_parser('work', help='Run worker to evaluate an objective.')  # noqa: E501
work_p.set_defaults(func=work)
work_p.add_argument('-c', '--connect', default='127.0.0.1:8080', metavar='HOST:PORT')  # noqa: E501
work_p.add_argument('-d', '--data-dir')
work_p.add_argument('-n', '--name')
work_p.add_argument('-p', '--pool-size', default=1, type=int)
work_p.add_argument('-t', '--timeout')
