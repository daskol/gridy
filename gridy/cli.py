# Copyright 2024 Daniel Bershatsky
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Command-line interface for the Gridy client and coordinator."""

import os
import sys
from argparse import REMAINDER, ArgumentParser, Namespace
from asyncio import run
from importlib.metadata import PackageNotFoundError, version as package_version
from inspect import iscoroutinefunction
from urllib.parse import urlsplit

from aiohttp import ClientError

from gridy.binary import find_binary
from gridy.export import export_database, export_uri

try:
    __version__ = package_version('gridy')
except PackageNotFoundError:
    __version__ = 'unknown'


class CommandParser(ArgumentParser):
    """Capture forwarded command arguments without parsing their options."""

    def parse_known_args(self, args=None, namespace=None):
        if not self.get_default('forward_args'):
            return super().parse_known_args(args, namespace)

        # The parent parser has already consumed global options and selected
        # this subcommand. Only initialize defaults and keep its arguments
        # opaque.
        ns, remaining = super().parse_known_args([], namespace)
        ns.args = list(sys.argv[1:] if args is None else args)
        return ns, remaining


def help_(ns: Namespace) -> None:
    """Show the available commands."""
    parser.print_help()


def version(ns: Namespace) -> None:
    """Show the Python package version."""
    print('gridy', __version__)


def serve(ns: Namespace) -> None:
    """Run the Go coordinator with its original arguments."""
    binary = find_binary()
    os.execv(binary, [str(binary), 'serve', *ns.args])


async def export(ns: Namespace) -> None:
    """Export completed trials from one sweep as JSONL."""
    scheme = urlsplit(ns.source).scheme
    if scheme in ('http', 'https', 'unix'):
        await export_uri(ns.source, ns.output, sweep=ns.sweep)
    elif not scheme or '://' not in ns.source or scheme == 'file':
        export_database(ns.source, ns.output, sweep=ns.sweep)
    else:
        raise ValueError(f'Unsupported source URI scheme: {scheme}.')


def main(argv: list[str] | None = None) -> None:
    ns = parser.parse_args(argv)
    if ns.func is None:
        parser.print_usage()
        return

    try:
        if iscoroutinefunction(ns.func):
            code = run(ns.func(ns))
        else:
            code = ns.func(ns)
    except (ClientError, OSError, RuntimeError, ValueError) as e:
        parser.exit(1, f'{parser.prog}: {e}\n')
    except KeyboardInterrupt:
        sys.exit(130)

    if code is not None:
        sys.exit(code)


parser = ArgumentParser(description=__doc__)
parser.set_defaults(func=None)
parser.add_argument('--version', action='version',
                    version=f'%(prog)s {__version__}')

subparsers = parser.add_subparsers(parser_class=CommandParser)

# Describe subcommand `help`.
p_help = subparsers.add_parser(
    'help', add_help=False, help='show this message and exit')
p_help.set_defaults(func=help_)

# Describe subcommand `version`.
p_version = subparsers.add_parser(
    'version', add_help=False, help='show the Python package version')
p_version.set_defaults(func=version)

# Describe subcommand `serve`.
p_serve = subparsers.add_parser(
    'serve', add_help=False, description=serve.__doc__,
    help='run the Go coordinator (arguments are forwarded unchanged)')
p_serve.add_argument('args', nargs=REMAINDER, help='Go coordinator arguments')
p_serve.set_defaults(func=serve, forward_args=True)

# Describe subcommand `export`.
p_export = subparsers.add_parser(
    'export', description=export.__doc__,
    help='export completed trials as JSONL')
p_export.add_argument('source', metavar='URI|DB_PATH',
                      help='HTTP(S) or Unix socket URI')
p_export.add_argument('output', metavar='OUTPUT',
                      help='output JSONL file, or - for stdout')
p_export.add_argument('-s', '--sweep', metavar='ID',
                      help='sweep identifier (optional if only one exists)')
p_export.set_defaults(func=export)
