# Copyright 2026 Daniel Bershatsky
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

from copy import deepcopy
from pathlib import Path

import pytest
from pytest import CaptureFixture, MonkeyPatch

from gridy import cli


def test_no_command_prints_usage(capsys):
    cli.main([])
    captured = capsys.readouterr()
    assert captured.out.startswith('usage:')
    assert captured.err == ''


@pytest.mark.parametrize('args', [['help'], ['--help']])
def test_help_lists_commands(args: list[str], capsys: CaptureFixture[str]):
    if args == ['--help']:
        with pytest.raises(SystemExit) as raised:
            cli.main(args)
        assert raised.value.code == 0
    else:
        cli.main(args)

    captured = capsys.readouterr()
    for command in ('export', 'help', 'serve', 'version'):
        assert command in captured.out
    assert captured.err == ''


def test_version_flag_matches_subcommand(monkeypatch: MonkeyPatch, capsys):
    monkeypatch.setattr(cli.parser, 'prog', 'gridy')

    cli.main(['version'])
    command = capsys.readouterr()

    with pytest.raises(SystemExit) as raised:
        cli.main(['--version'])
    flag = capsys.readouterr()

    assert raised.value.code == 0
    assert command.out == flag.out
    assert command.out == f'gridy {cli.__version__}\n'
    assert command.err == flag.err == ''


@pytest.mark.parametrize('args', [
    [],
    ['--help'],
    ['--dsn', 'state with spaces.db', 'space.toml'],
    ['space.toml', '--http-listen', '127.0.0.1:8080'],
    ['--unknown-go-flag', '--', '-search-space.toml'],
])
def test_serve_forwards_arguments_without_parsing(args, tmp_path, monkeypatch):
    binary = tmp_path / 'Go coordinator'
    binary.write_text('#!/bin/sh\nexit 0\n')
    binary.chmod(0o755)
    monkeypatch.setenv('GRIDY_BINARY', str(binary))
    executions = []

    def execute(path, argv):
        executions.append((path, argv))

    monkeypatch.setattr(cli.os, 'execv', execute)
    cli.main(['serve', *args])
    assert executions == [(binary, [str(binary), 'serve', *args])]


@pytest.mark.parametrize('args', [
    [],
    ['--help'],
    ['--dsn', 'state with spaces.db', 'space.toml', '--', '-literal.toml'],
    ['--log-level', 'go-specific-level', '--help'],
])
def test_global_options_before_serve_preserve_forwarded_arguments(
    args: list[str], tmp_path: Path, monkeypatch: MonkeyPatch,
):
    parser = deepcopy(cli.parser)
    parser.add_argument('--xxx-level', choices=['debug', 'info'])
    parser.add_argument('--x-config')
    monkeypatch.setattr(cli, 'parser', parser)

    binary = tmp_path / 'Go coordinator'
    binary.write_text('#!/bin/sh\nexit 0\n')
    binary.chmod(0o755)
    monkeypatch.setenv('GRIDY_BINARY', str(binary))
    executions = []

    def execute(path, argv):
        executions.append((path, argv))

    monkeypatch.setattr(cli.os, 'execv', execute)
    cli.main(['--x-config', 'serve', '--xxx-level', 'debug', 'serve', *args])

    assert executions == [(binary, [str(binary), 'serve', *args])]


@pytest.mark.parametrize('args', [
    ['--help'],
    ['--dsn', 'state.db', 'space.toml', '--http-listen', '127.0.0.1:8080'],
    ['--unknown-go-flag', '--', '-search-space.toml'],
])
def test_parser_captures_serve_arguments_directly(args):
    ns = cli.parser.parse_args(['serve', *args])
    assert ns.func is cli.serve
    assert ns.args == args


def test_serve_missing_binary_reports_actionable_error(
    tmp_path: Path, monkeypatch: MonkeyPatch, capsys: CaptureFixture[str],
):
    missing = tmp_path / 'missing-gridy'
    monkeypatch.setenv('GRIDY_BINARY', str(missing))

    with pytest.raises(SystemExit) as raised:
        cli.main(['serve'])

    captured = capsys.readouterr()
    assert raised.value.code == 1
    assert captured.out == ''
    assert 'GRIDY_BINARY' in captured.err
    assert 'Traceback' not in captured.err
