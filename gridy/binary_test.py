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

"""Check executable selection when Go and Python commands share a name."""

import sys
from pathlib import Path

import pytest
from pytest import MonkeyPatch

from gridy import binary as gridy_binary
from gridy.binary import find_binary

_LAUNCHER = b'#!/usr/bin/python\nfrom gridy.cli import main\nmain()\n'
_BINARY = b'\x7fELF\x00gridy'


def executable(path: Path, data: bytes = _BINARY) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    path.chmod(0o755)
    return path


@pytest.fixture(autouse=True)
def clear_override(tmp_path, monkeypatch):
    monkeypatch.delenv('GRIDY_BINARY', raising=False)
    monkeypatch.setattr(gridy_binary, '_BUNDLED_BINARY',
                        tmp_path / 'package/bin/gridy')


def test_finds_bundled_binary_without_path(monkeypatch):
    binary = executable(gridy_binary._BUNDLED_BINARY)
    monkeypatch.setenv('PATH', '')
    resolved = find_binary()
    assert resolved == binary


def test_bundled_binary_takes_precedence_over_path(tmp_path, monkeypatch):
    binary = executable(gridy_binary._BUNDLED_BINARY)
    executable(tmp_path / 'bin/gridy')
    monkeypatch.setenv('PATH', str(tmp_path / 'bin'))
    resolved = find_binary()
    assert resolved == binary


@pytest.mark.parametrize('data, mode', [
    (_BINARY, 0o644),
    (_LAUNCHER, 0o755),
])
def test_invalid_bundled_binary_falls_back(data, mode, tmp_path, monkeypatch):
    bundled = executable(gridy_binary._BUNDLED_BINARY, data)
    bundled.chmod(mode)
    binary = executable(tmp_path / 'go/gridy')
    monkeypatch.setenv('PATH', str(binary.parent))
    resolved = find_binary()
    assert resolved == binary


def test_module_invocation_skips_installed_launcher(tmp_path, monkeypatch):
    launcher = executable(tmp_path / 'python/gridy', _LAUNCHER)
    binary = executable(tmp_path / 'go/gridy')
    monkeypatch.setattr(sys, 'argv', ['gridy/__main__.py', 'serve'])
    monkeypatch.setenv('PATH', f'{launcher.parent}:{binary.parent}')
    resolved = find_binary()
    assert resolved == binary


def test_skips_symlink_to_running_launcher(tmp_path, monkeypatch):
    launcher = executable(tmp_path / 'launcher', b'#!/usr/bin/python\n')
    alias = tmp_path / 'bin/gridy'
    alias.parent.mkdir()
    alias.symlink_to(launcher)
    binary = executable(tmp_path / 'go/gridy')
    monkeypatch.setattr(sys, 'argv', [str(launcher)])
    monkeypatch.setenv('PATH', f'{alias.parent}:{binary.parent}')
    resolved = find_binary()
    assert resolved == binary


def test_explicit_override_takes_precedence(tmp_path, monkeypatch):
    binary = executable(tmp_path / 'custom/gridy-server')
    executable(gridy_binary._BUNDLED_BINARY)
    executable(tmp_path / 'bin/gridy')
    monkeypatch.setenv('GRIDY_BINARY', str(binary))
    monkeypatch.setenv('PATH', str(tmp_path / 'bin'))
    resolved = find_binary()
    assert resolved == binary


@pytest.mark.parametrize('configured', ['', 'missing', 'not-executable'])
def test_invalid_override_does_not_fall_back(configured: str, tmp_path: Path,
                                             monkeypatch: MonkeyPatch):
    executable(gridy_binary._BUNDLED_BINARY)
    executable(tmp_path / 'bin/gridy')
    (tmp_path / 'bin/not-executable').write_text('not executable')
    monkeypatch.setenv('PATH', str(tmp_path / 'bin'))
    monkeypatch.setenv('GRIDY_BINARY', configured)
    with pytest.raises(FileNotFoundError, match='GRIDY_BINARY'):
        find_binary()


def test_override_rejects_python_launcher(tmp_path, monkeypatch):
    launcher = executable(tmp_path / 'gridy', _LAUNCHER)
    monkeypatch.setattr(sys, 'argv', ['gridy/__main__.py'])
    monkeypatch.setenv('GRIDY_BINARY', str(launcher))
    with pytest.raises(ValueError, match='Python CLI'):
        find_binary()


def test_missing_binary_suggests_override(tmp_path, monkeypatch):
    monkeypatch.setenv('PATH', str(tmp_path))
    with pytest.raises(FileNotFoundError, match='GRIDY_BINARY'):
        find_binary()


def test_skips_nonexecutable_files(tmp_path, monkeypatch):
    nonexecutable = executable(tmp_path / 'plain/gridy')
    nonexecutable.chmod(0o644)
    binary = executable(tmp_path / 'go/gridy')
    monkeypatch.setenv('PATH', f'{nonexecutable.parent}:{binary.parent}')
    resolved = find_binary()
    assert resolved == binary
