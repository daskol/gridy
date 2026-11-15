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

"""Find Gridy's Go executable without recursing into its Python CLI."""

import os
import re
import sys
from pathlib import Path
from shutil import which

_CONSOLE_SCRIPT = re.compile(
    rb'\b(?:from\s+gridy\.cli\s+import\b|import\s+gridy\.cli\b)')
_BUNDLED_BINARY = Path(__file__).with_name('bin') / 'gridy'


def _is_launcher(path: Path) -> bool:
    """Recognize the running launcher and generated console-script copies."""
    if sys.argv and Path(sys.argv[0]).is_file():
        try:
            if path.samefile(sys.argv[0]):
                return True
        except OSError:
            pass

    try:
        with path.open('rb') as stream:
            prefix = stream.read(4096)
    except OSError:
        return False
    return prefix.startswith(b'#!') and bool(_CONSOLE_SCRIPT.search(prefix))


def find_binary() -> Path:
    """Resolve `GRIDY_BINARY`, the bundled executable, or gridy on PATH."""
    if (configured := os.environ.get('GRIDY_BINARY')) is not None:
        if not configured or (resolved := which(configured)) is None:
            raise FileNotFoundError(
                f'GRIDY_BINARY does not name an executable: {configured!r}.')

        candidate = Path(resolved)
        if _is_launcher(candidate):
            raise ValueError(
                'Environment variable GRIDY_BINARY points recursively to the '
                'Python CLI: set it to the Go binary instead.')
        return candidate.resolve()

    if (_BUNDLED_BINARY.is_file() and
        os.access(_BUNDLED_BINARY, os.X_OK) and
        not _is_launcher(_BUNDLED_BINARY)):
        return _BUNDLED_BINARY.resolve()

    for directory in os.get_exec_path():
        candidate = Path(directory) / 'gridy'
        if (candidate.is_file() and
            os.access(candidate, os.X_OK) and
            not _is_launcher(candidate)):
            return candidate.resolve()

    raise FileNotFoundError(
        'Gridy Go binary was not found in the package or on PATH. '
        'Set GRIDY_BINARY to its path.')
