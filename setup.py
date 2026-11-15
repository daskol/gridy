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

"""Build the Go coordinator into Gridy's platform-specific Python wheel."""

import os
import subprocess
from pathlib import Path

from setuptools import Distribution, setup
from setuptools.command.bdist_wheel import bdist_wheel
from setuptools.command.build_py import build_py


class BinaryDistribution(Distribution):
    """Install the package with its executable into platlib."""

    def has_ext_modules(self):
        return True


class BuildPy(build_py):
    """Generate the Go API and compile the coordinator into package data."""

    def run(self):
        super().run()

        self._root = Path(__file__).resolve().parent
        self._exec(['go', 'tool', 'buf', 'generate'])

        binary = Path(self.build_lib).resolve() / 'gridy/bin/gridy'
        binary.parent.mkdir(parents=True, exist_ok=True)
        self._exec(['go', 'build', '-buildvcs=false', '-o', str(binary),
                    '-trimpath', './cmd/gridy'])
        binary.chmod(0o755)

    def _exec(self, cmd: list[str]) -> None:
        env = os.environ | {'CGO_ENABLED': '0'}
        subprocess.run(cmd, cwd=self._root, env=env, check=True)


class BdistWheel(bdist_wheel):
    """Use a platform tag without tying the executable to a Python ABI."""

    def get_tag(self):
        _, _, platform = super().get_tag()
        return 'py3', 'none', platform


setup(distclass=BinaryDistribution,
      cmdclass={'build_py': BuildPy, 'bdist_wheel': BdistWheel})
