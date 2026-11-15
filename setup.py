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

import json
import os
import shlex
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from types import MappingProxyType
from typing import ClassVar, Self

from setuptools import Distribution, setup
from setuptools.command.bdist_wheel import bdist_wheel
from setuptools.command.build_py import build_py


class _GoPlatformMappings:
    """Keep platform constants separate from the context's instance fields."""

    __slots__ = ()

    GOOS: ClassVar[Mapping[str, str]] = MappingProxyType({
        'linux': 'linux',
        'darwin': 'macosx_13_0',
    })

    GOARCH: ClassVar[Mapping[tuple[str, str], str]] = MappingProxyType({
        ('linux', 'amd64'): 'x86_64',
        ('linux', 'arm64'): 'aarch64',
        ('darwin', 'amd64'): 'x86_64',
        ('darwin', 'arm64'): 'arm64',
    })

    ARCHFLAGS: ClassVar[Mapping[str, str]] = MappingProxyType({
        'x86_64': 'amd64',
        'arm64': 'arm64',
    })


@dataclass(frozen=True, slots=True)
class GoBuildContext(_GoPlatformMappings):
    """Run Go commands for the build host or the selected wheel target."""

    root: Path
    env: Mapping[str, str]
    host: tuple[str, str]
    target: tuple[str, str]

    @property
    def goarch(self) -> str:
        return self.GOARCH[self.target]

    @property
    def goos(self) -> str:
        return self.GOOS[self.target[0]]

    @property
    def platform(self) -> str:
        return f'{self.goos}_{self.goarch}'

    @classmethod
    def from_root_dir(cls, root_dir: Path) -> Self:
        """Capture the environment and resolve Go settings once for a build."""
        root = root_dir.resolve()
        env = os.environ | {'CGO_ENABLED': '0'}
        if not (root / 'go.mod').is_file():
            raise FileNotFoundError(
                'Missing go.mod: include MANIFEST.in and the Go sources in '
                'the source distribution.')

        result = subprocess.run(
            ['go', 'env', '-json', 'GOOS', 'GOARCH', 'GOHOSTOS', 'GOHOSTARCH'],
            cwd=root, env=env, capture_output=True, text=True, check=True)
        go = json.loads(result.stdout)

        # cibuildwheel selects a macOS architecture through ARCHFLAGS. An
        # explicit GOARCH takes precedence for manual cross-compilation.
        if go['GOOS'] == 'darwin' and not env.get('GOARCH'):
            flags = shlex.split(env.get('ARCHFLAGS', ''))
            archs = {flags[i + 1] for i, flag in enumerate(flags[:-1])
                     if flag == '-arch'}
            if archs:
                if len(archs) != 1 or not archs <= cls.ARCHFLAGS.keys():
                    raise ValueError(
                        'Build separate x86_64 and arm64 wheels: '
                        'universal2 executables are not supported.')
                go['GOARCH'] = cls.ARCHFLAGS[archs.pop()]

        host = go['GOHOSTOS'], go['GOHOSTARCH']
        target = go['GOOS'], go['GOARCH']
        if target[0] not in cls.GOOS or target not in cls.GOARCH:
            raise ValueError(
                f'Unsupported Go wheel target: {target[0]}/'
                f'{target[1]}. Use linux or darwin with amd64 or arm64.')
        return cls(root, MappingProxyType(env), host, target)

    def run(self, cmd: list[str], *, host: bool = False):
        goos, goarch = self.host if host else self.target
        env = self.env | {'GOOS': goos, 'GOARCH': goarch}
        return subprocess.run(cmd, cwd=self.root, env=env, check=True)


def get_go_build_context(root_dir: Path | None = None) -> GoBuildContext:
    if root_dir is None:
        root = Path(__file__).resolve().parent
    else:
        root = root_dir
    return GoBuildContext.from_root_dir(root)


class BinaryDistribution(Distribution):
    """Install the package with its executable into platlib."""

    @cached_property
    def go_build_context(self) -> GoBuildContext:
        """Share one Go environment snapshot across this build's commands."""
        return get_go_build_context()

    def has_ext_modules(self):
        return True


class BuildPy(build_py):
    """Generate the Go API and compile the coordinator into package data."""

    def run(self):
        super().run()

        context = self.distribution.go_build_context
        context.run(['go', 'tool', 'buf', 'generate'], host=True)

        binary = Path(self.build_lib).resolve() / 'gridy/bin/gridy'
        binary.parent.mkdir(parents=True, exist_ok=True)
        context.run(['go', 'build', '-buildvcs=false', '-o', str(binary),
                     '-trimpath', './cmd/gridy'])
        binary.chmod(0o755)


class BdistWheel(bdist_wheel):
    """Use a platform tag without tying the executable to a Python ABI."""

    def get_tag(self):
        return 'py3', 'none', self.distribution.go_build_context.platform


setup(distclass=BinaryDistribution,
      cmdclass={'build_py': BuildPy, 'bdist_wheel': BdistWheel})
