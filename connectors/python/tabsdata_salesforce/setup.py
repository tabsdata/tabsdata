#
#  Copyright 2025 Tabs Data Inc.
#


import io
import os
import platform
from pathlib import Path
from sysconfig import get_platform

from setuptools import find_packages, setup
from setuptools.command.bdist_egg import bdist_egg as _bdist_egg
from setuptools.command.build import build as _build
from setuptools.command.sdist import sdist as _sdist
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel

# noinspection DuplicatedCode
TD_IGNORE_CONNECTOR_REQUIREMENTS = "TD_IGNORE_CONNECTOR_REQUIREMENTS"

TABSDATA_PACKAGES_PREFIX = "tabsdata_"


class CustomBuild(_build):
    def initialize_options(self):
        super().initialize_options()
        self.build_base = os.path.join("target", "python", "build")
        os.makedirs(self.build_base, exist_ok=True)


class CustomSDist(_sdist):
    def __init__(self, dist):
        super().__init__(dist)
        self.temp_dir = None
        self.dist_dir = None

    def initialize_options(self):
        super().initialize_options()
        self.dist_dir = os.path.join("target", "python", "dist")
        self.temp_dir = os.path.join("target", "python", "sdist")
        os.makedirs(self.temp_dir, exist_ok=True)


class CustomBDistWheel(_bdist_wheel):
    def __init__(self, dist):
        super().__init__(dist)
        self.dist_dir = None

    def initialize_options(self):
        super().initialize_options()
        self.dist_dir = os.path.join("target", "python", "dist")


class CustomBDistEgg(_bdist_egg):
    def __init__(self, dist):
        super().__init__(dist)
        self.build_base = None

    def initialize_options(self):
        super().initialize_options()
        self.dist_dir = os.path.join("target", "python", "dist")
        self.build_base = os.path.join("target", "python", "build")
        os.makedirs(self.build_base, exist_ok=True)


def read(*paths, **kwargs):
    with io.open(
        os.path.join(os.path.dirname(__file__), *paths),
        encoding=kwargs.get("encoding", "utf8"),
    ) as open_file:
        content = open_file.read().strip()
    return content


def get_platname():
    system = platform.system()
    architecture = platform.machine().lower()

    if system == "Linux":
        if architecture in ["x86_64", "amd64"]:
            return "manylinux1_x86_64"
        elif architecture in ["aarch64", "arm64"]:
            return "manylinux1_aarch64"
        else:
            return f"manylinux1_{architecture}"
    elif system == "Darwin":
        if architecture in ["aarch64", "arm64"]:
            return "macosx_11_0_arm64"
        elif architecture == "x86_64":
            return "macosx_10_15_x86_64"
        else:
            return f"macosx_11_0_{architecture}"
    else:
        return get_platform().replace("-", "_").replace(".", "_")


def get_binaries_folder():
    system = platform.system()
    if system == "Windows":
        return "Scripts"
    else:
        return "bin"


def read_requirements(path, visited=None):
    if visited is None:
        visited = set()
    path = Path(path).resolve()
    if path in visited:
        raise ValueError(f"Circular dependency detected: {path}")
    visited.add(path)
    requirements = []
    with path.open(encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith(("#", '"')):
                continue
            if line.startswith("-r"):
                included_path = line.split(maxsplit=1)[1]
                if not os.path.isabs(included_path):
                    included_path = path.parent / included_path
                requirements.extend(read_requirements(included_path, visited))
            elif not line.startswith(("-", "git+")):
                requirements.append(line)
    if os.environ.get("TD_IGNORE_CONNECTOR_REQUIREMENTS"):
        requirements = [
            requirement
            for requirement in requirements
            if not requirement.startswith(TABSDATA_PACKAGES_PREFIX)
        ]
    for requirement in requirements:
        print(f" - {requirement}")
    return requirements


# noinspection DuplicatedCode
if platform.python_implementation() != "CPython":
    raise RuntimeError("The Tabsdata package requires CPython to function correctly.")


setup(
    name="tabsdata_salesforce",
    version=read("tabsdata_salesforce/VERSION"),
    description="Tabsdata plugin to access Salesforce data.",
    long_description=read("tabsdata_salesforce/README-PyPi.md"),
    long_description_content_type="text/markdown",
    license_files=(
        os.path.join("..", "..", "..", "variant", "assets", "manifest", "LICENSE"),
    ),
    author="Tabs Data Inc.",
    python_requires=">=3.12",
    install_requires=[],
    extras_require={"deps": read_requirements("requirements.txt")},
    options={
        "bdist_wheel": {
            "python_tag": "py312",
            "plat_name": get_platname(),
        }
    },
    cmdclass={
        "build": CustomBuild,
        "sdist": CustomSDist,
        "bdist_wheel": CustomBDistWheel,
        "bdist_egg": CustomBDistEgg,
    },
    packages=find_packages(
        where=os.getcwd(),
    ),
    package_dir={
        "tabsdata_salesforce": "tabsdata_salesforce",
    },
)
