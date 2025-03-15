#
# Copyright 2024 Tabs Data Inc.
#

import io
import os
import platform
import shutil
from pathlib import Path
from sysconfig import get_platform

from setuptools import find_packages, setup
from setuptools.command.bdist_egg import bdist_egg as _bdist_egg
from setuptools.command.build import build as _build
from setuptools.command.sdist import sdist as _sdist
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel

# noinspection DuplicatedCode
if platform.python_implementation() != "CPython":
    raise RuntimeError("The Tabsdata package requires CPython to function correctly.")


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


def read_requirements(path):
    requirements = [
        line.strip()
        for line in read(path).split("\n")
        if not line.startswith(('"', "#", "-", "git+"))
    ]
    if os.environ.get("TD_IGNORE_CONNECTOR_REQUIREMENTS"):
        requirements = [
            requirement
            for requirement in requirements
            if not requirement.startswith("tabsdata")
        ]
    return requirements


profile = os.getenv("profile") or os.getenv("PROFILE", "debug")
if profile in ("", "dev"):
    profile = "debug"

REQUIRE_SERVER_BINARIES = os.getenv("REQUIRE_SERVER_BINARIES", "False").lower() in (
    "1",
    "true",
    "yes",
    "y",
    "on",
)

REQUIRE_THIRD_PARTY = os.getenv("REQUIRE_THIRD_PARTY", "False").lower() in (
    "1",
    "true",
    "yes",
    "y",
    "on",
)

print(f"Using Rust profile: '{profile}'")

td_target = os.getenv("td-target", "")

print(f"Using tabsdata target: '{td_target}'")

target_release_folder = os.path.join("target", td_target, profile)

print(f"Using tabsdata target release folder: '{target_release_folder}'")

base_binaries = [
    "apisrv",
    "bootloader",
    "importer",
    "supervisor",
    "tdserver",
    "transporter",
]

binaries = [
    binary
    for base in base_binaries
    for binary in (base, f"{base}.exe")
    if os.path.exists(os.path.join(target_release_folder, binary))
]

missing_binaries = [
    base
    for base in base_binaries
    if not any(
        os.path.exists(os.path.join(target_release_folder, binary))
        for binary in (base, f"{base}.exe")
    )
]

if missing_binaries and REQUIRE_SERVER_BINARIES:
    raise FileNotFoundError(
        "The following binaries are missing in "
        f"{target_release_folder}: {', '.join(missing_binaries)}"
    )

datafiles = [
    (
        get_binaries_folder(),
        [os.path.join(target_release_folder, binary) for binary in binaries],
    )
]

print(f"Including tabsdata binaries: {datafiles}")

# noinspection DuplicatedCode
print(f"Current path in setup is {Path.cwd()}")
variant_assets_folder = os.path.join("variant", "assets")
client_assets_folder = os.path.join("client", "td-sdk", "tabsdata", "assets")
print(f"Copying contents of {variant_assets_folder} to {client_assets_folder}")
if (
    not os.path.exists(os.path.join(variant_assets_folder, "manifest", "THIRD-PARTY"))
    and REQUIRE_THIRD_PARTY
):
    raise FileNotFoundError(
        f"The THIRD-PARTY file is missing in {variant_assets_folder}."
    )
shutil.copytree(variant_assets_folder, client_assets_folder, dirs_exist_ok=True)


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


os.makedirs(os.path.join("target", "python", "egg"), exist_ok=True)


setup(
    name="tabsdata",
    version=read(os.path.join("assets", "manifest", "VERSION")),
    description="Tabsdata is a publish-subscribe (pub/sub) server for tables.",
    long_description=read(
        os.path.join("variant", "assets", "manifest", "README-PyPi.md")
    ),
    long_description_content_type="text/markdown",
    license_files=(os.path.join("variant", "assets", "manifest", "LICENSE"),),
    author="Tabs Data Inc.",
    python_requires=">=3.12",
    install_requires=read_requirements("requirements.txt"),
    extras_require={
        "salesforce": read_requirements("requirements-salesforce.txt"),
        "test": read_requirements("requirements-dev.txt"),
    },
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
    packages=[
        *find_packages(
            where=os.path.join("client", "td-sdk"),
            exclude=[
                "tests",
                "tests*",
                "examples",
                "examples*",
                "local_dev",
                "local_dev*",
            ],
        ),
        # ToDo: this requires being revisited for a cleaner and more pythonic approach
        *find_packages(
            where=os.path.join("client", "td-lib"),
            exclude=[
                "tests",
                "tests.*",
                "*.tests",
                "*.tests.*",
                "tests*",
                "tests*.*",
                "*.tests*",
                "*.tests*.*",
                "examples",
                "examples.*",
                "*.examples",
                "*.examples.*",
                "examples*",
                "examples*.*",
                "*.examples*",
                "*.examples*.*",
            ],
        ),
        # ToDo: this requires being revisited for a cleaner and more pythonic approach
        *find_packages(
            where=os.path.join("extensions", "python", "td-lib"),
            exclude=[
                "tests",
                "tests.*",
                "*.tests",
                "*.tests.*",
                "tests*",
                "tests*.*",
                "*.tests*",
                "*.tests*.*",
                "examples",
                "examples.*",
                "*.examples",
                "*.examples.*",
                "examples*",
                "examples*.*",
                "*.examples*",
                "*.examples*.*",
            ],
        ),
    ],
    package_dir={
        "ta_interceptor": os.path.join(
            "client",
            "td-lib",
            "ta_interceptor",
        ),
        "td_features": os.path.join(
            "client",
            "td-lib",
            "td_features",
        ),
        "tabsdata": os.path.join(
            "client",
            "td-sdk",
            "tabsdata",
        ),
        "tabsserver": os.path.join(
            "client",
            "td-sdk",
            "tabsserver",
        ),
        "tabsserver.function_execution": os.path.join(
            "client",
            "td-sdk",
            "tabsserver",
            "function_execution",
        ),
        "td_interceptor": os.path.join(
            "extensions", "python", "td-lib", "td_interceptor"
        ),
    },
    package_data={
        "tabsdata": [
            os.path.join("examples", "*"),
            os.path.join("examples", "input", "*.csv"),
            os.path.join("assets", "manifest", "*"),
        ],
        "tabsserver": [
            "*.yaml",
            os.path.join("function_execution", "*.yaml"),
        ],
    },
    data_files=datafiles,
    entry_points={
        "console_scripts": [
            "td = tabsdata.cli.cli:cli",
            "tdmain = tabsserver.main:main",
            "tdvenv = tabsserver.pyenv_creation:main",
            "tdupgrade = tabsserver.server.upgrade:main",
        ]
    },
    include_package_data=True,
)
