#
# Copyright 2024 Tabs Data Inc.
#

import io
import os
import platform
import shutil
from pathlib import Path
from sysconfig import get_platform

import psutil
from setuptools import find_packages, setup
from setuptools.command.bdist_egg import bdist_egg as _bdist_egg
from setuptools.command.build import build as _build
from setuptools.command.sdist import sdist as _sdist
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel


# noinspection DuplicatedCode
def root_folder() -> str:
    current_folder = Path(os.getenv("PWD", psutil.Process().cwd()))
    print(f"📁 Current setup folder is: {current_folder}")
    while True:
        root_file = Path(
            os.path.join(
                current_folder,
                ".root",
            )
        )
        root_file_exists = root_file.exists() and root_file.is_file()
        if root_file_exists:
            print(f"🗂️ Root project folder for setup is: {current_folder}")
            return current_folder
        else:
            parent_folder = current_folder.parent
            if current_folder == parent_folder:
                print(
                    "☢️️ "
                    "Root folder is unreachable from current setup folder! "
                    "Defaulting to '.'"
                )
                return os.path.join(
                    ".",
                )
            current_folder = parent_folder


# noinspection DuplicatedCode
ROOT = root_folder()
print(f"ROOT folder for setup is: {ROOT}")

TABSDATA_PACKAGES_PREFIX = "tabsdata_"

REQUIRE_SERVER_BINARIES = "REQUIRE_SERVER_BINARIES"
REQUIRE_THIRD_PARTY = "REQUIRE_THIRD_PARTY"
TD_IGNORE_CONNECTOR_REQUIREMENTS = "TD_IGNORE_CONNECTOR_REQUIREMENTS"
TD_SKIP_NON_EXISTING_ASSETS = "TD_SKIP_NON_EXISTING_ASSETS"

TRUE_VALUES = {"1", "true", "yes", "y", "on"}

require_server_binaries = (
    os.getenv(
        REQUIRE_SERVER_BINARIES,
        "False",
    ).lower()
    in TRUE_VALUES
)
require_third_party = (
    os.getenv(
        REQUIRE_THIRD_PARTY,
        "False",
    ).lower()
    in TRUE_VALUES
)
ignore_connector_requirements = (
    os.getenv(
        TD_IGNORE_CONNECTOR_REQUIREMENTS,
        "True",
    ).lower()
    in TRUE_VALUES
)
skip_non_existing_assets = (
    os.getenv(
        TD_SKIP_NON_EXISTING_ASSETS,
        "True",
    ).lower()
    in TRUE_VALUES
)

THIRD_PARTY = "THIRD-PARTY"

BANNER = "BANNER"
LICENSE = "LICENSE"


# noinspection DuplicatedCode
class CustomBuild(_build):
    def initialize_options(self):
        super().initialize_options()
        self.build_base = os.path.join(
            "target",
            "python",
            "build",
        )
        os.makedirs(self.build_base, exist_ok=True)


class CustomSDist(_sdist):
    def __init__(self, dist):
        super().__init__(dist)
        self.temp_dir = None
        self.dist_dir = None

    def initialize_options(self):
        super().initialize_options()
        self.dist_dir = os.path.join(
            "target",
            "python",
            "dist",
        )
        self.temp_dir = os.path.join(
            "target",
            "python",
            "sdist",
        )
        os.makedirs(self.temp_dir, exist_ok=True)


class CustomBDistWheel(_bdist_wheel):
    def __init__(self, dist):
        super().__init__(dist)
        self.dist_dir = None

    def initialize_options(self):
        super().initialize_options()
        self.dist_dir = os.path.join(
            "target",
            "python",
            "dist",
        )


class CustomBDistEgg(_bdist_egg):
    def __init__(self, dist):
        super().__init__(dist)
        self.build_base = None

    def initialize_options(self):
        super().initialize_options()
        self.dist_dir = os.path.join(
            "target",
            "python",
            "dist",
        )
        self.build_base = os.path.join(
            "target",
            "python",
            "build",
        )
        os.makedirs(self.build_base, exist_ok=True)


def read(*paths, **kwargs):
    with io.open(
        os.path.join(
            os.path.dirname(__file__),
            *paths,
        ),
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


# noinspection DuplicatedCode
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
    if ignore_connector_requirements:
        requirements = [
            requirement
            for requirement in requirements
            if not requirement.startswith(TABSDATA_PACKAGES_PREFIX)
        ]
    print("📦 List of application requirements in setup.py:")
    for requirement in requirements:
        print(f" - 📚 {requirement}")
    return requirements


# noinspection DuplicatedCode
if platform.python_implementation() != "CPython":
    raise RuntimeError("The Tabsdata package requires CPython to function correctly.")

profile = os.getenv("profile") or os.getenv("PROFILE", "debug")
if profile in ("", "dev"):
    profile = "debug"
print(f"Using Rust profile: '{profile}'")

td_target = os.getenv("td-target", "")
print(f"Using tabsdata target: '{td_target}'")

target_release_folder = os.path.join(
    "target",
    td_target,
    profile,
)
print(f"Using tabsdata target release folder: '{target_release_folder}'")

# noinspection DuplicatedCode
base_binaries = [
    "apiserver",
    "bootloader",
    "importer",
    "supervisor",
    "tdserver",
    "transporter",
]

# noinspection DuplicatedCode
binaries = [
    binary
    for base in base_binaries
    for binary in (base, f"{base}.exe")
    if os.path.exists(
        os.path.join(
            target_release_folder,
            binary,
        )
    )
]

missing_binaries = [
    base
    for base in base_binaries
    if not any(
        os.path.exists(
            os.path.join(
                target_release_folder,
                binary,
            )
        )
        for binary in (base, f"{base}.exe")
    )
]

if missing_binaries and require_server_binaries:
    raise FileNotFoundError(
        "The following binaries are missing in "
        f"{target_release_folder}: {', '.join(missing_binaries)}"
    )

datafiles = [
    (
        get_binaries_folder(),
        [
            os.path.join(
                target_release_folder,
                binary,
            )
            for binary in binaries
        ],
    )
]
print(f"Including tabsdata binaries: {datafiles}")

# noinspection DuplicatedCode
print(f"Current path in setup is {ROOT}")

assets_folder = os.path.join(
    ROOT,
    "assets",
)
variant_assets_folder = os.path.join(
    ROOT,
    "variant",
    "assets",
)

variant_manifest_folder = os.path.join(
    ROOT,
    "variant",
    "assets",
    "manifest",
)

package_assets_folder = os.path.join(
    "client",
    "td-sdk",
    "tabsdata",
    "assets",
)

if (
    not os.path.exists(
        os.path.join(
            variant_assets_folder,
            "manifest",
            "THIRD-PARTY",
        )
    )
    and require_third_party
):
    raise FileNotFoundError(
        f"The THIRD-PARTY file is missing in {variant_assets_folder}."
    )

print(f"Copying contents of {variant_assets_folder} to {package_assets_folder}")
try:
    shutil.copytree(variant_assets_folder, package_assets_folder, dirs_exist_ok=True)
except Exception as e:
    print(
        f"🦠 Warning: Failed to copy {variant_assets_folder} to"
        f" {package_assets_folder}: {e}"
    )
    if not skip_non_existing_assets:
        print(
            "🦠 Raising error as 'TD_SKIP_NON_EXISTING_ASSETS' is set to"
            f" {skip_non_existing_assets}"
        )
        raise
    else:
        print(
            "🦠 Ignoring error as 'TD_SKIP_NON_EXISTING_ASSETS' is set to"
            f" {skip_non_existing_assets}"
        )

os.makedirs(
    os.path.join(
        "target",
        "python",
        "egg",
    ),
    exist_ok=True,
)

setup(
    name="tabsdata",
    version=read(
        os.path.join(
            "assets",
            "manifest",
            "VERSION",
        )
    ),
    description="Tabsdata is a publish-subscribe (pub/sub) server for tables.",
    long_description=read(
        os.path.join(
            "variant",
            "assets",
            "manifest",
            "README-PyPi.md",
        )
    ),
    long_description_content_type="text/markdown",
    license_files=(
        os.path.join(
            "variant",
            "assets",
            "manifest",
            "LICENSE",
        ),
    ),
    author="Tabs Data Inc.",
    python_requires=">=3.12",
    install_requires=read_requirements("requirements.txt"),
    extras_require={
        "mongodb": read_requirements("requirements-connector-mongodb.txt"),
        "salesforce": read_requirements("requirements-connector-salesforce.txt"),
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
            where=os.path.join(
                "client",
                "td-sdk",
            ),
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
            where=os.path.join(
                "client",
                "td-lib",
            ),
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
            where=os.path.join(
                "extensions",
                "python",
                "td-lib",
            ),
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
        "ta_tableframe": os.path.join(
            "client",
            "td-lib",
            "ta_tableframe",
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
        "te_tableframe": os.path.join(
            "extensions",
            "python",
            "td-lib",
            "te_tableframe",
        ),
    },
    package_data={
        "tabsdata": [
            os.path.join(
                "examples",
                "*",
            ),
            os.path.join(
                "examples",
                "input",
                "*.csv",
            ),
            os.path.join(
                "assets",
                "manifest",
                "*",
            ),
            os.path.join(
                "tabsserver",
                "*.yaml",
            ),
            os.path.join(
                "tabsserver",
                "function",
                "*.yaml",
            ),
        ],
    },
    data_files=datafiles,
    entry_points={
        "console_scripts": [
            "td = tabsdata.cli.cli:cli",
            "tdmain = tabsdata.tabsserver.main:main",
            "tdvenv = tabsdata.tabsserver.pyenv_creation:main",
            "tdupgrade = tabsdata.tabsserver.server.upgrade:main",
        ]
    },
    include_package_data=True,
)
