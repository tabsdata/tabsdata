#
#  Copyright 2025 Tabs Data Inc.
#

import io
import logging
import os
import platform
import shutil
import sys
import sysconfig
import threading
import time
import warnings
from contextlib import contextmanager
from pathlib import Path
from sysconfig import get_platform

import colorama
import psutil
import tqdm
from setuptools import find_packages, setup
from setuptools.command.bdist_egg import bdist_egg as _bdist_egg
from setuptools.command.build import build as _build
from setuptools.command.sdist import sdist as _sdist

# noinspection PyDeprecation
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel

# noinspection DuplicatedCode
colorama.init()

# noinspection DuplicatedCode
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# noinspection PyBroadException
try:
    # noinspection PyUnresolvedReferences
    from setuptools.command.build_py import _IncludePackageDataAbuse

    _IncludePackageDataAbuse._Warning._DETAILS = ""

    # noinspection PyProtectedMember
    warnings.filterwarnings("ignore", category=_IncludePackageDataAbuse._Warning)
except Exception:
    pass


# noinspection DuplicatedCode
def root_folder() -> str:
    current_folder = Path(os.getenv("PWD", psutil.Process().cwd()))
    logger.debug(f"📁 Current setup folder is: {current_folder}")
    while True:
        root_file = Path(
            os.path.join(
                current_folder,
                ".root",
            )
        )
        root_file_exists = root_file.exists() and root_file.is_file()
        if root_file_exists:
            logger.debug(f"🗂️ Root project folder for setup is: {current_folder}")
            return current_folder
        else:
            parent_folder = current_folder.parent
            if current_folder == parent_folder:
                logger.error(
                    "☢️️ "
                    "Root folder is unreachable from current setup folder! "
                    "Defaulting to '../../..'"
                )
                return os.path.join(
                    "..",
                    "..",
                    "..",
                )
            current_folder = parent_folder


# noinspection DuplicatedCode
ROOT = root_folder()
logger.debug(f"ROOT folder for setup is: {ROOT}")

TABSDATA_PACKAGES_PREFIX = "tabsdata"

REQUIRE_SERVER_BINARIES = "REQUIRE_SERVER_BINARIES"
REQUIRE_THIRD_PARTY = "REQUIRE_THIRD_PARTY"
REQUIRE_PYDOC_CSV = "REQUIRE_PYDOC_CSV"
TD_IGNORE_CONNECTOR_REQUIREMENTS = "TD_IGNORE_CONNECTOR_REQUIREMENTS"
TD_SKIP_NON_EXISTING_ASSETS = "TD_SKIP_NON_EXISTING_ASSETS"
TD_USE_MUSLLINUX = "TD_USE_MUSLLINUX"

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
require_pydoc_csv = (
    os.getenv(
        REQUIRE_PYDOC_CSV,
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
def get_python_tags():
    tags = {}

    def check_for_spec_py(base):
        spec_path = os.path.join(base, "client", "td-sdk", "tabsdata", "__spec.py")
        return spec_path if os.path.isfile(spec_path) else None

    spec_py = check_for_spec_py(ROOT)
    if not spec_py:
        root = os.path.dirname(os.path.dirname(__file__))
        if os.path.basename(root) == "local_packages":
            for entry in os.listdir(root):
                entry_path = os.path.join(root, entry)
                if os.path.isdir(entry_path):
                    candidate = check_for_spec_py(entry_path)
                    if candidate:
                        spec_py = candidate
                        break
    if not spec_py:
        raise FileNotFoundError(
            f"Could not locate '__spec.py' in any of the expected locations: {ROOT}."
        )

    with open(spec_py) as f:
        exec(f.read(), tags)

    return (
        tags["MIN_PYTHON_VERSION"],
        tags["PYTHON_IMPLEMENTATION"],
        tags["MIN_PYTHON_ABI"],
    )


python_version, python_implementation, python_abi = get_python_tags()

python_version_spec = f">={python_version}"
# noinspection PyCompatibility
python_version_tag = f"{python_implementation}{python_version.replace(".", "")}"
python_version_abi = python_abi


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

    def check_readme(self):
        # README lives in shared assets; skip setuptools validation warning.
        logger.debug("Skipping README validation for sdist build")
        return


class CustomBDistWheel(_bdist_wheel):
    def __init__(self, dist):
        super().__init__(dist)
        self.dist_dir = None
        self.root_is_pure: bool | None = None

    def initialize_options(self):
        super().initialize_options()
        self.dist_dir = os.path.join(
            "target",
            "python",
            "dist",
        )

    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False

    def get_tag(self):
        _, _, plat = super().get_tag()
        return python_version_tag, python_version_abi, get_platname()


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


# noinspection DuplicatedCode
# PEP-513: A Platform Tag for Portable Linux Built Distributions
#     https://peps.python.org/pep-0513/
#
# PEP-599: The manylinux2014 Platform Tag
#     https://peps.python.org/pep-0599/
#
# PEP-656: Platform Tag for Linux Distributions Using Musl
#     https://peps.python.org/pep-0656/
def get_platname():
    system = platform.system()
    architecture = platform.machine().lower()
    use_musllinux = (
        os.getenv(
            TD_USE_MUSLLINUX,
            "False",
        ).lower()
        in TRUE_VALUES
    )

    # Linux
    if system == "Linux":
        if architecture in ["x86_64", "amd64"]:
            if use_musllinux:
                return "musllinux_1_1_x86_64"
            else:
                return "manylinux1_x86_64"
        elif architecture in ["aarch64", "arm64"]:
            if use_musllinux:
                return "musllinux_1_1_aarch64"
            else:
                return "manylinux2014_aarch64"
        else:
            if use_musllinux:
                platname = f"musllinux_1_1_{architecture}"
                return platname.replace("-", "_").replace(".", "_")
            else:
                platname = f"manylinux1_{architecture}"
                return platname.replace("-", "_").replace(".", "_")
    # macOS
    elif system == "Darwin":
        if architecture in ["aarch64", "arm64"]:
            return "macosx_11_0_arm64"
        elif architecture == "x86_64":
            return "macosx_10_15_x86_64"
        else:
            platname = f"macosx_11_0_{architecture}"
            return platname.replace("-", "_").replace(".", "_")
    # Windows
    else:
        return get_platform().replace("-", "_").replace(".", "_")


def get_binaries_folder():
    system = platform.system()
    if system == "Windows":
        return "Scripts"
    else:
        return "bin"


# noinspection DuplicatedCode
def read_requirements(from_path, visited=None):
    if visited is None:
        visited = set()
    from_path = Path(from_path).resolve()
    if from_path in visited:
        raise ValueError(f"Circular dependency detected: {from_path}")
    visited.add(from_path)
    requirements = []
    with from_path.open(encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith(("#", '"')):
                continue
            if line.startswith("-r"):
                included_path = line.split(maxsplit=1)[1]
                if not os.path.isabs(included_path):
                    included_path = from_path.parent / included_path
                requirements.extend(read_requirements(included_path, visited))
            elif not line.startswith(("-", "git+")):
                requirements.append(line)
    if ignore_connector_requirements:
        requirements = [
            requirement
            for requirement in requirements
            if not requirement.startswith(TABSDATA_PACKAGES_PREFIX)
        ]
    logger.debug("📦 List of application requirements in setup.py:")
    for requirement in requirements:
        logger.debug(f" - 📚 {requirement}")
    return requirements


# noinspection DuplicatedCode
if platform.python_implementation() != "CPython":
    raise RuntimeError("The Tabsdata package requires CPython to function correctly.")

profile = os.getenv("profile") or os.getenv("PROFILE", "debug")
if profile in ("", "dev"):
    profile = "debug"
logger.debug(f"Using Rust profile: '{profile}'")

td_target = os.getenv("td-target", "")
logger.debug(f"Using tabsdata target: '{td_target}'")

target_release_folder = os.path.join(
    "target",
    td_target,
    profile,
)
logger.debug(f"Using tabsdata target release folder: '{target_release_folder}'")

# noinspection DuplicatedCode
base_binaries = []

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
logger.debug(f"Including tabsdata binaries: {datafiles}")

# noinspection DuplicatedCode
logger.debug(f"Current path in setup is {ROOT}")

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

package_manifest_folder = os.path.join(
    "tabsdata_snowflake",
    "assets",
    "manifest",
)

# noinspection DuplicatedCode
banner_file = os.path.join(
    variant_manifest_folder,
    BANNER,
)
logger.debug(
    f"Copying {banner_file} ({os.path.abspath(banner_file)}) to"
    f" {package_manifest_folder}"
)
try:
    shutil.copy(banner_file, package_manifest_folder)
except Exception as e:
    logger.warning(
        f"🦠 Warning: Failed to copy {banner_file} to {package_manifest_folder}: {e}"
    )
    if not skip_non_existing_assets:
        logger.error(
            "🦠 Raising error as 'TD_SKIP_NON_EXISTING_ASSETS' is set to"
            f" {skip_non_existing_assets}"
        )
        raise
    else:
        logger.debug(
            "🦠 Ignoring error as 'TD_SKIP_NON_EXISTING_ASSETS' is set to"
            f" {skip_non_existing_assets}"
        )

# noinspection DuplicatedCode
license_file = os.path.join(
    variant_manifest_folder,
    LICENSE,
)
logger.debug(f"Copying {license_file} to {package_manifest_folder}")
try:
    shutil.copy(license_file, package_manifest_folder)
except Exception as e:
    logger.warning(
        f"🦠 Warning: Failed to copy {license_file} ({os.path.abspath(license_file)})"
        f" to {package_manifest_folder}: {e}"
    )
    if not skip_non_existing_assets:
        logger.error(
            "🦠 Raising error as 'TD_SKIP_NON_EXISTING_ASSETS' is set to"
            f" {skip_non_existing_assets}"
        )
        raise
    else:
        logger.debug(
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


# noinspection DuplicatedCode
class SpinnerStream:

    def __init__(self, stream, batr):
        self.stream = stream
        self.bar = batr

    def write(self, text):
        if text.strip():
            self.bar.write(text.rstrip("\n"), file=self.stream)
        elif text:
            self.stream.write(text)

    def flush(self):
        self.stream.flush()

    def __getattr__(self, name):
        return getattr(self.stream, name)


# noinspection DuplicatedCode
@contextmanager
def global_spinner(desc: str):
    if not tqdm or not sys.stderr.isatty():
        yield
        return

    stop = threading.Event()
    spinner_symbols = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    spinner_index = 0
    spinner_bar = None

    sysconfig.PREFIX

    def worker():
        nonlocal spinner_index, spinner_bar
        with tqdm.tqdm(
            desc=desc,
            dynamic_ncols=True,
            bar_format=(
                f"{colorama.Fore.CYAN}{{desc}} {{elapsed}}{colorama.Style.RESET_ALL}"
            ),
            leave=True,
            file=sys.stderr,
        ) as bar:
            spinner_bar = bar
            while not stop.is_set():
                bar.set_description_str(f"⏳ {spinner_symbols[spinner_index]} {desc}")
                spinner_index = (spinner_index + 1) % len(spinner_symbols)
                bar.refresh()
                time.sleep(0.1)

    t = threading.Thread(target=worker, daemon=True)
    t.start()

    while spinner_bar is None:
        time.sleep(0.01)

    sys_stdout = sys.stdout
    sys_stderr = sys.stderr
    sys.stdout = SpinnerStream(sys_stdout, spinner_bar)
    sys.stderr = SpinnerStream(sys_stderr, spinner_bar)

    try:
        yield
    finally:
        sys.stdout = sys_stdout
        sys.stderr = sys_stderr
        stop.set()
        t.join()

with global_spinner("Building package 'tabsdata-snowflake' (Snowflake Connector)..."):
    setup(
        name="tabsdata_snowflake",
        version=read(
            os.path.join(
                "tabsdata_snowflake",
                "assets",
                "manifest",
                "VERSION",
            )
        ),
        description="Tabsdata plugin to access Snowflake data.",
        long_description=read(
            os.path.join(
                "tabsdata_snowflake",
                "assets",
                "manifest",
                "README-PyPi.md",
            )
        ),
        long_description_content_type="text/markdown",
        license_files=(
            os.path.join(
                "tabsdata_snowflake",
                "assets",
                "manifest",
                "LICENSE",
            ),
        ),
        author="Tabs Data Inc.",
        url="https://tabsdata.com",
        project_urls={
            "Source": "https://github.com/tabsdata/tabsdata",
        },
        python_requires=python_version_spec,
        install_requires=[],
        extras_require={"deps": read_requirements("requirements.txt")},
        cmdclass={
            "build": CustomBuild,
            "sdist": CustomSDist,
            "bdist_wheel": CustomBDistWheel,
            "bdist_egg": CustomBDistEgg,
        },
        packages=find_packages(
            where=os.getcwd(),
            exclude=[
                "tests",
                "tests*",
                "tests_tabsdata_snowflake",
                "tests_tabsdata_snowflake*",
            ],
        ),
        package_dir={
            "tabsdata_snowflake": "tabsdata_snowflake",
        },
        package_data={
            "tabsdata_snowflake": [
                os.path.join(
                    "assets",
                    "manifest",
                    "*",
                ),
            ],
        },
        data_files=datafiles,
        entry_points={},
        include_package_data=True,
    )
