#
# Copyright 2024 Tabs Data Inc.
#

import io
import json
import logging
import os
import platform
import shutil
import sys
import threading
import time
import warnings
from contextlib import contextmanager
from pathlib import Path
from sysconfig import get_platform
from uuid import uuid4

import colorama
import tqdm
from setuptools import find_packages, setup
from setuptools.command.bdist_egg import bdist_egg as _bdist_egg
from setuptools.command.build import build as _build
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.sdist import sdist as _sdist

# noinspection PyDeprecation
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel

# noinspection DuplicatedCode
colorama.init()

# noinspection DuplicatedCode
logger = logging.getLogger()
logger.setLevel(logging.ERROR)
handler = logging.StreamHandler()
handler.setLevel(logging.ERROR)
logger.addHandler(handler)

logging.getLogger("distutils").setLevel(logging.ERROR)
logging.getLogger("setuptools").setLevel(logging.ERROR)

# noinspection PyBroadException
try:
    # noinspection PyUnresolvedReferences
    from setuptools.command.build_py import _IncludePackageDataAbuse

    # noinspection PyProtectedMember
    _IncludePackageDataAbuse._Warning._DETAILS = ""

    # noinspection PyProtectedMember
    warnings.filterwarnings("ignore", category=_IncludePackageDataAbuse._Warning)
except Exception:
    pass

warnings.filterwarnings(
    "ignore", message=".*is absent from the `packages` configuration.*"
)


# noinspection DuplicatedCode
def root_folder() -> str:
    # current_folder = Path(os.getenv("PWD", psutil.Process().cwd()))
    current_folder = Path(__file__).parent.absolute()
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
                    "Defaulting to '.'"
                )
                return os.path.join(
                    ".",
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

    def run(self):
        super().run()
        # tabsdata.expansions.tableframe -> tabsdata.libs
        tabsdata_libs_path = os.path.join(
            "expansions",
            "polars",
            "modules",
            "ty-tableframe",
            "python",
            "tabsdata.libs",
        )
        if os.path.exists(tabsdata_libs_path):
            tabsdata_libs_folder = os.path.join(self.build_lib, "tabsdata.libs")
            logger.info(f"Copying {tabsdata_libs_path} to {tabsdata_libs_folder}")
            if os.path.exists(tabsdata_libs_folder):
                shutil.rmtree(tabsdata_libs_folder)
            shutil.copytree(tabsdata_libs_path, tabsdata_libs_folder)


PACKAGE_RESOURCES = {
    Path(ROOT) / "variant" / "resources": Path(".") / "tabsdata" / "resources",
}


class CustomBuildPy(_build_py):

    def run(self):
        package_root = Path(self.build_lib)
        for source, target in PACKAGE_RESOURCES.items():
            if not source.exists():
                logger.warning(f"External resource {source} does not exist. Skipping.")
                continue
            destination = package_root / target
            if source.is_dir():
                shutil.copytree(source, destination, dirs_exist_ok=True)
            elif source.is_file():
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, destination)
            else:
                logger.warning(
                    f"External resource {source} has unsupported type. Skipping."
                )
        super().run()


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


# noinspection DuplicatedCode
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
def read_requirements(from_path, root=None, token=None, visited=None):  # noqa: C901
    if token is None:
        token = str(uuid4())
    if visited is None:
        visited = set()
    from_path = Path(from_path).resolve()

    logger.debug(f" - 🥁 {token} · Visiting requirements path: {root} - {from_path}")

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
                requirements.extend(
                    read_requirements(included_path, from_path, token, visited)
                )
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


# noinspection PyBroadException
def load_console_scripts() -> list[str]:
    setup_json = Path("setup.json")
    if not setup_json.exists():
        return []

    try:
        with setup_json.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []

    scripts = []
    try:
        for entry in data.get("entry_points", {}).get("console_scripts", []):
            name = entry.get("name")
            function = entry.get("function")
            if name and function:
                scripts.append(f"{name} = {function}")
    except Exception:
        pass

    return scripts


TABSDATA_VERSION = read(os.path.join("assets", "manifest", "VERSION"))

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

# Caution!: This list is replicated in project python file
# 'client/td-sdk/tabsdata/_utils/bundle_utils.py' to ensure that when testing
# with pytest, the binaries are distributed and available from tabsdata as a
# local package.
# Please, make sure you update this list in both places.
base_binaries = [
    "apiserver",
    "bootloader",
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

# noinspection DuplicatedCode
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
if (
    not os.path.exists(
        os.path.join(
            variant_assets_folder,
            "manifest",
            "PYDOC.csv",
        )
    )
    and require_pydoc_csv
):
    raise FileNotFoundError(
        f"The PYDOC.csv file is missing in {variant_assets_folder}."
    )

logger.debug(f"Copying contents of {variant_assets_folder} to {package_assets_folder}")
try:
    shutil.copytree(
        variant_assets_folder,
        package_assets_folder,
        dirs_exist_ok=True,
        symlinks=False,
    )
except Exception as e:
    logger.warning(
        f"🦠 Warning: Failed to copy {variant_assets_folder} to"
        f" {package_assets_folder}: {e}"
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

yaml_files = []
yaml_root_folder = Path(
    os.path.join(
        "client",
        "td-sdk",
        "tabsdata",
    )
)
for path in yaml_root_folder.rglob("*.yaml"):
    relative_path = path.relative_to(yaml_root_folder)
    yaml_files.append(str(relative_path))

os.makedirs(
    os.path.join(
        "target",
        "python",
        "egg",
    ),
    exist_ok=True,
)

console_scripts: list[str] = [
    # Tabsdata CLI
    "td = tabsdata._cli.cli:cli",
    # Tabsdata client tools
    "x_oracle_check = tabsdata._tabsserver.tools.x_oracle_check:cli",
    # Supervisor init workers
    "tdcfgrsv = tabsdata._tabsserver.tools.config_resolver:main",
    "tdsrvinf = tabsdata._tabsserver.tools.server_info:main",
    "tdmntext = tabsdata._tabsserver.tools.mount_extractor:main",
    # Supervisor regular workers
    "janitor = tabsdata._tabsserver.tools.janitor:main",
    # Supervisor tools
    "tdinvoker = tabsdata._tabsserver.invoker:main",
    "tdupgrader = tabsdata._tabsserver.server.upgrader.upgrader:main",
    "tdvenv = tabsdata._tabsserver.pyenv_creation:main",
]
console_scripts.extend(load_console_scripts())


def build_extras_require(root: str) -> dict[str, list[str]]:
    connectors_folders = [
        os.path.join(root, "connectors", "python"),
        # os.path.join(root, "connectors.ee", "python"),
    ]

    extras: dict[str, list[str]] = {
        "test": read_requirements(os.path.join(root, "requirements-test.txt")),
    }

    all_connectors_requirements: list[str] = []

    requirement_template = "tabsdata_{connector}[deps]=={version}"

    for folder in connectors_folders:
        if not os.path.isdir(folder):
            continue

        for entry in os.scandir(folder):
            if entry.is_dir():
                connector_name = entry.name.split("_", 1)
                if len(connector_name) != 2 or connector_name[0] != "tabsdata":
                    logger.debug(f"⛔️ Invalid connector folder name: {entry.name}")
                else:
                    connector = connector_name[1]
                    logger.info(
                        "📦️ Adding requirements of connector requirements of "
                        f"{connector}: {entry.name}"
                    )
                    connector_requirement = requirement_template.format(
                        connector=connector, version=TABSDATA_VERSION
                    )
                    extras[connector] = [connector_requirement]
                    all_connectors_requirements.append(connector_requirement)

    extras["all"] = sorted(all_connectors_requirements)
    return extras


TABSDATA_LICENSE = read(os.path.join(variant_manifest_folder, "LICENSE.setup"))


# noinspection DuplicatedCode
class TqdmStream:
    def __init__(self, file):
        self.file = file

    def write(self, text):
        if text and text.strip():
            tqdm.tqdm.write(text.rstrip("\n"), file=self.file)

    def flush(self):
        self.file.flush()

    def __getattr__(self, name):
        return getattr(self.file, name)


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

    log_handlers = []
    for w_handler in logging.root.handlers[:]:
        if isinstance(w_handler, logging.StreamHandler):
            log_handlers.append((w_handler, w_handler.stream))
            w_handler.setStream(TqdmStream(w_handler.stream))

    try:
        sys.stdout = TqdmStream(sys_stdout)
        sys.stderr = TqdmStream(sys_stderr)
        yield
    finally:
        sys.stdout = sys_stdout
        sys.stderr = sys_stderr

        for w_handler, original_stream in log_handlers:
            w_handler.setStream(original_stream)

        stop.set()
        t.join()


with global_spinner("Building package 'tabsdata' (TabsData)..."):
    setup(
        name="tabsdata",
        version=TABSDATA_VERSION,
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
        # On Windows, setuptools interprets license_files paths as glob patterns,
        # and backslashes (\) are treated as escape characters, which may cause
        # unexpected parsing errors or “invalid character” complaints. Because of
        # this, back-slashes are replaced with forward-slashes.
        license=TABSDATA_LICENSE,
        license_files=(
            os.path.join(
                "variant",
                "assets",
                "manifest",
                "LICENSE",
            ).replace(os.sep, "/"),
        ),
        author="Tabs Data Inc.",
        url="https://tabsdata.com",
        project_urls={
            "Source": "https://github.com/tabsdata/tabsdata",
        },
        python_requires=python_version_spec,
        install_requires=read_requirements("requirements.txt"),
        extras_require=build_extras_require(os.getcwd()),
        cmdclass={
            "build": CustomBuild,
            "bdist_egg": CustomBDistEgg,
            "bdist_wheel": CustomBDistWheel,
            "sdist": CustomSDist,
            "build_py": CustomBuildPy,
        },
        packages=[
            # tabsdata
            *find_packages(
                where=os.path.join(
                    "client",
                    "td-sdk",
                ),
                exclude=[
                    "tests",
                    "tests*",
                    "tabsdata.assets",
                    "tabsdata.assets*",
                    "tabsdata.resources",
                    "tabsdata.resources*",
                ],
            ),
            # tabsdata.extensions._features.api
            *find_packages(
                where=os.path.join(
                    "client",
                    "td-lib",
                    "ta_features",
                ),
                include=["tabsdata.extensions._features.api*"],
            ),
            # tabsdata.extensions._tableframe.api
            *find_packages(
                where=os.path.join(
                    "client",
                    "td-lib",
                    "ta_tableframe",
                ),
                include=["tabsdata.extensions._tableframe.api*"],
            ),
            # tabsdata.extensions._examples
            *find_packages(
                where=os.path.join(
                    "extensions",
                    "python",
                    "td-lib",
                    "te_examples",
                ),
                include=["tabsdata.extensions._examples*"],
            ),
            # tabsdata.extensions._tableframe
            *find_packages(
                where=os.path.join(
                    "extensions",
                    "python",
                    "td-lib",
                    "te_tableframe",
                ),
                include=["tabsdata.extensions._tableframe*"],
            ),
            # tabsdata.expansions.tableframe
            *find_packages(
                where=os.path.join(
                    "expansions",
                    "polars",
                    "modules",
                    "ty-tableframe",
                    "python",
                ),
                include=["tabsdata.expansions.tableframe*"],
            ),
        ],
        package_dir={
            "tabsdata": os.path.join(
                "client",
                "td-sdk",
                "tabsdata",
            ),
            "tabsdata.extensions._features.api": os.path.join(
                "client",
                "td-lib",
                "ta_features",
                "tabsdata",
                "extensions",
                "_features",
                "api",
            ),
            "tabsdata.extensions._tableframe.api": os.path.join(
                "client",
                "td-lib",
                "ta_tableframe",
                "tabsdata",
                "extensions",
                "_tableframe",
                "api",
            ),
            "tabsdata.extensions._examples": os.path.join(
                "extensions",
                "python",
                "td-lib",
                "te_examples",
                "tabsdata",
                "extensions",
                "_examples",
            ),
            "tabsdata.extensions._tableframe": os.path.join(
                "extensions",
                "python",
                "td-lib",
                "te_tableframe",
                "tabsdata",
                "extensions",
                "_tableframe",
            ),
            "tabsdata.expansions.tableframe": os.path.join(
                "expansions",
                "polars",
                "modules",
                "ty-tableframe",
                "python",
                "tabsdata",
                "expansions",
                "tableframe",
            ),
        },
        package_data={
            "tabsdata": [
                os.path.join(
                    "assets",
                    "manifest",
                    "*",
                ),
                os.path.join(
                    "resources",
                    "**",
                    "*",
                ),
                *yaml_files,
            ],
            "tabsdata.extensions._examples": [
                os.path.join(
                    "guides",
                    "book",
                    "**",
                    "*",
                ),
                os.path.join(
                    "cases",
                    "**",
                    "*",
                ),
            ],
            "tabsdata.expansions.tableframe": [
                os.path.join(
                    "_*",
                ),
            ],
        },
        data_files=datafiles,
        entry_points={"console_scripts": console_scripts},
        include_package_data=True,
    )
