#
# Copyright 2024 Tabs Data Inc.
#

import argparse
import collections
import hashlib
import importlib.metadata
import importlib.util
import json
import logging
import os
import os.path
import pathlib
import pkgutil
import platform
import re
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import time

# noinspection PyCompatibility
import tomllib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Tuple, TypeAlias

import importlib_metadata
import yaml
from filelock import FileLock, Timeout
from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

# noinspection PyProtectedMember
from yaml import MappingNode
from yaml.constructor import ConstructorError

from tabsdata.__spec import MIN_PYTHON_VERSION
from tabsdata._tabsserver.function.global_utils import CURRENT_PLATFORM, setup_logging
from tabsdata._tabsserver.server.instance import (
    DEFAULT_ENVIRONMENT_FOLDER,
    DEFAULT_INSTANCE,
    DEFAULT_INSTANCES_FOLDER,
    DEFAULT_TABSDATA_FOLDER,
    LOCK_FOLDER,
    LOG_FOLDER,
    WORK_FOLDER,
    WORKSPACE_FOLDER,
)
from tabsdata._tabsserver.utils import ABSOLUTE_LOCATION, TimeBlock

# noinspection PyProtectedMember
from tabsdata._utils.bundle_utils import (
    LOCAL_PACKAGES_FOLDER,
    PYTHON_DEVELOPMENT_PACKAGES_KEY,
    PYTHON_IGNORE_UNAVAILABLE_PUBLIC_PACKAGES_KEY,
    PYTHON_INSTALL_DEPENDENCIES_KEY,
    PYTHON_PUBLIC_PACKAGES_KEY,
    PYTHON_VERSION_KEY,
)

# noinspection PyProtectedMember
from tabsdata._utils.constants import (
    FALSE_FALSE,
    PYTHON_EDITABLE_PACKAGE_PREFIX,
    PYTHON_EDITABLE_PACKAGE_SUFFIX,
    TABSDATA_AGENT_PACKAGE_NAME,
    TABSDATA_CONNECTORS,
    TABSDATA_CONNECTORS_NAMES,
    TABSDATA_MODULE_NAME,
    TABSDATA_PACKAGES,
    TABSDATA_TOOLKIT_PACKAGE_NAME,
    TD_TABSDATA_AGENT_DEV_PKG,
    TD_TABSDATA_DEV_PKG,
    TD_TABSDATA_TOOLKIT_DEV_PKG,
    TRUE_VALUES,
)

# noinspection PyProtectedMember
from tabsdata._utils.id import encode_id

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._constants import PYTEST_CONTEXT_ACTIVE
from tabsdata._utils.temps import tabsdata_temp_folder
from tabsdatak.common._resources.reader import td_resource

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
time_block = TimeBlock()

STDLIB_RETRY_INTERVAL_SECONDS = 5
STDLIB_RETRY_TIMEOUT_SECONDS = 60

REMOTE_DEBUG = "TD_REMOTE_DEBUG"

FILE_PROTOCOL = "file://"
WINDOWS_OS_NAME = "nt"
WINDOWS_URL_PREFIX = "/"
BACK_SLASH = "\\"

TD_BUILD_TIMESTAMP = "TD_BUILD_TIMESTAMP"

PYTHONWARNINGS = "PYTHONWARNINGS"

ENV_td_target = "td-target"
ENV_TD_TARGET = "TD_TARGET"
ENV_profile = "profile"
ENV_PROFILE = "PROFILE"

REQUIRE_SERVER_BINARIES = "REQUIRE_SERVER_BINARIES"
REQUIRE_THIRD_PARTY = "REQUIRE_THIRD_PARTY"
TD_IGNORE_CONNECTOR_REQUIREMENTS = "TD_IGNORE_CONNECTOR_REQUIREMENTS"
TD_SKIP_NON_EXISTING_ASSETS = "TD_SKIP_NON_EXISTING_ASSETS"
TD_SETUP_GENERATE_LOCKS = "TD_SETUP_GENERATE_LOCKS"

HostPackageSource: TypeAlias = Literal[
    "Development",
    "Local",
]

# Base environments are added the hash of this string to ensure that no function
# environment accidentally resolves to a base environment.
# !!! Do not change this string... never !!!
# noinspection SpellCheckingInspection
BASE_ENV_SALT = "RekvSLlYqSt0VXJghaYhbQ5UyaofKk4h"

BASE_ENVIRONMENT_PREFIX = "."
DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER = os.path.join(
    DEFAULT_TABSDATA_FOLDER, "available_environments"
)
LOCK_EXTENSION = ".lock"

WHEEL_EXTENSION = ".whl"
TARGET_FOLDER = "target"

PYTHON_BASE_VERSION = MIN_PYTHON_VERSION

UV_EXECUTABLE = "uv"

ENVIRONMENT_LOCK_TIMEOUT = 5  # 5 seconds
MAXIMUM_LOCK_TIME = 60 * 30  # 30 minutes
PYTHON_VERSION_LOCK_TIMEOUT = 10  # 10 seconds
TESTIMONY_LOCK_TIMEOUT = 2 * 60  # 2 minutes

TD_INHERIT_TABSDATA_PACKAGES = "TD_INHERIT_TABSDATA_PACKAGES"
TD_USE_CURRENT_VENV = "TD_USE_CURRENT_VENV"


@dataclass
class TestimonyData:
    real_name: str
    logical_name: str
    last_used: str
    last_used_timestamp: float
    times_used: int


DEBUG_PACKAGES = [
    "gtts",
    "pydevd",
    "pydevd-pycharm",
    "pygame",
]

BUILD_MANIFEST_LOCATION = "assets/manifest/BUILD"
BUILD_TIMESTAMP_KEY = "X-Build-Timestamp-UTC"

PackageProvider: TypeAlias = Literal[
    "Archive (Project)",
    "Archive (Folder)",
    "Archive (Wheel)",
    "Folder (Editable)",
    "Folder (Frozen)",
    "Package",
]


def extract_package_name(requirement):
    match = re.match(r"^\s*([A-Za-z0-9_.-]+)", requirement)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Invalid requirement format: {requirement}")


def remove_path(folder: str) -> None:
    original = Path(folder)
    if not original.exists():
        logger.warning(f"Path not found; nothing to delete: '{original}'")
        return
    if not original.is_dir():
        raise NotADirectoryError(f"Expected a directory, but got: '{original}'")
    parent = original.parent
    _, code = encode_id()
    quarantine = parent / f"_{code}"

    try:
        os.replace(original, quarantine)
    except FileNotFoundError:
        logger.warning(
            f"Original path vanished before renaming; nothing to rename: '{original}'"
        )
        return
    except Exception as e:
        logger.exception(f"Could not rename '{original}' to '{quarantine}': {e}")
        raise

    try:
        shutil.rmtree(quarantine)
        logger.info(f"Deleted quarantine path: '{quarantine}'")
    except FileNotFoundError:
        logger.warning(
            "Quarantine path vanished before deletion; nothing to delete: "
            f"'{quarantine}'"
        )
        return
    except Exception as e:
        logger.warning(f"Failed to delete quarantine path '{quarantine}': {e}")
        return


def delete_virtual_environment(
    logical_environment_name: str, real_environment_name: str, log_error_on_fail=True
) -> bool:
    """Delete a Python virtual environment"""

    logger.info(
        f"Deleting Python virtual environment {logical_environment_name} with real name"
        f" {real_environment_name}"
    )

    # Delete the Python virtual environment
    path_to_environment = os.path.join(
        DEFAULT_ENVIRONMENT_FOLDER, real_environment_name
    )
    try:
        delete_testimony(logical_environment_name)
        remove_path(path_to_environment)
    except FileNotFoundError as e:
        if log_error_on_fail:
            logger.error(
                "Failed to delete Python virtual environment"
                f" {logical_environment_name} with real name"
                f" {real_environment_name}: {e}"
            )
        else:
            logger.info(
                "No remnants of Python virtual environment"
                f" {logical_environment_name} with real name {real_environment_name} "
                "found"
            )
        return False
    except Exception as e:
        if log_error_on_fail:
            logger.error(
                "Fatal error deleting the Python virtual environment"
                f" {logical_environment_name} with real name"
                f" {real_environment_name}: {e}"
            )
        else:
            logger.info(
                "The environment could not be totally deleted due to an internal error:"
                f" {logical_environment_name} with real name {real_environment_name} - "
                "{e}"
            )
        return False
    else:
        logger.info(
            "Deleted Python virtual environment"
            f" {logical_environment_name} with real name {real_environment_name} "
            "successfully"
        )
        return True


def list_folders(directory: str) -> list:
    try:
        return [
            name
            for name in os.listdir(directory)
            if os.path.isdir(os.path.join(directory, name))
        ]
    except FileNotFoundError:
        logger.warning(f"Directory {directory} not found.")
        return []


def get_existing_virtual_environments():
    existing_virtual_environments = sorted(list_folders(DEFAULT_ENVIRONMENT_FOLDER))
    logger.debug("Existing virtual environments:")
    for environment in existing_virtual_environments:
        logger.debug(f"· {environment}")
    return existing_virtual_environments


def verify_single_package_installable_for_environment(
    package: str,
    real_environment_name: str,
) -> bool:
    """Test if a single package can be installed."""
    logger.debug(f"Verifying if package {package} is installable with pip")
    pip_install_dry_run = add_python_target_to_command(
        [
            UV_EXECUTABLE,
            "pip",
            "install",
            "--link-mode",
            "hardlink",
            package,
            "--dry-run",
            "--no-deps",
        ],
        real_environment_name,
    )
    logger.info(f"Running command: '{' '.join(pip_install_dry_run)}'")
    with time_block:
        result = subprocess.run(
            pip_install_dry_run,
            capture_output=True,
            text=True,
        )
    if result.returncode == 0:
        logger.debug(f"Package {package} is installable with pip")
        return True
    else:
        logger.warning(f"Package {package} is not installable with pip")
        logger.warning(
            "Reason:"
            f"\n'{str(result.returncode)}'"
            f"\n'{str(result.stdout)}'"
            f"\n'{str(result.stderr)}'"
        )
        return False


def verify_batch_of_packages_installable_for_environment(
    packages: list[str],
    real_environment_name: str,
) -> bool:
    """Test if a batch of packages can all be installed."""
    logger.debug(f"Batch verifying {len(packages)} packages")
    if not packages:
        return True
    pip_install_dry_run = add_python_target_to_command(
        [
            UV_EXECUTABLE,
            "pip",
            "install",
            "--link-mode",
            "hardlink",
            "--dry-run",
            "--no-deps",
        ]
        + packages,
        real_environment_name,
    )
    logger.info(f"Running batch command: '{' '.join(pip_install_dry_run)}'")
    with time_block:
        result = subprocess.run(
            pip_install_dry_run,
            capture_output=True,
            text=True,
        )
    if result.returncode == 0:
        logger.debug(
            f"All {len(packages)} packages in batch are installable with uv pip:"
            f" ({packages}) "
        )
        return True
    else:
        logger.debug(
            f"Some of the {len(packages)} packages are not installable with uv pip:"
            f" ({packages}) "
        )
        logger.debug(
            "Reason:"
            f"\n'{str(result.returncode)}'"
            f"\n'{str(result.stdout)}'"
            f"\n'{str(result.stderr)}'"
        )
        return False


def found_requirements(
    requirements: list[str],
    development_packages: list[tuple[str, PackageProvider]],
    real_environment_name: str,
) -> list[str]:
    logger.info(f"Checking if the packages {requirements} are available on PyPi")

    inherit_tabsdata_packages = (
        os.getenv(
            TD_INHERIT_TABSDATA_PACKAGES,
            "False",
        ).lower()
        in TRUE_VALUES
    )

    # Build a set of canonical names of tabsdata packages that will be
    # installed from local development sources. These must be excluded from
    # PyPI resolution to avoid dependency conflicts between the lock file
    # and the published package metadata on PyPI.
    development_package_names: set[str] = set()
    for dev_location, _dev_provider in development_packages:
        dev_pkg_name = extract_tabsdata_package_name(dev_location)
        if dev_pkg_name:
            development_package_names.add(canonicalize_name(dev_pkg_name))

    # Split requirements into tabsdata, tabsdata skipped & and non-tabsdata packages.
    tabsdata_packages = []
    tabsdata_skipped_packages = []
    non_tabsdata_packages = []

    for package in requirements:
        module = extract_package_name(package)
        if module.startswith(TABSDATA_MODULE_NAME):
            if canonicalize_name(module) in development_package_names:
                tabsdata_skipped_packages.append(package)
            else:
                tabsdata_packages.append(package)
        else:
            non_tabsdata_packages.append(package)

    if tabsdata_skipped_packages:
        logger.info(
            f"Excluding {len(tabsdata_skipped_packages)} "
            f"tabsdata packages from PyPI resolution "
            f"(they will be installed from local development sources): "
            f"{tabsdata_skipped_packages}"
        )

    logger.info(
        "Split requirements into "
        f"{len(non_tabsdata_packages)} non-tabsdata and "
        f"{len(tabsdata_packages)} tabsdata packages"
    )

    available_packages = []

    # First we make an optimistic validation of all packages, that is fast and
    # can be assumed to be correct most times. If it fails, we fall back to
    # individual checks for proper reporting; it is irrelevant speed here, as
    # environment will not be created and function will not be run, if any.

    # Process non-tabsdata packages
    if non_tabsdata_packages:
        found_requirements_non_tabsdata(
            non_tabsdata_packages,
            available_packages,
            real_environment_name,
        )

    # Process tabsdata packages
    if tabsdata_packages:
        found_requirements_tabsdata(
            tabsdata_packages,
            available_packages,
            real_environment_name,
            inherit_tabsdata_packages,
            development_packages,
        )

    logger.info(f"Available packages: {available_packages}")
    expected_packages = set(requirements) - set(tabsdata_skipped_packages)
    if set(available_packages) != expected_packages:
        missing_packages = expected_packages - set(available_packages)
        logger.warning(f"Missing packages: {missing_packages}")
    return available_packages


def found_requirements_non_tabsdata(
    non_tabsdata_packages: list[str],
    available_packages: list[str],
    real_environment_name: str,
):
    if non_tabsdata_packages:
        logger.info(
            f"Batch checking {len(non_tabsdata_packages)} non-tabsdata packages"
        )
        if verify_batch_of_packages_installable_for_environment(
            non_tabsdata_packages,
            real_environment_name,
        ):
            logger.info("All non-tabsdata packages are available")
            available_packages.extend(non_tabsdata_packages)
        else:
            logger.info(
                "Batch check failed for non-tabsdata packages, checking individually"
            )
            for package in non_tabsdata_packages:
                if verify_single_package_installable_for_environment(
                    package,
                    real_environment_name,
                ):
                    logger.info(f"Package {package} marked as: available")
                    available_packages.append(package)
                else:
                    logger.info(f"Package {package} marked as: unavailable")


def found_requirements_tabsdata(
    tabsdata_packages: list[str],
    available_packages: list[str],
    real_environment_name: str,
    inherit_tabsdata_packages: bool,
    development_packages: list[tuple[str, PackageProvider]],
):
    if tabsdata_packages:
        logger.info(f"Batch checking {len(tabsdata_packages)} tabsdata packages")
        if verify_batch_of_packages_installable_for_environment(
            tabsdata_packages,
            real_environment_name,
        ):
            logger.info("All tabsdata packages are available")
            available_packages.extend(tabsdata_packages)
        else:
            logger.info(
                "Batch check failed for tabsdata packages, checking individually"
            )
            for package in tabsdata_packages:
                if verify_single_package_installable_for_environment(
                    package,
                    real_environment_name,
                ):
                    logger.info(f"Package {package} marked as: available")
                    available_packages.append(package)
                else:
                    module = extract_package_name(package)
                    if module in TABSDATA_PACKAGES:
                        if inherit_tabsdata_packages:
                            td_provider, td_location = get_tabsdata_package_metadata(
                                module,
                                None,
                            )
                            logger.info(
                                f"Package {package} determined as: "
                                f"provider: {td_provider} - "
                                f"location: {td_location}"
                            )
                            if td_provider in (
                                "Archive (Project)",
                                "Archive (Folder)",
                                "Archive (Wheel)",
                                "Folder (Editable)",
                                "Folder (Frozen)",
                            ):
                                # This feature is only meant to be used for development.
                                # Environment hash has already been computed. Therefore,
                                # any changes in the inherited packages will not be
                                # reflected in a new environment hash.
                                development_packages.append((
                                    str(td_location),
                                    td_provider,
                                ))
                                logger.info(
                                    f"Package {package} marked as: td-available"
                                )
                                logger.info(
                                    f"Adding package {package} to the development"
                                    " packages specification"
                                )
                            else:
                                logger.info(
                                    f"Package {package} marked as: td-non-available"
                                )
                        else:
                            logger.info(f"Package {package} marked as: td-unavailable")
                    else:
                        logger.info(f"Package {package} marked as: unavailable")
        development_packages[:]: list[tuple[str, PackageProvider]] = list(
            dict.fromkeys(development_packages)
        )


def get_build_timestamp() -> str:
    td_build_timestamp = os.getenv(TD_BUILD_TIMESTAMP)
    if td_build_timestamp:
        return td_build_timestamp
    metadata: dict[str, Any] = {}
    try:
        build = td_resource(BUILD_MANIFEST_LOCATION)
        try:
            with open(build, "r", encoding="utf-8") as f:
                metadata = yaml.safe_load(f) or {}
        except Exception as exception:
            logger.warning(f"Warning: Could not parse BUILD manifest: {exception}")
    except Exception as exception:
        logger.warning(f"Error: Could not locate BUILD manifest file: {exception}.")
    return str(
        metadata.get(
            BUILD_TIMESTAMP_KEY,
            datetime.now(tz=timezone.utc),
        )
    )


def get_dict_hash(data):
    # Add build timestamp to the data to ensure different hashes for different builds
    # (specifically for local development environments)
    data[BUILD_TIMESTAMP_KEY] = get_build_timestamp()
    # Convert the dictionary to a JSON string with sorted keys
    json_str = json.dumps(data, sort_keys=True)
    # Encode the JSON string to bytes
    json_bytes = json_str.encode("utf-8")
    # Compute the SHA-256 hash
    hash_obj = hashlib.sha256(json_bytes)
    # Return the hexadecimal representation of the hash
    return hash_obj.hexdigest()


def hash_string(string: str, length=None) -> str:
    """Return the SHA-256 hash of a string"""
    hash_obj = hashlib.sha256(string.encode("utf-8"))
    return hash_obj.hexdigest() if not length else hash_obj.hexdigest()[:length]


def include_in_hash(path: Path) -> bool:
    """Use only certain files for the hash calculation. Currently, only Python files
    and the requirements.txt file are included."""
    return path.name.endswith(".py") or path.name == "requirements.txt"


def get_dir_hash(directory):
    hash_function = hashlib.sha256()

    # Traverse the directory recursively and find all Python files
    for path in sorted(Path(directory).rglob("*"), key=lambda p: str(p).lower()):
        # Skip files under folders 'target', to get consistent and deterministic hashes.
        if "target" in path.parts:
            continue
        if path.is_file() and include_in_hash(path):
            with open(path, "rb") as file:
                while chunk := file.read(4096):
                    hash_function.update(chunk)

    return hash_function.hexdigest()


def read_yaml_file(yaml_file: str) -> dict:
    # Add a custom constructor for YAML strings so that Python version is not loaded
    # as a float
    class Loader(yaml.SafeLoader):
        def construct_mapping(self, node, deep=False):
            # noinspection PyUnreachableCode
            if not isinstance(node, MappingNode):
                raise ConstructorError(
                    None,
                    None,
                    "expected a mapping node, but found %s" % node,
                )
            mapping = {}
            for key_node, value_node in node.value:
                key = self.construct_object(key_node, deep=deep)
                if not isinstance(key, collections.abc.Hashable):
                    raise ConstructorError(
                        "while constructing a mapping",
                        node.start_mark,
                        "found unhashable key",
                        key_node.start_mark,
                    )
                # CUSTOM VERSION KEY HANDLING:
                if key == PYTHON_VERSION_KEY:
                    value = value_node.value
                else:
                    value = self.construct_object(value_node, deep=deep)
                mapping[key] = value
            return mapping

    try:
        logger.info(f"Reading yaml file {yaml_file}")
        with open(yaml_file, "r") as file:
            data = yaml.load(file, Loader=Loader)
    except FileNotFoundError as e:
        logger.error(f"File {yaml_file} not found: {e}")
        raise e
    logger.info(f"Data read from yaml file: {data}")
    return data


def add_hex_numbers(hex1, hex2):
    # Convert hexadecimal strings to integers
    int1 = int(hex1, 16)
    int2 = int(hex2, 16)

    # Add the integers
    result_int = int1 + int2

    # Convert the result back to a hexadecimal string
    result_hex = hex(result_int)

    return result_hex


def get_current_tabsdata_version():
    available_modules = [module.name for module in pkgutil.iter_modules()]
    mapping = importlib_metadata.packages_distributions()
    real_modules = []
    for module in available_modules:
        if module in mapping:
            real_modules.extend(mapping[module])
        elif module.startswith(PYTHON_EDITABLE_PACKAGE_PREFIX) and module.endswith(
            PYTHON_EDITABLE_PACKAGE_SUFFIX
        ):
            real_modules.append(module)
    for module in real_modules:
        if module == TABSDATA_MODULE_NAME or (
            module.startswith(PYTHON_EDITABLE_PACKAGE_PREFIX + TABSDATA_MODULE_NAME)
            and module.endswith(PYTHON_EDITABLE_PACKAGE_SUFFIX)
            and not any(
                module.startswith(PYTHON_EDITABLE_PACKAGE_PREFIX + connector)
                for connector in TABSDATA_CONNECTORS_NAMES
            )
        ):
            try:
                distribution_name = (
                    TABSDATA_MODULE_NAME
                    if module.startswith(PYTHON_EDITABLE_PACKAGE_PREFIX)
                    and module.endswith(PYTHON_EDITABLE_PACKAGE_SUFFIX)
                    else module
                )
                version = importlib_metadata.version(distribution_name)
                return version
            except Exception as e:
                logger.error(f"Error getting version of {module}: {e}")
                logger.error(
                    "This should never happen as the tabsdata code is "
                    "currently being run, please reach out to the "
                    "development team for assistance."
                )
                raise e
    logger.error("Could not find the tabsdata module in the available modules.")
    logger.error(
        "This should never happen, as the tabsdata code is currently being run; "
        "please reach out to the development team for assistance."
    )
    raise ValueError("Could not find the tabsdata module in the available modules.")


def inject_tabsdata_version(required_modules: list[str]) -> list[str]:  # noqa: C901
    """Inject tabsdata and dependencies versions into the list of required modules"""
    try:
        tabsdata_version = get_current_tabsdata_version()
    except ValueError:
        # Package tabsdata can be injected as a local package when running
        # pytest tests; in this case, current version is not available as
        # usual. We will use the version in the required modules' specification.
        if os.environ.get(PYTEST_CONTEXT_ACTIVE) is not None:
            tabsdata_version = None
        else:
            raise

    has_tabsdata = any(
        TABSDATA_MODULE_NAME == extract_package_name(module)
        for module in required_modules
    )

    if not has_tabsdata:
        logger.info(
            "Package tabsdata is not a required module. Injecting its current version."
        )

    tabsdata_modules = [
        module
        for module in required_modules
        if extract_package_name(module).startswith(TABSDATA_MODULE_NAME)
    ]

    legacy_tabsdata_modules = [
        module
        for module in tabsdata_modules
        if canonicalize_name(extract_package_name(module)) not in TABSDATA_PACKAGES
    ]

    for legacy_module in legacy_tabsdata_modules:
        logger.warning(
            f"Removing module '{legacy_module}' as it is not a current tabsdata package"
        )

    original_tabsdata_packages = {
        canonicalize_name(extract_package_name(module))
        for module in tabsdata_modules
        if canonicalize_name(extract_package_name(module)) in TABSDATA_PACKAGES
    }

    required_modules = [
        module
        for module in required_modules
        if not extract_package_name(module).startswith(TABSDATA_MODULE_NAME)
    ]

    (tabsdata_provider, _, _) = retrieve_tabsdata_package()

    def load_requirements_lock(
        lock_path: str,
    ) -> dict[str, tuple[Requirement, str]]:
        lock_map: dict[str, tuple[Requirement, str]] = {}
        try:
            requirements_lock = td_resource(lock_path)
            with open(requirements_lock, "r", encoding="utf-8") as f:
                for line in f:
                    raw_line = line.strip()
                    if not raw_line or raw_line.startswith("#"):
                        continue
                    try:
                        requirement = Requirement(raw_line)
                        requirement_canonical = canonicalize_name(requirement.name)
                        lock_map[requirement_canonical] = (
                            requirement,
                            raw_line,
                        )
                    except InvalidRequirement:
                        logger.warning(
                            f"Unexpected requirement lock spec: "
                            f"{lock_path} - {raw_line}"
                        )
        except Exception as error:
            logger.warning(
                f"Requirements lock file does not exist or cannot be read: {error}"
            )
        return lock_map

    requirements_lock_map = load_requirements_lock(
        "resources/lock/tabsdata-requirements.txt"
    )

    if tabsdata_provider in (
        "Archive (Project)",
        "Archive (Folder)",
        "Archive (Wheel)",
        "Folder (Editable)",
        "Folder (Frozen)",
    ):
        requirements_dev_lock_map = load_requirements_lock(
            "resources/lock/tabsdata-requirements-dev.txt"
        )
        requirements_lock_map.update(requirements_dev_lock_map)

    required_modules_map: dict[str, tuple[Requirement, str]] = {}
    for required_module in required_modules:
        try:
            requirement = Requirement(required_module)
            canonical_module = canonicalize_name(requirement.name)
            required_modules_map[canonical_module] = (
                requirement,
                required_module,
            )
        except InvalidRequirement:
            logger.warning(f"Unexpected required module spec: {required_module}")

    for locked_module, (
        locked_requirement,
        locked_raw,
    ) in requirements_lock_map.items():
        if locked_module in required_modules_map:
            current_requirement, current_raw = required_modules_map[locked_module]
            if str(current_requirement.specifier) != str(locked_requirement.specifier):
                logger.info(
                    f"Overriding version of '{locked_module}' "
                    f"from '{current_requirement.specifier or 'unspecified'}' "
                    f"to '{locked_requirement.specifier}'"
                )
            if str(current_requirement.marker or "") != str(
                locked_requirement.marker or ""
            ):
                logger.info(
                    f"Overriding marker of '{locked_module}' "
                    f"from '{current_requirement.marker or 'none'}' "
                    f"to '{locked_requirement.marker or 'none'}'"
                )
            required_modules_map[locked_module] = (
                locked_requirement,
                locked_raw,
            )
        else:
            logger.info(f"Adding '{locked_raw}' from lock file")
            required_modules_map[locked_module] = (
                locked_requirement,
                locked_raw,
            )

    required_modules = sorted(raw for _, raw in required_modules_map.values())

    # noinspection PyListCreation
    packages_to_inject = []

    packages_to_inject.append(canonicalize_name(TABSDATA_MODULE_NAME))

    if canonicalize_name(TABSDATA_TOOLKIT_PACKAGE_NAME) in original_tabsdata_packages:
        packages_to_inject.append(canonicalize_name(TABSDATA_TOOLKIT_PACKAGE_NAME))

    if canonicalize_name(TABSDATA_AGENT_PACKAGE_NAME) in original_tabsdata_packages:
        packages_to_inject.append(canonicalize_name(TABSDATA_AGENT_PACKAGE_NAME))

    for package in TABSDATA_PACKAGES:
        if canonicalize_name(package) not in [
            canonicalize_name(TABSDATA_MODULE_NAME),
            canonicalize_name(TABSDATA_TOOLKIT_PACKAGE_NAME),
            canonicalize_name(TABSDATA_AGENT_PACKAGE_NAME),
        ]:
            packages_to_inject.append(canonicalize_name(package))

    logger.info(
        f"Injecting tabsdata packages {packages_to_inject} "
        f"with version {tabsdata_version}"
    )
    for package in packages_to_inject:
        if tabsdata_version is not None:
            new_package_version = f"{package}=={tabsdata_version}"
        else:
            new_package_version = package
        required_modules.append(new_package_version)

    if tabsdata_version is not None:
        current_tabsdata_modules = [
            module
            for module in tabsdata_modules
            if module not in legacy_tabsdata_modules
        ]
        if current_tabsdata_modules:
            logger.warning(
                "Found tabsdata modules in the requirements: "
                f"{current_tabsdata_modules}. "
                "Replacing them with all current tabsdata packages with version "
                f"{tabsdata_version}."
            )
        else:
            logger.warning(
                "Could not find current tabsdata modules in the requirements. "
                "Injecting all current tabsdata packages with version "
                f"{tabsdata_version}."
            )
    else:
        logger.warning(
            "No tabsdata current version. Skipping version regularization..."
        )

    return required_modules


def create_virtual_environment(  # noqa: C901
    requirements_description_file: str,
    locks_folder: str,
    current_instance: str | None = None,
    environment_prefix: str | None = None,
    inject_current_tabsdata: bool = False,
    salt: str | None = None,
) -> str | None:
    """Create a Python virtual environment with pyenv"""

    # Check whether to use the current virtual environment or create a new one instead.
    if os.getenv(TD_USE_CURRENT_VENV, "").lower() in TRUE_VALUES:
        logger.info("TD_USE_CURRENT_VENV is set; using current virtual environment.")
        current_venv_path = sys.prefix
        logger.info(f"Current virtual environment path: {current_venv_path}")
        return current_venv_path

    requirements_data = read_yaml_file(requirements_description_file)

    python_version = requirements_data[PYTHON_VERSION_KEY]
    # We sort the requirements and remove duplicates to ensure consistent hashing
    required_modules = sorted(list(set(requirements_data[PYTHON_PUBLIC_PACKAGES_KEY])))
    if inject_current_tabsdata:
        required_modules = inject_tabsdata_version(required_modules)
    # By default, we install dependencies of the packages provided in the list.
    # This can be overridden by setting the key to False in the requirements file.
    # When inferring the requirements from the local system, we do not install
    # dependencies.
    install_dependencies = requirements_data.get(PYTHON_INSTALL_DEPENDENCIES_KEY, True)
    check_module_availability = requirements_data.get(
        PYTHON_IGNORE_UNAVAILABLE_PUBLIC_PACKAGES_KEY, False
    )

    # Check if the local packages folder exists
    local_packages = (
        os.path.join(
            os.path.dirname(os.path.abspath(requirements_description_file)),
            LOCAL_PACKAGES_FOLDER,
        )
        if os.path.isdir(
            os.path.join(
                os.path.dirname(os.path.abspath(requirements_description_file)),
                LOCAL_PACKAGES_FOLDER,
            )
        )
        else None
    )

    environment_hash = get_dict_hash({
        PYTHON_VERSION_KEY: python_version,
        PYTHON_PUBLIC_PACKAGES_KEY: required_modules,
        PYTHON_INSTALL_DEPENDENCIES_KEY: install_dependencies,
        PYTHON_IGNORE_UNAVAILABLE_PUBLIC_PACKAGES_KEY: check_module_availability,
    })
    if local_packages:
        logger.info(
            f"Local packages folder found: {local_packages}, hashing its contents"
        )
        environment_hash = add_hex_numbers(
            environment_hash, get_dir_hash(local_packages)
        )

    development_packages = requirements_data.get(PYTHON_DEVELOPMENT_PACKAGES_KEY)
    if development_packages:
        development_packages = [
            tuple(pkg) if isinstance(pkg, list) else pkg for pkg in development_packages
        ]
        logger.info(
            f"Development packages provided: {development_packages}, hashing its"
            " contents"
        )
        for development_package_location, _ in development_packages:
            environment_hash = add_hex_numbers(
                environment_hash, get_dir_hash(development_package_location)
            )
    else:
        # Defaulting to an empty array to avoid references issue when inheriting
        # tabsdata packages.
        development_packages: list[tuple[str, PackageProvider]] = []
    if salt:
        environment_hash = add_hex_numbers(environment_hash, hash_string(salt))

    if not os.path.isdir(locks_folder):
        logger.warning(
            f"Locks folder {locks_folder} does not exist. If in production, this could"
            " be a sign of an issue with the --locks-folder parameter provided, as it"
            " should have been created before calling the environment-creation script."
            f" Creating the folder now: {locks_folder}."
        )
        os.makedirs(locks_folder, exist_ok=True)

    if current_instance:
        logical_environment_name = f"td_{current_instance}_{environment_hash}"
    else:
        logical_environment_name = f"td_{environment_hash}"

    # Add the environment prefix if provided, currently used for testing and to create
    # base environments.
    if environment_prefix:
        logical_environment_name = f"{environment_prefix}_{logical_environment_name}"

    time_locked = 0
    real_environment_name = hash_string(logical_environment_name, 10)
    lock_location = os.path.join(locks_folder, f"{real_environment_name}.lock")
    lock = FileLock(lock_location)
    while time_locked < MAXIMUM_LOCK_TIME:
        try:
            logger.info(
                f"Trying to acquire lock '{lock_location}' for environment "
                f"creation of '{logical_environment_name}'."
            )
            with lock.acquire(timeout=ENVIRONMENT_LOCK_TIMEOUT):
                logger.info(
                    f"Lock '{lock_location}' acquired for environment creation of"
                    f"'{logical_environment_name}'. Creating it now."
                )
                real_environment_created = atomic_environment_creation(
                    logical_environment_name,
                    real_environment_name,
                    development_packages,
                    local_packages,
                    python_version,
                    required_modules,
                    install_dependencies,
                    check_module_availability,
                )
                logger.info(
                    f"Environment created: {logical_environment_name} with real "
                    f"name {real_environment_created}. Removing lock {lock_location}."
                )
                return real_environment_created

        except Timeout:
            time_locked += ENVIRONMENT_LOCK_TIMEOUT
            logger.warning(
                "Could not acquire lock for environment creation after"
                f" {time_locked} seconds. Retrying..."
            )
    logger.error(
        f"Failed to acquire lock '{lock_location}' for the creation of "
        f"environment '{logical_environment_name}' after {time_locked} seconds. "
        "Exiting environment creation with an error."
    )
    return None


def store_testimony(logical_environment_name, real_environment_name):
    logger.info(
        f"Storing testimony of the environment '{logical_environment_name}' in "
        f"'{DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER}'"
    )
    os.makedirs(DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER, exist_ok=True)
    testimony_file = os.path.join(
        DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER, logical_environment_name
    )
    now = datetime.now(tz=timezone.utc)
    testimony_data = TestimonyData(
        real_name=real_environment_name,
        logical_name=logical_environment_name,
        last_used=now.isoformat(),
        last_used_timestamp=now.timestamp(),
        times_used=1,
    )
    with open(testimony_file, "w") as f:
        yaml.dump(
            asdict(testimony_data),
            f,
            default_flow_style=False,
            sort_keys=False,
            Dumper=yaml.SafeDumper,
        )
    logger.info(f"Testimony file '{testimony_file}' stored successfully.")


def read_testimony(logical_environment_name) -> TestimonyData:
    testimony_file = os.path.join(
        DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER, logical_environment_name
    )
    if not os.path.exists(testimony_file):
        raise FileNotFoundError(
            f"Testimony file '{testimony_file}' does not exist for environment "
            f"'{logical_environment_name}'"
        )
    try:
        with open(testimony_file, "r") as f:
            testimony_dict = yaml.safe_load(f)
        if not isinstance(testimony_dict, dict):
            raise ValueError(
                f"Testimony file '{testimony_file}' does not contain a valid yaml."
            )
        return TestimonyData(**testimony_dict)
    except TypeError as error:
        raise ValueError(
            f"Testimony file '{testimony_file}' is missing required fields: {error}"
        ) from error
    except yaml.YAMLError as error:
        raise ValueError(
            f"Failed to parse testimony file '{testimony_file}' as yaml: {error}"
        ) from error
    except Exception as error:
        raise ValueError(
            f"Error reading testimony file '{testimony_file}': {error}"
        ) from error


def update_testimony(logical_environment_name, real_environment_name):
    logger.info(
        f"Updating testimony of the environment '{logical_environment_name}' in "
        f"'{DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER}'"
    )
    os.makedirs(DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER, exist_ok=True)
    testimony_file = os.path.join(
        DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER, logical_environment_name
    )
    lock_file = f"{testimony_file}.lock"
    lock = FileLock(lock_file, timeout=TESTIMONY_LOCK_TIMEOUT)
    try:
        logger.info(
            f"Trying to acquire lock '{lock_file}' for testimony update of "
            f"'{logical_environment_name}'."
        )
        with lock:
            logger.info(
                f"Lock '{lock_file}' acquired for testimony update of "
                f"'{logical_environment_name}'. Updating it now."
            )
            existing_testimony = read_testimony(logical_environment_name)
            now = datetime.now(tz=timezone.utc)
            testimony_data = TestimonyData(
                real_name=real_environment_name,
                logical_name=logical_environment_name,
                last_used=now.isoformat(),
                last_used_timestamp=now.timestamp(),
                times_used=existing_testimony.times_used + 1,
            )
            with open(testimony_file, "w") as f:
                yaml.dump(
                    asdict(testimony_data),
                    f,
                    default_flow_style=False,
                    sort_keys=False,
                    Dumper=yaml.SafeDumper,
                )
            logger.info(
                f"Testimony file '{testimony_file}' updated successfully. "
                f"Removing lock {lock_file}."
            )
    except Timeout:
        logger.warning(
            f"Could not acquire lock '{lock_file}' for testimony update after "
            f"{TESTIMONY_LOCK_TIMEOUT} seconds. This may indicate a stuck lock or "
            "heavy concurrent access."
        )
        raise


def delete_testimony(environment_name):
    logger.info(
        f"Deleting testimony of the environment '{environment_name}' in "
        f"'{DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER}'"
    )
    testimony_file = os.path.join(
        DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER, environment_name
    )
    try:
        os.remove(testimony_file)
    except FileNotFoundError:
        logger.warning(f"Testimony file '{testimony_file}' not found.")
    else:
        logger.info(f"Testimony file '{testimony_file}' deleted successfully.")

    logger.info(
        f"Deleting testimony lock file of the environment '{environment_name}' in "
        f"'{DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER}'"
    )
    testimony_lock_file = os.path.join(
        DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER, f"{environment_name}{LOCK_EXTENSION}"
    )
    try:
        os.remove(testimony_lock_file)
    except FileNotFoundError:
        logger.warning(f"Testimony lock file '{testimony_lock_file}' not found.")
    else:
        logger.info(
            f"Testimony lock file '{testimony_lock_file}' deleted successfully."
        )


def check_if_testimony_exists(environment_name) -> bool:
    testimony_file = os.path.join(
        DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER, environment_name
    )
    logger.info(f"Checking if testimony file '{testimony_file}' exists.")
    try:
        existing_testimonies = sorted(os.listdir(DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER))
        logger.debug("Existing testimonies:")
        for testimony in existing_testimonies:
            logger.debug(f"· {testimony}")
    except FileNotFoundError:
        logger.warning(
            f"Testimony folder '{DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER}' not found."
        )
    if os.path.exists(testimony_file):
        logger.info(f"Testimony file '{testimony_file}' exists.")
        return True
    else:
        logger.info(f"Testimony file '{testimony_file}' does not exist.")
        return False


def atomic_environment_creation(
    logical_environment_name: str,
    real_environment_name: str,
    development_packages: list[tuple[str, PackageProvider]],
    local_packages: str,
    python_version: str,
    required_modules: list[str],
    install_dependencies: bool,
    check_module_availability: bool,
) -> str | None:
    # Check if the Python environment already exists
    existing_virtual_environments = get_existing_virtual_environments()
    # We verify that the environment exists (environment_name in
    # existing_virtual_environments) and that it has been properly initialized
    # (testimony_exists(environment_name)). This is to avoid two edge cases: if the
    # environment was properly created and then deleted, there will be a testimony
    # but the environment will not appear in existing_virtual_environments. If the
    # environment was created but then the process of installing dependencies was
    # interrupted, it will show as existing but there will be no testimony, since it
    # is stored at the end of a successful environment creation.
    if (
        check_if_testimony_exists(logical_environment_name)
        and real_environment_name in existing_virtual_environments
    ):
        logger.info(
            f"Python virtual environment {logical_environment_name} already exists "
            f"with the real name {real_environment_name}."
        )
        update_testimony(logical_environment_name, real_environment_name)
        return real_environment_name
    # Ensure we are working with a clean slate for the environment
    delete_virtual_environment(
        logical_environment_name=logical_environment_name,
        real_environment_name=real_environment_name,
        log_error_on_fail=False,
    )
    # Install the required Python version (only if not already available and working.)
    if is_python_version_available(python_version):
        logger.info(
            f"Python version '{python_version}' is already installed and "
            f"functional; skipping installation."
        )
    else:
        logger.info(
            f"Installing Python version '{python_version}' for the environment"
            f" '{logical_environment_name}'."
        )
        install_python_version(python_version)
        logger.info(
            f"Python version '{python_version}' installed for the environment"
            f" '{logical_environment_name}'."
        )
    # Create the Python virtual environment
    logger.info(
        f"Creating Python virtual environment {logical_environment_name} with Python"
        f" version {python_version}"
    )
    command = [
        UV_EXECUTABLE,
        "venv",
        "--link-mode",
        "hardlink",
        "--python",
        python_version,
        "--seed",
        os.path.join(DEFAULT_ENVIRONMENT_FOLDER, real_environment_name),
    ]
    logger.info(f"Running command: {' '.join(command)}")
    with time_block:
        result = subprocess.run(command)
        logger.debug(f"Result: {result}")
    if result.returncode != 0:
        logger.error(
            "Failed to create virtual environment with Python version"
            f" {python_version} and environment name {logical_environment_name}. Please"
            " check err.log in the same folder for more details."
        )
        logger.error(
            "Reason:"
            f"\n'{str(result.returncode)}'"
            f"\n'{str(result.stdout)}'"
            f"\n'{str(result.stderr)}'"
        )
        return None
    else:
        logger.info(
            f"Python virtual environment {logical_environment_name} created"
            f" successfully. Time taken: {time_block.time_taken():.2f}s"
        )
    # Install the required Python packages
    pip_upgrade_command = add_python_target_to_command(
        [
            UV_EXECUTABLE,
            "pip",
            "install",
            "--link-mode",
            "hardlink",
            "--upgrade",
            "pip",
        ],
        real_environment_name,
    )
    logger.info(
        f"Upgrading pip version for the virtual environment {logical_environment_name}"
    )
    logger.info(f"Running command: '{' '.join(pip_upgrade_command)}'")
    with time_block:
        result = subprocess.run(
            pip_upgrade_command,
            capture_output=True,
            text=True,
        )
        logger.info(f"Result: {result}")
        # noinspection PyUnreachableCode
        if result.returncode != 0:
            logger.error(
                "Failed to upgrade pip version for the virtual environment"
                f" {logical_environment_name}"
            )
            logger.error(
                "Reason:"
                f"\n'{str(result.returncode)}'"
                f"\n'{str(result.stdout)}'"
                f"\n'{str(result.stderr)}'"
            )
            # delete_virtual_environment(
            #     logical_environment_name=logical_environment_name,
            #     real_environment_name=real_environment_name,
            # )
            return None
    logger.info(
        "pip upgraded successfully for the virtual environment "
        f"{logical_environment_name}. Time taken: {time_block.time_taken():.2f}s"
    )
    # Remove the packages that are not available on PyPi
    logger.info("Selecting all the packages that are available on PyPi")
    with time_block:
        required_modules = (
            found_requirements(
                required_modules,
                development_packages,
                real_environment_name,
            )
            if check_module_availability
            else required_modules
        )
    logger.info(
        "Selected all the packages that are available on PyPi. Time taken:"
        f" {time_block.time_taken():.2f}s"
    )
    # Install the required packages
    result = install_requirements(
        required_modules,
        install_dependencies,
        logical_environment_name,
        real_environment_name,
    )
    if not result:
        return None

    tabsdata_toolkit_local_packages: list[str] = []
    non_tabsdata_toolkit_local_packages: list[str] = []
    tabsdata_toolkit_development_packages: list[tuple[str, PackageProvider]] = []
    non_tabsdata_toolkit_development_packages: list[tuple[str, PackageProvider]] = []

    if local_packages:
        for local_package in os.listdir(local_packages):
            package_folder = os.path.join(local_packages, local_package)
            pyproject_path = os.path.join(package_folder, "pyproject.toml")
            is_toolkit = False
            if os.path.exists(pyproject_path):
                with open(pyproject_path, "rb") as f:
                    data = tomllib.load(f)
                package_name = data.get("tool", {}).get("tabsdata", {}).get("name")
                if package_name == TABSDATA_TOOLKIT_PACKAGE_NAME:
                    is_toolkit = True
            if is_toolkit:
                tabsdata_toolkit_local_packages.append(package_folder)
            else:
                non_tabsdata_toolkit_local_packages.append(package_folder)

    if development_packages:
        tabsdata_toolkit_package_canonical_name = canonicalize_name(
            TABSDATA_TOOLKIT_PACKAGE_NAME
        )
        for (
            development_package_location,
            development_package_provider,
        ) in development_packages:
            development_package_name = os.path.basename(
                development_package_location.rstrip(os.sep)
            )
            if (
                canonicalize_name(development_package_name)
                == tabsdata_toolkit_package_canonical_name
            ):
                tabsdata_toolkit_development_packages.append((
                    development_package_location,
                    development_package_provider,
                ))
            else:
                non_tabsdata_toolkit_development_packages.append((
                    development_package_location,
                    development_package_provider,
                ))

    if tabsdata_toolkit_local_packages:
        logger.info("Installing tabsdata-toolkit as a local package...")
        for package_folder in tabsdata_toolkit_local_packages:
            result = install_host_package(
                "Local",
                package_folder,
                None,
                logical_environment_name,
                real_environment_name,
            )
            if not result:
                return None

    if tabsdata_toolkit_development_packages:
        logger.info("Installing tabsdata-toolkit as a development package...")
        result = install_development_packages(
            tabsdata_toolkit_development_packages,
            logical_environment_name,
            real_environment_name,
        )
        if not result:
            return None

    if non_tabsdata_toolkit_local_packages:
        for package_folder in non_tabsdata_toolkit_local_packages:
            result = install_host_package(
                "Local",
                package_folder,
                None,
                logical_environment_name,
                real_environment_name,
            )
            if not result:
                return None

    if non_tabsdata_toolkit_development_packages:
        result = install_development_packages(
            non_tabsdata_toolkit_development_packages,
            logical_environment_name,
            real_environment_name,
        )
        if not result:
            return None

    store_testimony(logical_environment_name, real_environment_name)
    return real_environment_name


def get_path_to_environment_bin(python_environment):
    if CURRENT_PLATFORM.is_windows():
        path_to_environment_bin = os.path.join(
            DEFAULT_ENVIRONMENT_FOLDER, python_environment, "Scripts", "python.exe"
        )
    else:
        path_to_environment_bin = os.path.join(
            DEFAULT_ENVIRONMENT_FOLDER, python_environment, "bin", "python"
        )
    return path_to_environment_bin


def add_python_target_to_command(
    command: list[str], environment_name: str
) -> list[str]:
    """Append the python target option to the given command argv list."""
    return command + [
        "--python",
        get_path_to_environment_bin(environment_name),
    ]


def install_requirements(
    requirements: list[str],
    install_dependencies: bool,
    logical_environment_name: str,
    real_environment_name: str,
) -> bool:
    """Install the required Python packages to the current environment.
    Returns true if the requirements are installed successfully, false otherwise."""

    if not requirements:
        logger.warning("No requirements to install")
        return True

    pip_install_requirements_command = add_python_target_to_command(
        [
            UV_EXECUTABLE,
            "pip",
            "install",
            "--link-mode",
            "hardlink",
        ]
        + requirements,
        real_environment_name,
    )
    if not install_dependencies:
        logger.info("Installing the requirements without dependencies")
        pip_install_requirements_command.append("--no-deps")
    else:
        logger.info("Installing the requirements with dependencies")
    logger.info(
        f"Installing the requirements {requirements} for the virtual environment"
        f" {logical_environment_name}"
    )
    logger.info(f"Running command: '{' '.join(pip_install_requirements_command)}'")
    with time_block:
        result = subprocess.run(
            pip_install_requirements_command,
            capture_output=True,
            text=True,
        )
    if result.returncode != 0:
        logger.error(
            f"Failed to install the requirements {requirements} for the virtual"
            f" environment {logical_environment_name}"
        )
        logger.error(
            "Reason:"
            f"\n'{str(result.returncode)}'"
            f"\n'{str(result.stdout)}'"
            f"\n'{str(result.stderr)}'"
        )
        # delete_virtual_environment(
        #     logical_environment_name=logical_environment_name,
        #     real_environment_name=real_environment_name,
        # )
        return False
    else:
        logger.info(
            f"Requirements {requirements} installed successfully for the virtual"
            f" environment {logical_environment_name}. Time taken:"
            f" {time_block.time_taken():.2f}s"
        )
        return True


def install_development_packages(
    development_packages: list[tuple[str, PackageProvider]],
    logical_environment_name: str,
    real_environment_name: str,
) -> bool | None:
    logger.info(f"Installing development packages for folder: {development_packages}")
    with time_block:
        for (
            development_package_location,
            development_package_provider,
        ) in development_packages:
            install_result = install_host_package(
                "Development",
                development_package_location,
                development_package_provider,
                logical_environment_name,
                real_environment_name,
            )
            if not install_result:
                return install_result
    logger.info(
        "Development packages installed successfully. Time taken:"
        f" {time_block.time_taken():.2f}s"
    )
    return True


def install_local_packages(
    local_packages: str,
    logical_environment_name: str,
    real_environment_name: str,
) -> bool | None:
    logger.info(f"Installing local packages for folder: {local_packages}")
    with time_block:
        for local_package in os.listdir(local_packages):
            package_folder = os.path.join(local_packages, local_package)
            install_result = install_host_package(
                "Local",
                package_folder,
                None,
                logical_environment_name,
                real_environment_name,
            )
            if not install_result:
                return install_result
    logger.info(
        "Local packages installed successfully. Time taken:"
        f" {time_block.time_taken():.2f}s"
    )
    return True


def link_base_binaries(  # noqa: C901
    package_location: str,
    real_environment_name: str,
) -> None:
    try:
        setup_toml_path = os.path.join(package_location, "setup.toml")
        if not os.path.exists(setup_toml_path):
            return

        with open(setup_toml_path, "rb") as setup_toml_file:
            setup_config = tomllib.load(setup_toml_file)
            base_binaries = setup_config.get("setup", {}).get("binaries", [])

        if not base_binaries:
            return

        current_venv_scripts = sysconfig.get_path("scripts")

        if CURRENT_PLATFORM.is_windows():
            new_venv_scripts = os.path.join(
                DEFAULT_ENVIRONMENT_FOLDER, real_environment_name, "Scripts"
            )
        else:
            new_venv_scripts = os.path.join(
                DEFAULT_ENVIRONMENT_FOLDER, real_environment_name, "bin"
            )

        for base_binary in base_binaries:
            for binary_name in [base_binary, f"{base_binary}.exe"]:
                source_binary = os.path.join(current_venv_scripts, binary_name)
                target_binary = os.path.join(new_venv_scripts, binary_name)
                if os.path.exists(source_binary):
                    try:
                        os.symlink(source_binary, target_binary)
                        now = time.time()
                        os.utime(new_venv_scripts, (now, now))
                        os.utime(target_binary, (now, now))
                        logger.info(
                            f"Created symlink for binary '{binary_name}' "
                            f"from '{source_binary}' "
                            f"to '{target_binary}'"
                        )
                    except FileExistsError:
                        logger.debug(
                            f"Symlink '{target_binary}' already exists for "
                            f"binary '{binary_name}'"
                            f"from '{source_binary}'"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to create symlink '{target_binary}' for "
                            f"binary '{binary_name}'"
                            f"from '{source_binary}': {e}"
                        )
                    break
                else:
                    logger.info(
                        f"Bypassing symlink for binary '{binary_name}' "
                        f"from '{source_binary}' "
                        f"to '{target_binary}'"
                    )
    except Exception as e:
        logger.warning(f"Failed to process binaries from setup.toml: {e}")


def extract_tabsdata_package_name(package_location: str) -> str | None:
    if not os.path.isdir(package_location):
        return None
    pyproject_file = os.path.join(package_location, "pyproject.toml")
    if not os.path.exists(pyproject_file):
        return None
    # noinspection PyBroadException
    try:
        with open(pyproject_file, "rb") as f:
            pyproject_toml = tomllib.load(f)
        package_name = pyproject_toml.get("tool", {}).get("tabsdata", {}).get("name")
        if package_name and canonicalize_name(package_name) in TABSDATA_PACKAGES:
            return package_name
    except Exception:
        logger.warning(
            "Error extracting tabsdata package name from pyproject.toml: "
            "{package_location} - {error}"
        )
    return None


def install_host_package(  # noqa: C901
    source: HostPackageSource,
    package_location: str,
    package_provider: PackageProvider | None,
    logical_environment_name: str,
    real_environment_name: str,
) -> bool | None:
    logger.info(f"Installing host package archive: {package_location}")
    if os.path.isdir(package_location) or (
        os.path.isfile(package_location)
        and pathlib.Path(package_location).suffix == WHEEL_EXTENSION
    ):
        install_command = [
            UV_EXECUTABLE,
            "pip",
            "install",
            "--link-mode",
            "hardlink",
        ]
        as_editable = False
        with_isolation = True
        if package_provider in [
            "Archive (Project)",
            "Archive (Folder)",
            "Folder (Editable)",
            "Folder (Frozen)",
        ]:
            if package_provider in ["Folder (Editable)"]:
                as_editable = True
            with_isolation = False
            link_base_binaries(package_location, real_environment_name)
        elif source == "Local":
            logger.info(f"Identified a local package: '{package_location}'")
            pyproject_path = os.path.join(package_location, "pyproject.toml")
            if os.path.exists(pyproject_path):
                logger.info(
                    "Identified a local package with pyproject.toml: "
                    f"'{package_location}'"
                )
                with open(pyproject_path, "rb") as f:
                    data = tomllib.load(f)
                package_name = data.get("tool", {}).get("tabsdata", {}).get("name")
                if package_name:
                    logger.info(
                        "Identified name in pyproject.toml: "
                        f"'{package_location}' - '{package_name}'"
                    )
                if package_name and package_name in TABSDATA_PACKAGES:
                    if package_name == TABSDATA_TOOLKIT_PACKAGE_NAME:
                        logger.info(
                            f"Identified tabsdata toolkit local package: "
                            f"'{package_location}' - '{package_name}'. "
                            f"Installing as non-editable local package."
                        )
                        as_editable = False
                        with_isolation = False
                    else:
                        logger.info(
                            f"Skipping local tabsdata package: "
                            f"'{package_location}' - '{package_name}'. "
                            f"It will be installed as a development package."
                        )
                        return True
        if as_editable:
            install_command.append("-e")
        install_command.append(package_location)
        if as_editable:
            install_command.extend([
                "--config-settings",
                "editable_mode=compat",
            ])
        if not with_isolation:
            install_command.extend([
                "--no-build-isolation",
            ])
        tabsdata_pkg_name = extract_tabsdata_package_name(package_location)
        if as_editable and tabsdata_pkg_name:
            install_command.extend([
                "--reinstall",
                "--no-cache",
                "--refresh-package",
                tabsdata_pkg_name,
            ])
            logger.info(
                f"Forcing full rebuild for editable tabsdata package "
                f"'{tabsdata_pkg_name}' in venv '{real_environment_name}' "
                f"(--reinstall --no-cache --refresh-package {tabsdata_pkg_name})"
            )
        pip_install_requirements_command = add_python_target_to_command(
            install_command,
            real_environment_name,
        )
        logger.info(
            f"Installing the host package at {package_location} "
            f"with provider {package_provider} "
            f"and source {source} "
            f"for the virtual environment {logical_environment_name}"
        )
        logger.info(f"Running command: '{' '.join(pip_install_requirements_command)}'")
        logger.info(
            f"Build environment variables:\n"
            f"td-target='{os.environ.get(ENV_td_target, 'unset')}'\n"
            f"TD_TARGET='{os.environ.get(ENV_TD_TARGET, 'unset')}'\n"
            f"profile='{os.environ.get(ENV_profile, 'unset')}\n"
            f"PROFILE='{os.environ.get(ENV_PROFILE, 'unset')}'"
        )
        pip_environment = os.environ.copy()
        if PYTHONWARNINGS in pip_environment:
            del pip_environment[PYTHONWARNINGS]
        if source == "Local":
            pip_environment[REQUIRE_SERVER_BINARIES] = "false"
            pip_environment[REQUIRE_THIRD_PARTY] = "false"
            pip_environment[TD_IGNORE_CONNECTOR_REQUIREMENTS] = "true"
            pip_environment[TD_SKIP_NON_EXISTING_ASSETS] = "true"
            pip_environment[TD_SETUP_GENERATE_LOCKS] = "false"
        result = subprocess.run(
            pip_install_requirements_command,
            capture_output=True,
            text=True,
            env=pip_environment,
        )
        if result.returncode != 0:
            logger.error(
                f"Failed to install the host package {package_location} for the "
                f"virtual environment {logical_environment_name}"
            )
            logger.error(
                "Reason:"
                f"\n'{str(result.returncode)}'"
                f"\n'{str(result.stdout)}'"
                f"\n'{str(result.stderr)}'"
            )
            # delete_virtual_environment(
            #     logical_environment_name=logical_environment_name,
            #     real_environment_name=real_environment_name,
            # )
            return None
        else:
            logger.info(
                f"Host package {package_location} installed successfully for the"
                f" virtual environment {logical_environment_name}"
            )
    else:
        message = (
            f"Host package '${package_location}' is not a directory or a wheel file."
        )
        if source == "Development":
            logger.error(message)
            return None
        else:
            logger.warning(f"{message} Discarding it.")
    return True


def find_matching_python_versions(python_version: str) -> list[dict]:
    result = subprocess.run(
        [
            UV_EXECUTABLE,
            "python",
            "list",
            "--only-installed",
            "--output-format",
            "json",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        return []
    entries = json.loads(result.stdout)
    python_version_parts = python_version.split(".")
    return [
        entry
        for entry in entries
        if all(
            str(entry["version_parts"][k]) == v
            for k, v in zip(["major", "minor", "patch"], python_version_parts)
        )
    ]


def is_python_version_available(python_version: str) -> bool:
    try:
        python_matching_versions = find_matching_python_versions(python_version)
        if not python_matching_versions:
            return False

        deadline = time.monotonic() + STDLIB_RETRY_TIMEOUT_SECONDS
        check = False
        while True:
            for python_matching_version in python_matching_versions:
                python_path = python_matching_version["path"]
                if not os.path.isfile(python_path):
                    continue
                # noinspection SpellCheckingInspection
                check = subprocess.run(
                    [
                        python_path,
                        "-c",
                        "import pkgutil",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                if check.returncode == 0:
                    logger.info(
                        f"Python {python_matching_version['version']} "
                        f"at {python_path} is available and functional."
                    )
                    return True

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                logger.warning(
                    f"Python {python_version} found but stdlib check failed "
                    f"after {STDLIB_RETRY_TIMEOUT_SECONDS}s: "
                    f"{check.stderr.strip()}"
                )
                return False

            logger.info(
                f"Python {python_version} stdlib not yet available; retrying "
                f"in {STDLIB_RETRY_INTERVAL_SECONDS}s ({remaining:.0f}s "
                f"remaining): {check.stderr.strip()}"
            )
            time.sleep(STDLIB_RETRY_INTERVAL_SECONDS)
    except Exception as error:
        logger.warning(f"Could not check Python version availability: {error}")
        return False


def install_python_version(python_version: str) -> None:
    logger.info(f"Installing Python version {python_version}")
    command = [
        UV_EXECUTABLE,
        "python",
        "install",
        "-v",
        "-n",
        python_version,
    ]
    logger.info(f"Running command: {' '.join(command)}")
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )
    # On Windows, exit status 2 means that the Python version is already installed,
    ok_codes = (0, 2) if platform.system() == "Windows" else (0,)
    # Check if the Python version is not found
    if result.returncode not in ok_codes:
        logger.error(f"Failed to install Python version '{python_version}'.")
        logger.error(
            "Reason:"
            f"\n'{str(result.returncode)}'"
            f"\n'{str(result.stdout)}'"
            f"\n'{str(result.stderr)}'"
        )
        logger.error("Please check the Python version number provided and try again.")
        return None
    else:
        logger.info(
            f"Python version {python_version} installed successfully or "
            "already existed."
        )
        return None


def get_tabsdata_package_metadata(
    module: str,
    variable: str | None,
) -> Tuple[
    PackageProvider | None,
    str | None,
]:
    td_tabsdata_dev_pkg = os.getenv(variable) if variable is not None else None
    if td_tabsdata_dev_pkg:
        return (
            "Archive (Project)",
            normalize_location(pathlib.Path(td_tabsdata_dev_pkg)),
        )
    try:
        packages = {
            canonicalize_name(dist.metadata["Name"]): dist.version
            for dist in importlib.metadata.distributions()
        }
        if canonicalize_name(module) not in packages:
            return (
                None,
                None,
            )
        distribution = importlib.metadata.distribution(canonicalize_name(module))
        direct_url_content = distribution.read_text("direct_url.json")
        if direct_url_content is None:
            return (
                "Package",
                None,
            )
        direct_url_data = json.loads(direct_url_content)
        provider, location = extract_from_direct_url_data(direct_url_data)
        return (
            provider,
            normalize_location(location),
        )
    except importlib.metadata.PackageNotFoundError:
        return (
            None,
            None,
        )


def extract_from_direct_url_data(
    direct_url_data: dict,
) -> tuple[PackageProvider | None, str | None]:
    url = direct_url_data.get("url", "")
    if not url.startswith(FILE_PROTOCOL):
        return (
            None,
            None,
        )
    url_string = url[len(FILE_PROTOCOL) :]
    if os.name == WINDOWS_OS_NAME and url_string.startswith(WINDOWS_URL_PREFIX):
        url_string = url_string[len(WINDOWS_URL_PREFIX) :]
    url_path = Path(url_string)
    if url_path.suffix == WHEEL_EXTENSION:
        if url_path.exists():
            return (
                "Archive (Wheel)",
                str(url_path),
            )
        while url_path.name != TARGET_FOLDER and url_path.parent != url_path:
            url_path = url_path.parent
        if url_path.name == TARGET_FOLDER:
            url_path = url_path.parent
        return (
            "Archive (Folder)",
            url_path,
        )
    is_editable = direct_url_data.get("dir_info", {}).get("editable", False)
    if is_editable:
        return (
            "Folder (Editable)",
            str(url_path),
        )
    return (
        "Folder (Frozen)",
        str(url_path),
    )


def normalize_location(
    location: pathlib.Path | None,
) -> pathlib.Path | None:
    if location is None:
        return None
    location_string = str(location)
    if location_string.startswith(BACK_SLASH):
        normalized = Path(location_string[len(BACK_SLASH) :])
        logger.info(f"Normalized location from '{location_string}' to '{normalized}'")
        return normalized
    return location


def inspect():
    _packages = sorted([
        pkg.metadata["Name"] for pkg in importlib.metadata.distributions()
    ])
    _modules = sorted([module.name for module in pkgutil.iter_modules()])

    logger.debug("📦 Installed Packages:")
    for package in _packages:
        logger.debug(f"   🗂️ · {package}")
    logger.debug("📚 Available Modules:")
    for module in _modules:
        logger.debug(f"   🗂️ · {module}")


def retrieve_tabsdata_connectors_packages() -> list[tuple[str, PackageProvider]]:
    tabsdata_connectors_development_packages: list[tuple[str, PackageProvider]] = []
    for module_name, metadata in TABSDATA_CONNECTORS.items():
        provider, location = get_tabsdata_package_metadata(
            module_name,
            metadata["is_dev_env"],
        )
        logger.info(
            f"Module {module_name} classified as: "
            f"provider: {provider} - "
            f"location: {location}"
        )

        if provider in (
            "Archive (Project)",
            "Archive (Folder)",
            "Archive (Wheel)",
            "Folder (Editable)",
            "Folder (Frozen)",
        ):
            tabsdata_connectors_development_packages.append((str(location), provider))
    return tabsdata_connectors_development_packages


# noinspection DuplicatedCode
def retrieve_tabsdata_package() -> Tuple[
    PackageProvider | None, str | None, list[tuple[str, PackageProvider]]
]:
    tabsdata_development_packages: list[tuple[str, PackageProvider]] = []
    provider, location = get_tabsdata_package_metadata(
        TABSDATA_MODULE_NAME,
        TD_TABSDATA_DEV_PKG,
    )
    logger.info(
        f"Module tabsdata classified as: provider: {provider} - location: {location}"
    )

    if provider in (
        "Archive (Project)",
        "Archive (Folder)",
        "Archive (Wheel)",
        "Folder (Editable)",
        "Folder (Frozen)",
    ):
        tabsdata_development_packages.append((str(location), provider))
    return provider, location, tabsdata_development_packages


# noinspection DuplicatedCode
def retrieve_tabsdata_toolkit_package() -> Tuple[
    PackageProvider | None, str | None, list[tuple[str, PackageProvider]]
]:
    tabsdata_toolkit_development_packages: list[tuple[str, PackageProvider]] = []
    provider, location = get_tabsdata_package_metadata(
        TABSDATA_TOOLKIT_PACKAGE_NAME,
        TD_TABSDATA_TOOLKIT_DEV_PKG,
    )
    logger.info(
        f"Package tabsdata-toolkit classified as: provider: {provider} - location:"
        f" {location}"
    )

    if provider in (
        "Archive (Project)",
        "Archive (Folder)",
        "Archive (Wheel)",
        "Folder (Editable)",
        "Folder (Frozen)",
    ):
        tabsdata_toolkit_development_packages.append((str(location), provider))
    return provider, location, tabsdata_toolkit_development_packages


# noinspection DuplicatedCode
def retrieve_tabsdata_agent_package() -> Tuple[
    PackageProvider | None, str | None, list[tuple[str, PackageProvider]]
]:
    tabsdata_agent_development_packages: list[tuple[str, PackageProvider]] = []
    provider, location = get_tabsdata_package_metadata(
        TABSDATA_AGENT_PACKAGE_NAME,
        TD_TABSDATA_AGENT_DEV_PKG,
    )
    logger.info(
        f"Package tabsdata-agent classified as: provider: {provider} - location:"
        f" {location}"
    )

    if provider in (
        "Archive (Project)",
        "Archive (Folder)",
        "Archive (Wheel)",
        "Folder (Editable)",
        "Folder (Frozen)",
    ):
        tabsdata_agent_development_packages.append((str(location), provider))
    return provider, location, tabsdata_agent_development_packages


def retrieve_tabsdata_requirements_packages(
    tabsdata_requirements: list[str],
) -> Tuple[list[tuple[str, PackageProvider]], list[str]]:
    tabsdata_requirements_development_packages: list[tuple[str, PackageProvider]] = []
    tabsdata_requirements_non_development_packages = []
    for requirement in tabsdata_requirements:
        requirement_object = Requirement(requirement)
        module_name = requirement_object.name
        is_dev_env = f"TD_{requirement_object.name.upper().replace('-', '_')}_DEV_PKG"
        provider, location = get_tabsdata_package_metadata(
            module_name,
            is_dev_env,
        )
        logger.info(
            f"Module {module_name} classified as: "
            f"provider: {provider} - "
            f"location: {location}"
        )

        if provider in (
            "Archive (Project)",
            "Archive (Folder)",
            "Archive (Wheel)",
            "Folder (Editable)",
            "Folder (Frozen)",
        ):
            tabsdata_requirements_development_packages.append((str(location), provider))
        else:
            tabsdata_requirements_non_development_packages.append(str(requirement))
    return (
        tabsdata_requirements_development_packages,
        tabsdata_requirements_non_development_packages,
    )


def extract_requirements(
    requirements_path: Path,
) -> tuple[list[str], list[str]]:
    tabsdata_requirements: list[str] = []
    non_tabsdata_requirements: list[str] = []

    if requirements_path is None:
        return tabsdata_requirements, non_tabsdata_requirements

    resolved_path = requirements_path.expanduser().resolve()
    if not resolved_path.exists():
        message = f"Requirements file does not exist: '{resolved_path}'"
        logger.error(message)
        raise FileNotFoundError(message)
    if not resolved_path.is_file():
        message = f"Requirements path is not a file: '{resolved_path}'"
        logger.error(message)
        raise ValueError(message)

    with resolved_path.open(encoding="utf-8") as requirements_file:
        for requirement_line in requirements_file:
            requirement_entry = requirement_line.strip()
            if not requirement_entry or requirement_entry.startswith("#"):
                continue
            try:
                requirement = Requirement(requirement_entry)
            except InvalidRequirement as error:
                message = (
                    "Invalid entry in requirements file "
                    f"'{resolved_path}': '{requirement_entry}'"
                )
                logger.error(message)
                raise ValueError(message) from error
            normalized_requirement = str(requirement)
            if requirement.name.startswith(TABSDATA_MODULE_NAME):
                tabsdata_requirements.append(normalized_requirement)
            else:
                non_tabsdata_requirements.append(normalized_requirement)

    logger.debug(
        "Validated requirements file '%s' with %d tabsdata and %d external entries",
        requirements_path,
        len(tabsdata_requirements),
        len(non_tabsdata_requirements),
    )

    return tabsdata_requirements, non_tabsdata_requirements


def extract_instance(instance: str) -> Tuple[str, Path]:
    instance = instance or DEFAULT_INSTANCE
    instance_path = Path(instance)
    if instance_path.is_absolute():
        if instance_path.exists() and not instance_path.is_dir():
            message = (
                f"Invalid instance: '{instance_path}'. An instance absolute path"
                " must be a directory or not exist."
            )
            logger.error(message)
            raise ValueError(message)
        instance = instance_path.name
    elif os.sep not in instance and (os.altsep is None or os.altsep not in instance):
        instance_path = Path(os.path.join(DEFAULT_INSTANCES_FOLDER, str(instance)))
        if instance_path.exists() and not instance_path.is_dir():
            message = (
                f"Invalid instance: '{instance_path}'. An instance relative path"
                " must be a directory or not exist."
            )
            logger.error(message)
            raise ValueError(message)
        instance = instance_path.name
    else:
        message = (
            f"Invalid instance: '{instance_path}'. It is neither an absolute path"
            " nor a single name."
        )
        logger.error(message)
        raise ValueError(message)
    return instance, instance_path


def main():  # noqa: C901
    def debug_enabled() -> bool:
        return os.getenv(REMOTE_DEBUG, FALSE_FALSE).lower() in TRUE_VALUES

    parser = argparse.ArgumentParser(
        description=(
            "Create a virtual environment to be used by a workers of a tabsdata "
            "instance. The environment will be either the instance base "
            "environment or an environment for a worker with a Python runtime "
            "specification"
        )
    )
    parser.add_argument(
        "--instance",
        type=str,
        help="Path of the tabsdata instance.",
        required=False,
    )
    parser.add_argument(
        "--requirements",
        type=Path,
        help="Path of the environment additional requirements.",
        required=False,
    )
    args = parser.parse_args()

    instance, instance_path = extract_instance(args.instance)

    # noinspection PyBroadException
    try:
        logs_folder = os.path.join(
            instance_path.absolute(),
            WORKSPACE_FOLDER,
            WORK_FOLDER,
            LOG_FOLDER,
        )
        logging_config = td_resource("resources/profile/workspace/config/logging.yaml")
        if os.path.exists(logging_config):
            setup_logging(
                logging_config,
                logs_folder=logs_folder,
            )
        else:
            setup_logging(
                default_path=os.path.join(
                    ABSOLUTE_LOCATION,
                    "logging.yaml",
                ),
                logs_folder=logs_folder,
            )
    except Exception:
        setup_logging(os.path.join(ABSOLUTE_LOCATION, "logging.yaml"))

    inspect()

    # Note: tabsdata added the last one as then dependencies to other tabsdata
    # packages do not need to be accessible through PyPI during development stages.
    with tempfile.NamedTemporaryFile(
        suffix=".yaml",
        mode="w",
        delete=False,
        dir=tabsdata_temp_folder(),
    ) as requirements_file:
        logger.debug(f"Temporary base requirements file: {requirements_file.name}")

        development_packages: list[tuple[str, PackageProvider]] = []

        tabsdata_connectors_development_packages: list[tuple[str, PackageProvider]] = (
            retrieve_tabsdata_connectors_packages()
        )
        development_packages.extend(tabsdata_connectors_development_packages)
        (
            tabsdata_provider,
            tabsdata_location,
            tabsdata_development_packages,
        ) = retrieve_tabsdata_package()
        development_packages.extend(tabsdata_development_packages)
        (
            _tabsdata_toolkit_provider,
            _tabsdata_toolkit_location,
            tabsdata_toolkit_development_packages,
        ) = retrieve_tabsdata_toolkit_package()
        development_packages.extend(tabsdata_toolkit_development_packages)
        (
            _tabsdata_agent_provider,
            _tabsdata_agent_location,
            tabsdata_agent_development_packages,
        ) = retrieve_tabsdata_agent_package()
        development_packages.extend(tabsdata_agent_development_packages)
        (
            additional_tabsdata_requirements,
            additional_non_tabsdata_requirements,
        ) = extract_requirements(args.requirements)
        (
            tabsdata_requirements_development_packages,
            tabsdata_requirements_non_development_packages,
        ) = retrieve_tabsdata_requirements_packages(additional_tabsdata_requirements)
        development_packages.extend(tabsdata_requirements_development_packages)
        development_packages: list[tuple[str, PackageProvider]] = list(
            dict.fromkeys(development_packages)
        )

        public_packages = tabsdata_requirements_non_development_packages
        public_packages.extend(additional_non_tabsdata_requirements)
        if debug_enabled():
            public_packages.extend(DEBUG_PACKAGES)
        public_packages = list(dict.fromkeys(public_packages))

        requirements_path = requirements_file.name
        requirements = {
            PYTHON_VERSION_KEY: PYTHON_BASE_VERSION,
            PYTHON_IGNORE_UNAVAILABLE_PUBLIC_PACKAGES_KEY: (
                tabsdata_provider
                in (
                    "Archive (Project)",
                    "Archive (Folder)",
                    "Archive (Wheel)",
                    "Folder (Editable)",
                    "Folder (Frozen)",
                )
            ),
            PYTHON_PUBLIC_PACKAGES_KEY: public_packages,
            PYTHON_DEVELOPMENT_PACKAGES_KEY: development_packages,
        }

        with open(requirements_path, "w") as file:
            yaml.dump(
                requirements,
                file,
                default_flow_style=False,
                Dumper=yaml.SafeDumper,
            )
        logger.debug(f"Temporary base requirements contents: {requirements}")

        requirements_description_file = requirements_file.name
        locks_folder = os.path.join(
            instance_path.absolute(), WORKSPACE_FOLDER, WORK_FOLDER, LOCK_FOLDER
        )
        current_instance = (
            f"{BASE_ENVIRONMENT_PREFIX}{instance}_{get_current_tabsdata_version()}"
        )
        environment_prefix = None
        inject_current_tabsdata = True
        os.makedirs(locks_folder, exist_ok=True)

        with open(requirements_description_file, "r", encoding="utf-8") as f:
            requirements_description_contents = f.read()
        logger.debug(
            "Creating base virtual environment:"
            f"\n - Requirements File: '{requirements_description_file}'"
            f"\n - Requirements File Contents: '{requirements_description_contents}'"
            f"\n - Lock Folder: '{locks_folder}'"
            f"\n - Current Instance: '{current_instance}'"
            f"\n - Environment Prefix: '{environment_prefix}'"
            f"\n - Inject Current Tabsdata Version: '{inject_current_tabsdata}'"
        )

        environment = create_virtual_environment(
            requirements_description_file,
            locks_folder,
            current_instance,
            environment_prefix,
            inject_current_tabsdata,
            BASE_ENV_SALT,
        )

        if not environment:
            message = "Failed to create the base virtual environment."
            logger.error(message)
            raise ValueError(message)

        logger.info(f"Base virtual environment is now: '{environment}'")
        print(
            "<environment>"
            f"{os.path.join(DEFAULT_ENVIRONMENT_FOLDER, environment)}"
            "</environment>"
        )


if __name__ == "__main__":
    main()
