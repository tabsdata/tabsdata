#
# Copyright 2024 Tabs Data Inc.
#

import argparse
import collections
import hashlib
import json
import logging
import os
import shutil
import subprocess
from pathlib import Path

import yaml
from filelock import FileLock, Timeout
from yaml import MappingNode
from yaml.constructor import ConstructorError

from tabsdata.utils.bundle_utils import (
    LOCAL_PACKAGES_FOLDER,
    PYTHON_CHECK_MODULE_AVAILABILITY_KEY,
    PYTHON_INSTALL_DEPENDENCIES_KEY,
    PYTHON_PUBLIC_PACKAGES_KEY,
    PYTHON_VERSION_KEY,
)
from tabsserver.function_execution.global_utils import CURRENT_PLATFORM
from tabsserver.utils import TimeBlock

logger = logging.getLogger(__name__)
time_block = TimeBlock()

# The environment name is the second last element in the yaml file name when split by .,
# the last being "yaml" (e.g. "python_environment_123456.yaml")
DEFAULT_TABSDATA_FOLDER = os.path.join(os.path.expanduser("~"), ".tabsdata")
DEFAULT_ENVIRONMENT_FOLDER = os.path.join(DEFAULT_TABSDATA_FOLDER, "environments")
DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER = os.path.join(
    DEFAULT_TABSDATA_FOLDER, "available_environments"
)

UV_EXECUTABLE = "uv"

ENVIRONMENT_LOCK_TIMEOUT = 5  # 5 seconds
MAXIMUM_LOCK_TIME = 60 * 30  # 30 minutes
PYTHON_VERSION_LOCK_TIMEOUT = 10  # 10 seconds


def remove_path(path: str):
    if os.path.islink(path):
        origin = os.readlink(path)
        os.unlink(path)
        if os.path.exists(origin):
            if os.path.isdir(origin):
                shutil.rmtree(origin)
            else:
                os.remove(origin)
    else:
        shutil.rmtree(path)


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
    existing_virtual_environments = list_folders(DEFAULT_ENVIRONMENT_FOLDER)
    logger.info(f"Existing virtual environments: {existing_virtual_environments}")
    return existing_virtual_environments


def verify_package_installable_for_environment(
    package: str, real_environment_name: str
) -> bool:
    logger.debug(f"Verifying if package {package} is installable with pip")
    pip_install_dry_run = add_python_target_and_join_commands(
        [
            UV_EXECUTABLE,
            "pip",
            "install",
            package,
            "--dry-run",
            "--no-deps",
        ],
        real_environment_name,
    )
    logger.debug(f"Running command: '{pip_install_dry_run}'")
    with time_block:
        result = subprocess.run(
            pip_install_dry_run,
            shell=True,
        )
    if result.returncode != 0:
        logger.warning(f"Package {package} is not installable with pip")
        return False
    else:
        logger.debug(f"Package {package} is installable with pip")
        return True


def found_requirements(
    requirements: list[str], real_environment_name: str
) -> list[str]:
    logger.info(f"Checking if the packages {requirements} are available on PyPi")
    available_packages = [
        package
        for package in requirements
        if verify_package_installable_for_environment(package, real_environment_name)
    ]
    logger.info(f"Available packages: {available_packages}")
    if set(available_packages) != set(requirements):
        missing_packages = set(requirements) - set(available_packages)
        logger.warning(f"Missing packages: {missing_packages}")
    return available_packages


def get_dict_hash(data):
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
            if not isinstance(node, MappingNode):
                raise ConstructorError(
                    None,
                    None,
                    "expected a mapping node, but found %s" % node.id,
                    node.start_mark,
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


def create_virtual_environment(
    requirements_description_file: str,
    locks_folder: str,
    current_instance: str | None = None,
    environment_prefix: str | None = None,
) -> str | None:
    """Create a Python virtual environment with pyenv"""

    requirements_data = read_yaml_file(requirements_description_file)

    python_version = requirements_data[PYTHON_VERSION_KEY]
    # We sort the requirements and remove duplicates to ensure consistent hashing
    required_modules = sorted(list(set(requirements_data[PYTHON_PUBLIC_PACKAGES_KEY])))
    # By default, we install dependencies of the packages provided in the list.
    # This can be overridden by setting the key to False in the requirements file.
    # When inferring the requirements from the local system, we do not install
    # dependencies.
    install_dependencies = requirements_data.get(PYTHON_INSTALL_DEPENDENCIES_KEY, True)
    check_module_availability = requirements_data.get(
        PYTHON_CHECK_MODULE_AVAILABILITY_KEY, False
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

    environment_hash = get_dict_hash(
        {
            PYTHON_VERSION_KEY: python_version,
            PYTHON_PUBLIC_PACKAGES_KEY: required_modules,
            PYTHON_INSTALL_DEPENDENCIES_KEY: install_dependencies,
            PYTHON_CHECK_MODULE_AVAILABILITY_KEY: check_module_availability,
        }
    )
    if local_packages:
        logger.info(
            f"Local packages folder found: {local_packages}, hashing its contents"
        )
        environment_hash = add_hex_numbers(
            environment_hash, get_dir_hash(local_packages)
        )

    if not os.path.isdir(locks_folder):
        logger.warning(
            f"Locks folder {locks_folder} does not exist. If in production, this could"
            " be a sign of an issue with the --locks-folder parameter provided, as it"
            " should have been created before calling the environment-creation script."
            " Creating the folder now."
        )
        os.makedirs(locks_folder, exist_ok=True)

    if current_instance:
        logical_environment_name = f"td_{current_instance}_{environment_hash}"
    else:
        logical_environment_name = f"td_{environment_hash}"

    # Add the environment prefix if provided, currently used for testing
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
                    local_packages,
                    python_version,
                    required_modules,
                    install_dependencies,
                    check_module_availability,
                )
                if real_environment_created:
                    store_testimony(logical_environment_name, real_environment_created)
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
    with open(testimony_file, "w") as f:
        f.write(real_environment_name)
    logger.info(f"Testimony file '{testimony_file}' stored successfully.")


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


def testimony_exists(environment_name) -> bool:
    testimony_file = os.path.join(
        DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER, environment_name
    )
    logger.info(f"Checking if testimony file '{testimony_file}' exists.")
    try:
        logger.info(
            f"Existing testimonies: {os.listdir(DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER)}"
        )
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
        testimony_exists(logical_environment_name)
        and real_environment_name in existing_virtual_environments
    ):
        logger.info(
            f"Python virtual environment {logical_environment_name} already exists "
            f"with the real name {real_environment_name}."
        )
        return real_environment_name
    # Ensure we are working with a clean slate for the environment
    delete_virtual_environment(
        logical_environment_name=logical_environment_name,
        real_environment_name=real_environment_name,
        log_error_on_fail=False,
    )
    # Install the required Python version
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
        "--python",
        python_version,
        os.path.join(DEFAULT_ENVIRONMENT_FOLDER, real_environment_name),
    ]
    logger.debug(f"Running command: {' '.join(command)}")
    with time_block:
        result = subprocess.run(
            " ".join(command),
            shell=True,
        )
        logger.debug(f"Result: {result}")
    if result.returncode != 0:
        logger.error(
            "Failed to create virtual environment with Python version"
            f" {python_version} and environment name {logical_environment_name}. Please"
            " check err.log in the same folder for more details."
        )
        return None
    else:
        logger.info(
            f"Python virtual environment {logical_environment_name} created"
            f" successfully. Time taken: {time_block.time_taken():.2f}s"
        )
    # Install the required Python packages
    pip_upgrade_command = add_python_target_and_join_commands(
        [
            UV_EXECUTABLE,
            "pip",
            "install",
            "--upgrade",
            "pip",
        ],
        real_environment_name,
    )
    logger.info(
        f"Upgrading pip version for the virtual environment {logical_environment_name}"
    )
    logger.debug(f"Running command: '{pip_upgrade_command}'")
    with time_block:
        result = subprocess.run(
            pip_upgrade_command,
            shell=True,
        )
        logger.info(f"Result: {result}")
        if result.returncode != 0:
            logger.error(
                "Failed to upgrade pip version for the virtual environment"
                f" {logical_environment_name}"
            )
            delete_virtual_environment(
                logical_environment_name=logical_environment_name,
                real_environment_name=real_environment_name,
            )
            return None
    logger.info(
        "pip upgraded successfully for the virtual environment "
        f"{logical_environment_name}. Time taken: {time_block.time_taken():.2f}s"
    )
    # Remove the packages that are not available on PyPi
    logger.info("Selecting all the packages that are available on PyPi")
    with time_block:
        required_modules = (
            found_requirements(required_modules, real_environment_name)
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
    if local_packages:
        result = install_local_packages(
            local_packages, logical_environment_name, real_environment_name
        )
        if not result:
            return None
    return real_environment_name


def add_python_target_and_join_commands(
    command: list[str], environment_name: str
) -> str:
    """Given the command that we want to execute, return it as a string with the
    python target option appended at the end of it"""
    if CURRENT_PLATFORM.is_windows():
        python_target_option = [
            "--python",
            os.path.join(
                DEFAULT_ENVIRONMENT_FOLDER, environment_name, "Scripts", "python.exe"
            ),
        ]
    else:
        python_target_option = [
            "--python",
            os.path.join(DEFAULT_ENVIRONMENT_FOLDER, environment_name, "bin", "python"),
        ]

    joint_command = " ".join(command + python_target_option)
    return joint_command


def install_requirements(
    requirements: list[str],
    install_dependencies: bool,
    logical_environment_name: str,
    real_environment_name: str,
) -> bool:
    """Install the required Python packages to the current environment.
    Returns true if the requirements are installed successfully, false otherwise."""

    pip_install_requirements_command = add_python_target_and_join_commands(
        [
            UV_EXECUTABLE,
            "pip",
            "install",
        ]
        + requirements,
        real_environment_name,
    )
    if not install_dependencies:
        logger.info("Installing the requirements without dependencies")
        pip_install_requirements_command += " --no-deps"
    else:
        logger.info("Installing the requirements with dependencies")
    logger.info(
        f"Installing the requirements {requirements} for the virtual environment"
        f" {logical_environment_name}"
    )
    logger.debug(f"Running command: '{pip_install_requirements_command}'")
    with time_block:
        result = subprocess.run(
            pip_install_requirements_command,
            shell=True,
        )
    if result.returncode != 0:
        logger.error(
            f"Failed to install the requirements {requirements} for the virtual"
            f" environment {logical_environment_name}"
        )
        delete_virtual_environment(
            logical_environment_name=logical_environment_name,
            real_environment_name=real_environment_name,
        )
        return False
    else:
        logger.info(
            f"Requirements {requirements} installed successfully for the virtual"
            f" environment {logical_environment_name}. Time taken:"
            f" {time_block.time_taken():.2f}s"
        )
        return True


def install_local_packages(
    local_packages: str, logical_environment_name: str, real_environment_name: str
) -> bool | None:
    logger.info("Installing local packages")
    with time_block:
        for package_number in os.listdir(local_packages):
            package_folder = os.path.join(local_packages, package_number)
            if os.path.isdir(package_folder):
                pip_install_requirements_command = add_python_target_and_join_commands(
                    [UV_EXECUTABLE, "pip", "install", package_folder],
                    real_environment_name,
                )
                logger.info(
                    f"Installing the local package in {package_folder} for the virtual "
                    f"environment {logical_environment_name}"
                )
                logger.debug(f"Running command: '{pip_install_requirements_command}'")
                result = subprocess.run(
                    pip_install_requirements_command,
                    shell=True,
                )
                if result.returncode != 0:
                    logger.error(
                        f"Failed to install the local package {package_folder} for the "
                        f"virtual environment {logical_environment_name}"
                    )
                    delete_virtual_environment(
                        logical_environment_name=logical_environment_name,
                        real_environment_name=real_environment_name,
                    )
                    return None
                else:
                    logger.info(
                        f"Local package {package_number} installed successfully for the"
                        f" virtual environment {logical_environment_name}"
                    )
    logger.info(
        "Local packages installed successfully. Time taken:"
        f" {time_block.time_taken():.2f}s"
    )
    return True


def install_python_version(python_version: str) -> None:
    logger.info(f"Installing Python version {python_version}")
    command = " ".join([UV_EXECUTABLE, "python", "install", "-v", "-n", python_version])
    logger.debug(f"Running command: {command}")
    result = subprocess.run(command, shell=True)
    # Check if the Python version is not found
    if result.returncode != 0:
        logger.error(f"Failed to install Python version '{python_version}'.")
        logger.error(f"ERROR: '{str(result.stderr)}'")
        logger.error("Please check the Python version number provided and try again.")
        return None
    else:
        logger.info(
            f"Python version {python_version} installed successfully or "
            "already existed."
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a Python environment with pyenv"
    )
    parser.add_argument(
        "--python-requirements-file", type=str, help="Path to Python requirements file"
    )
    parser.add_argument("--python-version", type=str, help="Python version to use")
    args = parser.parse_args()

    if not os.path.isfile(args.python_requirements_file):
        logger.error(
            f"Python requirements file {args.python_requirements_file} not found."
            " Please check the path and try again by providing it to the"
            " --python-requirements-file argument"
        )
        exit(1)
