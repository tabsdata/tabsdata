#
# Copyright 2024 Tabs Data Inc.
#

import json
import logging
import os
import pkgutil
import shutil
import sys
import tarfile
from enum import Enum
from pathlib import Path
from typing import Callable, List, Literal, LiteralString

import cloudpickle
import yaml

from tabsdata.exceptions import ErrorCode, RegistrationError
from tabsdata.plugin import DestinationPlugin, SourcePlugin
from tabsdata.tabsdatafunction import Input, Output, TabsdataFunction

# Importing like this to ensure backwards compatibility with Python 3.7 and prior
if sys.version_info >= (3, 8):
    from importlib import metadata as importlib_metadata
else:
    import importlib_metadata  # pragma: no cover

logger = logging.getLogger(__name__)

CODE_FOLDER = "original_code"
COMPRESSED_CONTEXT_FOLDER = "context.tar.gz"
CONFIG_ENTRY_POINT_FUNCTION_FILE_KEY = "functionFile"
CONFIG_ENTRY_POINT_KEY = "entryPoint"
CONFIG_FILE_NAME = "configuration.json"
CONFIG_INPUTS_KEY = "inputs"
CONFIG_OUTPUT_KEY = "output"
IGNORED_FOLDERS = (".venv", ".git", "__pycache__", "target")
LOCAL_PACKAGES_FOLDER = "local_packages"
PLUGINS_FOLDER = "plugins"

PYTHON_CHECK_MODULE_AVAILABILITY_KEY = "checkPackagesAvailability"
PYTHON_LOCAL_PACKAGES_KEY = "localPackages"
PYTHON_PUBLIC_PACKAGES_KEY = "publicPackages"
PYTHON_INSTALL_DEPENDENCIES_KEY = "installPackagesDependencies"
PYTHON_VERSION_KEY = "pythonVersion"
REQUIREMENTS_FILE_NAME = "requirements.yaml"


def create_configuration(function: TabsdataFunction, save_location: str):
    os.makedirs(save_location, exist_ok=True)
    configuration = dict()
    configuration[CONFIG_INPUTS_KEY] = create_input_configuration(
        function, save_location
    )
    configuration[CONFIG_ENTRY_POINT_KEY] = generate_entry_point_field(function)
    configuration[CONFIG_OUTPUT_KEY] = create_output_configuration(
        function, save_location
    )
    if save_location:
        with open(os.path.join(save_location, CONFIG_FILE_NAME), "w") as file:
            json.dump(configuration, file)
    return configuration


def create_output_configuration(function: TabsdataFunction, save_location: str) -> dict:
    # TODO: This will be removed when interaction with the backend is implemented
    #   https://tabsdata.atlassian.net/browse/TAB-36
    if isinstance(function.output, dict):
        return function.output

    return convert_to_dict_and_store_if_plugin(function.output, save_location)


def convert_to_dict_and_store_if_plugin(
    to_convert: SourcePlugin | DestinationPlugin | Input | Output, save_location: str
) -> dict:
    configuration_dict = to_convert.to_dict() if to_convert else {}
    if isinstance(to_convert, SourcePlugin) or isinstance(
        to_convert, DestinationPlugin
    ):
        plugins_location = os.path.join(save_location, PLUGINS_FOLDER)
        os.makedirs(plugins_location, exist_ok=True)
        with open(
            os.path.join(
                plugins_location,
                configuration_dict.get(to_convert.IDENTIFIER),
            ),
            "wb",
        ) as f:
            cloudpickle.dump(to_convert, f)
    return configuration_dict


def create_input_configuration(function: TabsdataFunction, save_location: str) -> dict:
    return convert_to_dict_and_store_if_plugin(function.input, save_location)


def generate_entry_point_field(function):
    return {
        CONFIG_ENTRY_POINT_FUNCTION_FILE_KEY: (
            f"{function.original_function.__name__}.pkl"
        ),
    }


def create_tarball(source_dir: str, output_filename: str):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.sep)


def create_requirements(
    save_location: str | os.PathLike, local_packages: List[str] | str | None = None
) -> List[str]:
    """Infers the requirements of the current environment and saves them to a YAML
    file. Furthermore, it saves the local packages to the save location if provided."""
    os.makedirs(save_location, exist_ok=True)
    requirements = obtain_ordered_dists()
    python_version = (
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )
    # Create a YAML file with the Python version and requirements
    # Since we are copying the entire environment, install dependencies is set to False
    data = {
        PYTHON_VERSION_KEY: python_version,
        PYTHON_PUBLIC_PACKAGES_KEY: requirements,
        PYTHON_INSTALL_DEPENDENCIES_KEY: False,
        PYTHON_CHECK_MODULE_AVAILABILITY_KEY: True,
    }

    if local_packages:
        if isinstance(local_packages, str):
            local_packages = [local_packages]
        elif not isinstance(local_packages, list):
            raise RegistrationError(ErrorCode.RE11, type(local_packages))
        bundle_local_packages(local_packages, save_location)
        data[PYTHON_LOCAL_PACKAGES_KEY] = local_packages

    yaml_output = yaml.dump(data, sort_keys=True)

    # Write the YAML file to disk
    with open(os.path.join(save_location, REQUIREMENTS_FILE_NAME), "w") as file:
        file.write(yaml_output)
    return requirements


def bundle_local_packages(local_packages, save_location):
    for count, package_path in enumerate(local_packages):
        if not os.path.isdir(package_path):
            raise RegistrationError(ErrorCode.RE6, package_path)
        else:
            store_folder_contents(
                package_path,
                os.path.join(save_location, LOCAL_PACKAGES_FOLDER, str(count)),
            )


def copy_and_verify_requirements_file(
    save_location: str | os.PathLike, requirements_file: str
) -> List[str]:
    try:
        with open(requirements_file, "r") as file:
            data = yaml.safe_load(file)
    except FileNotFoundError:
        raise RegistrationError(ErrorCode.RE7, requirements_file)
    if not data.get(PYTHON_VERSION_KEY):
        raise RegistrationError(
            ErrorCode.RE8, PYTHON_VERSION_KEY, requirements_file, data
        )
    try:
        requirements = sorted(
            list(set(data[PYTHON_PUBLIC_PACKAGES_KEY])), key=str.casefold
        )
    except KeyError:
        raise RegistrationError(
            ErrorCode.RE9, PYTHON_PUBLIC_PACKAGES_KEY, requirements_file, data
        )
    except TypeError:
        raise RegistrationError(
            ErrorCode.RE10,
            PYTHON_PUBLIC_PACKAGES_KEY,
            requirements_file,
            type(data[PYTHON_PUBLIC_PACKAGES_KEY]),
        )
    # Copy the requirements file to the save location
    os.makedirs(save_location, exist_ok=True)
    shutil.copy(requirements_file, os.path.join(save_location, REQUIREMENTS_FILE_NAME))
    # Copy the local packages to the save location
    if data.get(PYTHON_LOCAL_PACKAGES_KEY):
        bundle_local_packages(data.get(PYTHON_LOCAL_PACKAGES_KEY), save_location)
    return requirements


def obtain_ordered_dists() -> List[str]:
    available_modules = [module.name for module in pkgutil.iter_modules()]
    dists = []
    mapping = importlib_metadata.packages_distributions()
    real_modules = [
        mapping[module][0] for module in available_modules if module in mapping
    ]
    for module in real_modules:
        try:
            version = importlib_metadata.version(module)
            if version:
                dists.append(f"{module}=={version}")
        except importlib_metadata.PackageNotFoundError:  # pragma: no cover
            # Skip modules that do not have version information
            continue
    return sorted(
        list(set(dists)),
        key=str.casefold,
    )


def store_pickled_function(function, save_location):
    code_folder = os.path.join(save_location, CODE_FOLDER)
    os.makedirs(code_folder, exist_ok=True)
    with open(
        os.path.join(code_folder, f"{function.original_function.__name__}.pkl"), "wb"
    ) as f:
        cloudpickle.dump(function.original_function, f)


def store_file_contents(path_to_persist: str, save_location: str):
    os.makedirs(save_location, exist_ok=True)
    shutil.copy(
        path_to_persist,
        os.path.join(save_location, os.path.basename(path_to_persist)),
    )


def store_folder_contents(path_to_persist: str, save_location: str):
    os.makedirs(save_location, exist_ok=True)

    def ignorePath(path):
        # We ignore 2 kinds of folders: the original folder (to avoid infinite
        # recursion issues when the folder is stored inside itself), and folders with
        # names that begin with a . like .venv, since those should generally be ignored.
        # If facing issues regarding folders not being properly loaded, this might be
        # the place to look.
        def ignoref(directory, contents):
            result = [
                f
                for f in contents
                if os.path.abspath(os.path.join(directory, f)) == path
                or f in IGNORED_FOLDERS
            ]
            return result

        return ignoref

    os.makedirs(save_location, exist_ok=True)
    shutil.copytree(
        path_to_persist,
        save_location,
        ignore=ignorePath(save_location),
        dirs_exist_ok=True,
    )


def store_function_codebase(path_to_persist: str, save_location: str):
    code_folder = os.path.join(save_location, CODE_FOLDER)
    if os.path.isdir(path_to_persist):
        store_folder_contents(path_to_persist, code_folder)
    elif os.path.isfile(path_to_persist):
        store_file_contents(path_to_persist, code_folder)


class SaveTarget(Enum):
    FILE = "file"
    FOLDER = "folder"


def create_bundle_archive(
    function: TabsdataFunction | Callable,
    local_packages: List[str] | str | None = None,
    path_to_code: str | LiteralString = None,
    requirements: str = None,
    save_location: str | Path | None = None,
    save_target: Literal["file", "folder"] | None = None,
) -> str:
    """
    Register a function in the Tabsdata platform.

    Args:
        function (TabsdataFunction): The function to be registered.
        local_packages (List[str] | str | None): The local packages required by
            the function. If None, no local packages are required.
        path_to_code (str): The path to the code of the function. If None, the
            code will be inferred from the function itself.
        requirements (str): The path to the requirements file of the function.
            If it is not provided, the requirements will be inferred from the
            local system.
        save_location (str): The location where the context of the function will
            be stored. If None, the current working directory will be used.
        save_target ('file' | 'folder' | None): Whether to save only the 'file'
            where the function is defined or the whole 'folder'. If None,
            and path_to_code is None, 'folder' will be used.

    Returns:
        TabsetsHandle: The handle to the registered function.

    Raises:
        ValueError: If the input function is not a TabsDataFunction.
        ValueError: If path_to_code and save_target are used together.
        ValueError: If save_target is not one of the allowed values.
        ValueError: If save_location is not a valid folder path.
        ValueError: If path_to_persist is not a valid system path.
    """
    if not isinstance(function, TabsdataFunction):
        raise RegistrationError(ErrorCode.RE1)
    if not os.path.isdir(save_location):
        raise RegistrationError(ErrorCode.RE4, save_location)

    path_to_persist = _obtain_path_to_persist(function, path_to_code, save_target)

    # Keep track of where context of each function is stored
    uncompressed_context_location = os.path.join(
        save_location, f"{function.original_function.__name__}_context"
    )
    _delete_if_exists_and_create_directory(uncompressed_context_location)

    # Store the code of the function
    store_function_codebase(path_to_persist, uncompressed_context_location)
    store_pickled_function(function, uncompressed_context_location)

    # Create a requirements.yaml file with the dependencies and Python version
    if requirements:
        copy_and_verify_requirements_file(uncompressed_context_location, requirements)
    else:
        create_requirements(uncompressed_context_location, local_packages)

    # Create a configuration.json with the inputs and store all required files
    create_configuration(function, uncompressed_context_location)

    # Create a tarball with the context of the function
    compressed_context_location = os.path.join(
        save_location, f"{function.original_function.__name__}_compressed_context"
    )
    _delete_if_exists_and_create_directory(compressed_context_location)
    compressed_context_file = os.path.join(
        compressed_context_location,
        COMPRESSED_CONTEXT_FOLDER,
    )
    create_tarball(
        uncompressed_context_location,
        compressed_context_file,
    )
    return compressed_context_file


def _delete_if_exists_and_create_directory(directory: str):
    if os.path.isdir(directory):
        logger.warning(f"Deleting directory '{directory}' to store new context.")
        shutil.rmtree(directory, ignore_errors=True)
    os.makedirs(directory, exist_ok=True)


def _obtain_path_to_persist(function, path_to_code, save_target):
    """
    Obtain the path to the code to be persisted.

    Args:
        function (TabsdataFunction): The function to be registered.
        path_to_code (str): The path to the code of the function.
        save_target ('file' | 'folder' | None): Whether to save only the 'file'
            where the function is defined or the whole 'folder'. If None,
            and path_to_code is None, 'folder' will be used.
    """
    if path_to_code and save_target:
        raise RegistrationError(ErrorCode.RE2)
    elif not path_to_code and not save_target:
        save_target = SaveTarget.FOLDER.value

    if save_target:
        if save_target == SaveTarget.FOLDER.value:
            return function.original_folder
        elif save_target == SaveTarget.FILE.value:
            return os.path.join(function.original_folder, function.original_file)
        else:
            raise RegistrationError(
                ErrorCode.RE3, save_target, [element.value for element in SaveTarget]
            )
    else:
        if not os.path.exists(path_to_code):
            raise RegistrationError(ErrorCode.RE5, path_to_code)
        return path_to_code
