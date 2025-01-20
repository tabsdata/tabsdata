#
# Copyright 2024 Tabs Data Inc.
#

import argparse
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from tabsdata.utils.bundle_utils import REQUIREMENTS_FILE_NAME

# This is a workaround to be able to import the module from the parent directory
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    __package__ = "tabsserver"

import tabsserver.function_execution.execute_function_from_bundle_path
from tabsserver.function_execution.global_utils import CURRENT_PLATFORM, setup_logging
from tabsserver.function_execution.yaml_parsing import parse_request_yaml
from tabsserver.pyenv_creation import (
    DEFAULT_ENVIRONMENT_FOLDER,
    create_virtual_environment,
)
from tabsserver.utils import (
    ABSOLUTE_LOCATION,
    DEFAULT_DEVELOPMENT_LOCKS_LOCATION,
    TimeBlock,
    extract_tarfile_to_folder,
)

logger = logging.getLogger(__name__)
time_block = TimeBlock()

EXECUTION_CONTEXT_FILE_NAME = "request.yaml"
FUNCTION_EXECUTION_LOGS_FILE = "function_execution.log"
UNCOMPRESSED_FUNCTION_BUNDLE_FOLDER = "uncompressed_function_bundle"


def do(
    request_folder: str | Path,
    response_folder: str,
    output_folder: str,
    current_instance: str = None,
    bin_folder: str = None,
    environment_prefix: str = None,
    locks_folder: str = DEFAULT_DEVELOPMENT_LOCKS_LOCATION,
    logs_folder: str = None,
):
    execution_context_file = os.path.join(request_folder, EXECUTION_CONTEXT_FILE_NAME)
    setup_logging(os.path.join(ABSOLUTE_LOCATION, "logging.yaml"))
    if locks_folder == DEFAULT_DEVELOPMENT_LOCKS_LOCATION:
        logger.warning(
            f"Using the default locks folder {DEFAULT_DEVELOPMENT_LOCKS_LOCATION}."
            " This should not happen in a production environment. Use the "
            "--locks-folder parameter to specify the folder where the locks for "
            "the instance environments creation are stored."
        )
    execution_context_content = parse_request_yaml(execution_context_file)
    logger.debug(f"Request YAML content: {execution_context_content}")
    compressed_context_folder = execution_context_content.function_bundle_uri
    if bin_folder:
        function_bundle_folder = os.path.join(
            bin_folder, UNCOMPRESSED_FUNCTION_BUNDLE_FOLDER
        )
    else:
        temporary_directory = tempfile.TemporaryDirectory()
        if CURRENT_PLATFORM.is_windows():
            import win32api

            logger.debug(f"Short temp path: '{temporary_directory}")
            temporary_directory = win32api.GetLongPathName(temporary_directory.name)
            logger.debug(f"Long temp Path: {temporary_directory}")
        else:
            temporary_directory = temporary_directory.name
        function_bundle_folder = os.path.join(
            temporary_directory, UNCOMPRESSED_FUNCTION_BUNDLE_FOLDER
        )
    with time_block:
        extract_tarfile_to_folder(
            tarfile_uri=compressed_context_folder,
            destination_folder=function_bundle_folder,
        )
    logger.info(
        f"Extracted {compressed_context_folder} to {function_bundle_folder}. Time"
        f" taken: {time_block.time_taken():.2f}s"
    )

    logger.info(
        f"Creating the virtual environment for the context {function_bundle_folder}"
    )
    with time_block:
        python_environment = create_virtual_environment(
            requirements_description_file=os.path.join(
                function_bundle_folder, REQUIREMENTS_FILE_NAME
            ),
            current_instance=current_instance,
            locks_folder=locks_folder,
            environment_prefix=environment_prefix,
        )
    if not python_environment:
        logger.error(
            "Failed to create the virtual environment for the context"
            f" {function_bundle_folder}"
        )
        return None, 1
    else:
        logger.info(
            f"Created the virtual environment {python_environment} for the context"
            f" {function_bundle_folder}. Time taken: {time_block.time_taken():.2f}s"
        )
    if CURRENT_PLATFORM.is_windows():
        path_to_environment_bin = os.path.join(
            DEFAULT_ENVIRONMENT_FOLDER, python_environment, "Scripts", "python.exe"
        )
    else:
        path_to_environment_bin = os.path.join(
            DEFAULT_ENVIRONMENT_FOLDER, python_environment, "bin", "python3"
        )
    command_to_execute = [
        path_to_environment_bin,
        "-m",
        tabsserver.function_execution.execute_function_from_bundle_path.__name__,
        "--bundle-path",
        function_bundle_folder,
        "--execution-context-file",
        execution_context_file,
        "--response-folder",
        response_folder,
        "--output-folder",
        output_folder,
    ]
    if logs_folder:
        command_to_execute.extend(["--logs-folder", logs_folder])
    logger.info(
        "Executing the bundled function with command: " + " ".join(command_to_execute)
    )

    with time_block:
        result = subprocess.run(command_to_execute)
    logger.info(
        f"Result of executing the bundled function: {result}. Time taken:"
        f" {time_block.time_taken():.2f}s"
    )
    return python_environment, result.returncode


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Install a Python virtual environment and execute a function from a file."
        )
    )
    parser.add_argument(
        "--request-folder",
        type=str,
        help=(
            "Path of the folder where files with the parameters for the dataset "
            "execution are stored."
        ),
        required=True,
    )
    parser.add_argument(
        "--response-folder",
        type=str,
        help=(
            "Path of the folder where the output file named response.yaml with the "
            "relevant data of the function execution is stored."
        ),
        required=True,
    )
    parser.add_argument(
        "--current-instance",
        type=str,
        help="Instance name of the Tabsdata server where the code is being executed.",
        default=None,
    )
    parser.add_argument(
        "--bin-folder",
        type=str,
        help="Path of the folder where the decompressed bundle will be stored.",
        default=None,
    )
    parser.add_argument(
        "--environment_prefix",
        type=str,
        help=(
            "Prefix to add to the virtual environment linked to a dataset function."
            " Currently unused"
        ),
        default=None,
    )
    parser.add_argument(
        "--logs-folder",
        type=str,
        help="Path of the folder where the logs of the execution are stored.",
        default=None,
    )
    parser.add_argument(
        "--locks-folder",
        type=str,
        help=(
            "Path of the folder where the locks for the instance environments "
            "creation are stored."
        ),
        default=DEFAULT_DEVELOPMENT_LOCKS_LOCATION,
    )
    parser.add_argument(
        "--input-folder",
        type=str,
        help="Path of the input folder with input data",
        default=None,
    )
    parser.add_argument(
        "--output-folder",
        type=str,
        help=(
            "Path of the folder where the results of the function execution are stored."
        ),
        required=True,
    )

    args = parser.parse_args()
    environment_created, result = do(
        request_folder=args.request_folder,
        response_folder=args.response_folder,
        current_instance=args.current_instance,
        bin_folder=args.bin_folder,
        environment_prefix=None,
        locks_folder=args.locks_folder,
        logs_folder=args.logs_folder,
        output_folder=args.output_folder,
    )
    sys.exit(result)


if __name__ == "__main__":
    main()
