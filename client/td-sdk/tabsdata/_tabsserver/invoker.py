#
# Copyright 2024 Tabs Data Inc.
#

import argparse
import copy
import logging
import os
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

import tabsdata
from tabsdata._tabsserver.engine import EngineProvider
from tabsdata._tabsserver.function import execute_function_from_bundle_path
from tabsdata._tabsserver.function.global_utils import (
    DEFAULT_DEVELOPMENT_LOCKS_LOCATION,
    setup_logging,
)
from tabsdata._tabsserver.function.yaml_parsing import parse_request_yaml
from tabsdata._tabsserver.pyenv_creation import (
    create_virtual_environment,
    get_path_to_environment_bin,
)
from tabsdata._tabsserver.utils import (
    ABSOLUTE_LOCATION,
    TimeBlock,
    extract_bundle_folder,
)
from tabsdata._utils.bundle_utils import REQUIREMENTS_FILE_NAME

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._constants import PYTEST_CONTEXT_ACTIVE
from tabsdata._utils.temps import tabsdata_temp_folder

logger = logging.getLogger(__name__)
time_block = TimeBlock()

REQUEST_FILE_NAME = "request.yaml"
_ = execute_function_from_bundle_path


# We keep entries whose last path segment is 'egg' as they are development
# eggs that are not installed in the site-packages folder.
# ToDo...
@contextmanager
def clear_environment():
    original_sys_path = copy.deepcopy(sys.path)
    original_env = copy.deepcopy(os.environ)
    os.environ["PYTHONPATH"] = ""
    sys.path = []
    try:
        yield
    finally:
        sys.path = original_sys_path
        os.environ = original_env


# This is meant to be used when running pytest tests in development stages
# To avoid copying binaries to each local packages bundle, therefore alleviating
# their weight, we instead add this folder to the PATH when running pytest tests.
def find_binaries_root() -> str | None:
    # noinspection PyBroadException
    try:
        current = Path.cwd()
        for parent in [current, *current.parents]:
            if Path(os.path.join(parent, ".root")).is_file():
                return os.path.join(parent, "target", "debug")
        return None
    except Exception:
        return None


def invoke(
    request_folder: str | Path,
    response_folder: str,
    output_folder: str,
    current_instance: str = None,
    bin_folder: str = None,
    environment_prefix: str = None,
    locks_folder: str = DEFAULT_DEVELOPMENT_LOCKS_LOCATION,
    logs_folder: str = None,
    work: str = "0",
    # When running pytest tests, the current working directory is set relative to the
    # location of the test file.
    # This can "pollute" sys.path, leading to incorrect module resolution, especially
    # problematic with namespace packages (that we use).
    # In particular, it may cause Python to import the tabsdata module from the test
    # project directory instead of from local packages or the installed version.
    # To avoid this, when this parameter is set to True (typically during tests), a
    # temporary directory (considered a safe and neutral context) is used as the
    # working directory. Otherwise (in production or supervised execution), the current
    # directory is preserved, since the supervisor is responsible for setting it
    # correctly.
    # Note: This behaviour is further improved using parameter --import-mode=importlib
    #       when running pytest, which increases odds of having a proper sys path
    #       when running tests.
    #       (https://docs.pytest.org/en/stable/explanation/pythonpath.html)
    temp_cwd: bool = False,
):
    request_file_path = os.path.join(request_folder, REQUEST_FILE_NAME)
    setup_logging(os.path.join(ABSOLUTE_LOCATION, "logging.yaml"))
    if locks_folder == DEFAULT_DEVELOPMENT_LOCKS_LOCATION:
        logger.warning(
            f"Using the default locks folder {DEFAULT_DEVELOPMENT_LOCKS_LOCATION}."
            " This should not happen in a production environment. Use the "
            "--locks-folder parameter to specify the folder where the locks for "
            "the instance environments creation are stored."
        )
    request_content = parse_request_yaml(request_file_path)
    request_content.work = work
    logger.debug(f"Request YAML content: {request_content}")
    compressed_bundle_uri = request_content.function_bundle_uri

    uncompressed_bundle_folder = extract_bundle_folder(
        bin_folder, compressed_bundle_uri
    )

    logger.info(
        "Creating the virtual environment for the function in"
        f" {uncompressed_bundle_folder}"
    )
    with time_block:
        python_environment = create_virtual_environment(
            requirements_description_file=str(
                os.path.join(
                    uncompressed_bundle_folder,
                    REQUIREMENTS_FILE_NAME,
                )
            ),
            current_instance=current_instance,
            locks_folder=locks_folder,
            environment_prefix=environment_prefix,
            inject_current_tabsdata=True,
        )
    if not python_environment:
        logger.error(
            "Failed to create the virtual environment for the function in"
            f" {uncompressed_bundle_folder}"
        )
        return None, 1
    else:
        logger.info(
            f"Created the virtual environment {python_environment} for the function in"
            f" {uncompressed_bundle_folder}. Time taken: {time_block.time_taken():.2f}s"
        )

    python_bin_file = get_path_to_environment_bin(python_environment)
    python_bin_folder = os.path.dirname(python_bin_file)

    command_to_execute = [
        python_bin_file,
        "-m",
        tabsdata._tabsserver.function.execute_function_from_bundle_path.__name__,
        "--work",
        work,
        "--bundle-path",
        uncompressed_bundle_folder,
        "--request-file",
        request_file_path,
        "--response-folder",
        response_folder,
        "--output-folder",
        output_folder,
    ]
    env = os.environ.copy()

    # When running pytest tests, specially because tabsdata can be delivered as
    # a local package, we ensure the bin folder of the running virtual
    # environment is added to the PATH, so that the binaries coming from the
    # Rust build side and Python entry points side are properly found.
    if os.environ.get(PYTEST_CONTEXT_ACTIVE) is not None:
        env["PATH"] = env.get("PATH", "") + os.pathsep + python_bin_folder
        project_bin_folder = find_binaries_root()
        if project_bin_folder:
            env["PATH"] = env.get("PATH", "") + os.pathsep + project_bin_folder

    if logs_folder:
        command_to_execute.extend(["--logs-folder", logs_folder])
    logger.info(
        "Executing the bundled function with command: " + " ".join(command_to_execute)
    )
    with clear_environment():
        mount_options = sys.stdin.read()
        # See explanation on parameter temp_cwd
        if temp_cwd:
            with time_block:
                cwd = tempfile.mkdtemp(dir=tabsdata_temp_folder())
            logger.info(
                f"Using cwd for the running function: {cwd}. Time taken to "
                f"obtain cwd: {time_block.time_taken():.2f}s"
            )
        else:
            cwd = None
        with time_block:
            result = subprocess.run(
                command_to_execute,
                env=env,
                cwd=cwd,
                input=mount_options,
                text=True,
            )
        logger.info(
            f"Result of executing the bundled function: {result}. Time taken:"
            f" {time_block.time_taken():.2f}s"
        )

    return python_environment, result.returncode


def main():
    EngineProvider.instance(on_server=True)

    parser = argparse.ArgumentParser(
        description=(
            "Install a Python virtual environment and execute a function from a file."
        )
    )
    parser.add_argument(
        "--work",
        type=str,
        help="Counter of the work number for the same function run.",
        required=True,
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
    environment_created, result = invoke(
        work=args.work,
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
