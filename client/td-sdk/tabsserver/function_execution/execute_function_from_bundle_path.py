#
# Copyright 2024 Tabs Data Inc.
#

import argparse
import logging
import os
import traceback

from tabsdata.utils.bundle_utils import CONFIG_OUTPUT_KEY

from . import (
    configuration_utils,
    execution_utils,
    initial_values_utils,
    response_utils,
    store_results_utils,
    yaml_parsing,
)
from .global_utils import ABSOLUTE_LOCATION, setup_logging
from .initial_values_utils import INITIAL_VALUES

logger = logging.getLogger(__name__)


def execute_bundled_function(
    bundle_folder: str,
    execution_context_file: str,
    response_folder: str,
    output_folder: str,
):
    configuration = configuration_utils.load_configuration(bundle_folder)
    execution_context = yaml_parsing.parse_request_yaml(execution_context_file)
    INITIAL_VALUES.load_current_initial_values(execution_context)
    results = execution_utils.execute_function_from_config(
        config=configuration,
        working_dir=bundle_folder,
        execution_context=execution_context,
    )
    modified_tables = []
    if configuration.get(CONFIG_OUTPUT_KEY):
        modified_tables = store_results_utils.store_results(
            results=results,
            output_configuration=configuration.get(CONFIG_OUTPUT_KEY),
            working_dir=bundle_folder,
            execution_context=execution_context,
            output_folder=output_folder,
        )
    new_initial_values = initial_values_utils.store_initial_values(execution_context)
    response_utils.create_response(
        modified_tables, response_folder, execution_context, new_initial_values
    )
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Execute a python function from a file path"
    )
    parser.add_argument("--bundle-path", type=str, help="Path of the function bundle")
    parser.add_argument(
        "--execution-context-file",
        type=str,
        help="Path of the file with the backend context for the execution.",
        required=True,
    )
    parser.add_argument(
        "--logs-folder",
        type=str,
        help="Path of the folder where the logs of the execution are stored.",
        default=None,
    )
    parser.add_argument(
        "--response-folder",
        type=str,
        help=(
            "Path of the folder where the response file with the "
            "relevant data of the function execution is stored."
        ),
        required=True,
    )
    parser.add_argument(
        "--output-folder",
        type=str,
        help=(
            "Path of the folder where the results of the function execution are stored."
        ),
        default=None,
    )
    args = parser.parse_args()
    setup_logging(
        default_path=os.path.join(ABSOLUTE_LOCATION, "function_execution_logging.yaml"),
        logs_folder=args.logs_folder,
    )
    try:
        execute_bundled_function(
            args.bundle_path,
            args.execution_context_file,
            response_folder=args.response_folder,
            output_folder=args.output_folder,
        )
        logger.info("Function executed successfully. Exiting.")
    except Exception as e:
        logger.error(f"Error executing the function: {e}")
        logger.error(traceback.format_exc())
        raise e
