#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import argparse
import logging
import os
import traceback

from tabsdata.tabsserver.function.execution_context import (
    ExecutionContext,
    ExecutionPaths,
)
from tabsdata.tabsserver.function.execution_utils import execute_function_from_config
from tabsdata.tabsserver.function.global_utils import ABSOLUTE_LOCATION, setup_logging
from tabsdata.tabsserver.function.logging_utils import pad_string
from tabsdata.tabsserver.function.response_utils import create_response
from tabsdata.tabsserver.function.results_collection import ResultsCollection
from tabsdata.tabsserver.function.store_results_utils import store_results

logger = logging.getLogger(__name__)


def execute_bundled_function(
    execution_context: ExecutionContext,
) -> ResultsCollection:
    # Execute the function and obtain a ResultsCollection
    results = execute_function_from_config(execution_context)
    modified_tables = store_results(
        execution_context,
        results,
    )
    execution_context.store_initial_values()
    create_response(
        execution_context,
        modified_tables,
    )
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Execute a python function from a file path"
    )
    parser.add_argument("--bundle-path", type=str, help="Path of the function bundle")
    parser.add_argument(
        "--request-file",
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
    arguments = parser.parse_args()
    setup_logging(
        default_path=os.path.join(ABSOLUTE_LOCATION, "function_logging.yaml"),
        logs_folder=arguments.logs_folder,
    )
    try:
        execution_fs = ExecutionPaths(
            bundle_folder=arguments.bundle_path,
            request_file=arguments.request_file,
            response_folder=arguments.response_folder,
            output_folder=arguments.output_folder,
        )
        execution_context = ExecutionContext(paths=execution_fs)
        execute_bundled_function(execution_context)
        logger.info(pad_string("[Exiting function execution]"))
        logger.info("Function executed successfully. Exiting.")
    except Exception as e:
        logger.info(pad_string("[Exiting function execution]"))
        logger.error(f"Error executing the function: {e}")
        logger.error(traceback.format_exc())
        raise e
