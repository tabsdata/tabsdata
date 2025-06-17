#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List

from .yaml_parsing import Data, NoData, store_response_as_yaml

if TYPE_CHECKING:
    from tabsdata.tabsserver.function.execution_context import ExecutionContext

logger = logging.getLogger(__name__)

RESPONSE_FILE_NAME = "response.yaml"


def create_response(
    execution_context: ExecutionContext,
    modified_tables: List[str],
):
    request = execution_context.request
    execution_context_output_tables = [table.name for table in request.output]
    logger.info(f"Execution context output tables: {execution_context_output_tables}")
    not_modified_tables = [
        table
        for table in execution_context_output_tables
        if table not in modified_tables
    ]
    data_tables = [Data(table) for table in modified_tables]
    no_data_tables = [NoData(table) for table in not_modified_tables]
    initial_values_table_name = execution_context.status.offset.output_table_name

    if execution_context.status.offset.changed:
        data_tables.append(Data(initial_values_table_name))
        modified_tables.append(initial_values_table_name)
    else:
        no_data_tables.append(NoData(initial_values_table_name))
        not_modified_tables.append(initial_values_table_name)

    logger.info(f"Modified tables: {modified_tables}")
    logger.info(f"Not modified tables: {not_modified_tables}")
    response_content = data_tables + no_data_tables
    response_file = execution_context.paths.response_file
    logger.debug(f"Response content: {response_content}")
    logger.debug(f"Response file: {response_file}")
    store_response_as_yaml(response_content, response_file)
