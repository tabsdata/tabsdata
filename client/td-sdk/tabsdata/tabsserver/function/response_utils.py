#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List

from .initial_values_utils import INITIAL_VALUES_TABLE_NAME
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
    logger.info(f"Modified tables: {modified_tables}")
    not_modified_tables = [
        table
        for table in execution_context_output_tables
        if table not in modified_tables
    ]
    logger.info(f"Not modified tables: {not_modified_tables}")
    data_tables = [Data(table) for table in modified_tables]
    no_data_tables = [NoData(table) for table in not_modified_tables]
    if execution_context.initial_values.returns_values:
        data_tables.append(Data(INITIAL_VALUES_TABLE_NAME))
    else:
        no_data_tables.append(NoData(INITIAL_VALUES_TABLE_NAME))
    response_content = data_tables + no_data_tables
    response_file = execution_context.paths.response_file
    logger.debug(f"Response content: {response_content}")
    logger.debug(f"Response file: {response_file}")
    store_response_as_yaml(response_content, response_file)
