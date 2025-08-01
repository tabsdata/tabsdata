#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .yaml_parsing import Data, NoData, store_response_as_yaml

if TYPE_CHECKING:
    from tabsdata._tabsserver.function.execution_context import ExecutionContext

logger = logging.getLogger(__name__)

RESPONSE_FILE_NAME = "response.yaml"


def create_response(
    execution_context: ExecutionContext,
):
    modified_tables = execution_context.status.modified_tables
    request = execution_context.request
    execution_context_output_tables = [table.name for table in request.output]
    logger.info(f"Execution context output tables: {execution_context_output_tables}")
    try:
        modified_table_names: list[str] = [table["name"] for table in modified_tables]
    except KeyError as e:
        logger.error(
            f"Modified tables do not have 'name' key: {modified_tables}. "
            "This is most likely due to a bug in the 'modified_tables' list "
            "generation"
        )
        raise ValueError(
            f"Modified tables do not have 'name' key: {modified_tables}. "
            "This is most likely due to a bug in the 'modified_tables' list "
            "generation"
        ) from e
    not_modified_tables: list[str] = [
        table_name
        for table_name in execution_context_output_tables
        if table_name not in modified_table_names
    ]
    data_tables = []
    for table in modified_tables:
        try:
            table_name = table["name"]
            table_info = table["meta_info"]
            table_cols = table_info["column_count"]
            table_rows = table_info["row_count"]
            table_schema_hash = table_info["schema_hash"]
            data_tables.append(
                Data(
                    table_name,
                    column_count=table_cols,
                    row_count=table_rows,
                    schema_hash=table_schema_hash,
                )
            )
        except KeyError as e:
            logger.error(
                f"Modified table {table} does not have the expected keys. "
                "This is most likely due to a bug in the 'modified_tables' list "
                "generation"
            )
            raise ValueError(
                f"Modified table {table} does not have the expected keys. "
                "This is most likely due to a bug in the 'modified_tables' list "
                "generation"
            ) from e
    no_data_tables = [NoData(table) for table in not_modified_tables]
    initial_values_table_name = execution_context.status.offset.output_table_name
    initial_values_meta_info = execution_context.status.offset.meta_info

    if execution_context.status.offset.changed:
        try:
            data_tables.append(
                Data(
                    initial_values_table_name,
                    column_count=initial_values_meta_info["column_count"],
                    row_count=initial_values_meta_info["row_count"],
                    schema_hash=initial_values_meta_info["schema_hash"],
                )
            )
            modified_tables.append(
                {
                    "name": initial_values_table_name,
                    "meta_info": initial_values_meta_info,
                }
            )
        except KeyError as e:
            logger.error(
                f"Offset table '{initial_values_table_name}' does not have the "
                f"expected info in '{initial_values_meta_info}'. "
                "This is most likely due to a bug in the storage of the offset table"
            )
            raise ValueError(
                f"Offset table '{initial_values_table_name}' does not have the "
                f"expected info in '{initial_values_meta_info}'. "
                "This is most likely due to a bug in the storage of the offset table"
            ) from e
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
