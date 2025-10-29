#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from tabsdata._io.plugin import DestinationPlugin
from tabsdata._tabsserver.function.native_tables_utils import sink_lf_to_location
from tabsdata._tabsserver.function.store_results_utils import (
    get_table_meta_info_from_lf,
)
from tabsdata._tabsserver.function.yaml_parsing import Table
from tabsdata.exceptions import (
    DestinationConfigurationError,
    ErrorCode,
)
from tabsdata.tableframe.lazyframe.frame import TableFrame

if TYPE_CHECKING:
    from tabsdata._tabsserver.function.execution_context import ExecutionContext
    from tabsdata._tabsserver.function.results_collection import ResultsCollection

logger = logging.getLogger(__name__)


class TableOutput(DestinationPlugin):
    """
    Class for managing the configuration of table-based data outputs.

    Attributes:
        table (str | list[str]): The table(s) to create. If multiple tables are
            provided, they must be provided as a list.
    """

    def __init__(self, table: str | list[str]):
        """
        Initializes the TableOutput with the given table(s) to create.

        Args:
            table (str | list[str]): The table(s) to create. If multiple tables are
                provided, they must be provided as a list.
        """
        self.table = table

    @property
    def table(self) -> str | list[str]:
        """
        str | list[str]: The table(s) to create. If multiple tables are provided,
            they must be provided as a list.
        """
        return self._table

    @table.setter
    def table(self, table: str | list[str]):
        """
        Sets the table(s) to create.

        Args:
            table (str | list[str]): The table(s) to create. If multiple tables are
                provided, they must be provided as a list.
        """
        self._table = table
        self._table_list = table if isinstance(table, list) else [table]
        for single_table in self._table_list:
            if not isinstance(single_table, str):
                raise DestinationConfigurationError(
                    ErrorCode.DECE10, single_table, type(single_table)
                )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.table})"

    def _run(self, execution_context: ExecutionContext, results: ResultsCollection):
        self._tabsdata_internal_logger = execution_context.logger
        logger = self._tabsdata_internal_logger
        logger.info(f"Exporting files with plugin '{self}'")
        logger.debug("Processing results of the user-provided function")
        modified_tables = _store_results_in_table(results, self, execution_context)
        logger.info(f"Exported files with plugin {self} successfully")
        execution_context.status.modified_tables = modified_tables


# noinspection PyProtectedMember
def _store_results_in_table(
    results: ResultsCollection,
    destination: TableOutput,
    execution_context: ExecutionContext,
) -> list[dict]:
    results.normalize_frame()
    # Right now, source provides very little information, but we use it to do a small
    # sanity check and to ensure that everything is running properly
    # TODO: Decide if we want to add more checks here
    request_output_entry_list = execution_context.request.output
    logger.info(
        f"Storing results in tables '{request_output_entry_list}' and "
        f"matching them with destination '{destination}'"
    )
    table_list = []
    destination_tables = (
        destination.table
        if isinstance(destination.table, list)
        else [destination.table]
    )
    # Note: destination.table is a list of strings, it can't be a single string because
    # when we serialised it we stored it as such even if it was a single one.
    if len(request_output_entry_list) != len(destination_tables):
        logger.error(
            "Number of tables in the execution context output"
            f" ({len(request_output_entry_list)}: "
            f"{request_output_entry_list}) does not match the "
            "number"
            f" of tables in the destination ({len(destination_tables)}: "
            f"{destination_tables}). No data stored."
        )
        raise ValueError(
            "Number of tables in the execution context output"
            f" ({len(request_output_entry_list)}: "
            f"{request_output_entry_list}) does not match the "
            "number"
            f" of tables in the destination ({len(destination_tables)}: "
            f"{destination_tables}). No data stored."
        )
    for request_output_entry, table_name_in_decorator in zip(
        request_output_entry_list, destination_tables
    ):
        if isinstance(request_output_entry, Table):
            _match_tables_and_verify(request_output_entry, table_name_in_decorator)
            table_list.append(request_output_entry)
        else:
            logger.error(
                f"Invalid table type: {type(request_output_entry)}. No data stored."
            )
            raise TypeError(
                f"Invalid table type: {type(request_output_entry)}. No data stored."
            )
    logger.debug(f"Table list obtained: {table_list}")
    logger.debug(f"Obtained a total of {len(results)} results")
    if len(results) != len(table_list):
        logger.error(
            f"Number of results obtained ({len(results)}) does not match the number of "
            f"tables to store ({len(table_list)}). No data stored."
        )
        raise ValueError(
            f"Number of results obtained ({len(results)}) does not match the number of "
            f"tables to store ({len(table_list)}). No data stored."
        )
    modified_tables = []
    for result, table in zip(results, table_list):
        logger.info(f"Storing result in table '{table}'")
        if isinstance(result.value, TableFrame):
            # First we create a new TableFrame where system columns to be kept are kept,
            # and those requiring regeneration are regenerated with new to persist
            # values.
            result_value: TableFrame = result.value
            lf = result_value._to_lazy()
            tf = TableFrame.__build__(
                df=lf,
                mode="sys",
                idx=result_value._idx,
                properties=result_value._properties,
            )
            sink_lf_to_location(tf._to_lazy(), execution_context, table.location)
            table_meta_info = get_table_meta_info_from_lf(lf)
            table_info = {"name": table.name, "meta_info": table_meta_info}
            modified_tables.append(table_info)
            logger.debug(
                f"Result stored in table '{table}', added to modified_tables "
                f"list with information '{table_info}'"
            )
        elif result is None:
            logger.warning(f"Result is None. No data stored: '{table}'.")
        elif result.value is None:
            logger.warning(f"Result value is None. No data stored: '{table}'.")
        else:
            logger.error(
                f"Invalid result type: '{type(result.value)}'. No data stored."
            )
            raise TypeError(
                f"Invalid result type: '{type(result.value)}'. No data stored."
            )
    logger.info("Results stored in tables")
    logger.debug(f"Modified tables: {modified_tables}")
    return modified_tables


def _match_tables_and_verify(
    execution_context_table: Table, destination_table_name: str
):
    # For now, we do only this small check for the table name, but we could
    # add more checks in the future.
    logger.debug(
        f"Matching table '{execution_context_table}' with destination table"
        f" '{destination_table_name}'"
    )
    if execution_context_table.name != destination_table_name:
        logger.warning(
            f"Execution context table name '{execution_context_table.name}' does not "
            f"match the destination table name '{destination_table_name}'"
        )
