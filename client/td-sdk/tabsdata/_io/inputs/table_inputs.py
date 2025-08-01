#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List

from tabsdata._io.plugin import SourcePlugin
from tabsdata._tableuri import build_table_uri_object
from tabsdata._tabsserver.function.native_tables_utils import (
    scan_tf_from_table,
)
from tabsdata._tabsserver.function.yaml_parsing import (
    Table,
    TableVersions,
)
from tabsdata.exceptions import (
    ErrorCode,
    SourceConfigurationError,
)

if TYPE_CHECKING:
    from tabsdata._tabsserver.function.execution_context import ExecutionContext
    from tabsdata.tableframe.lazyframe.frame import TableFrame

logger = logging.getLogger(__name__)


class TableInput(SourcePlugin):
    """
    Class for managing the configuration of table-based data inputs.

    Attributes:
        table (str | List[str]): The table(s) to load.
    """

    def __init__(self, table: str | List[str]):
        """
        Initializes the TableInput with the given tables. If multiple tables are
            provided, they must be provided as a list.

        Args:
            table (str | List[str]): The table(s) to load.
                If multiple tables are provided, they must be provided as a list.
        """
        self.table = table

    @property
    def table(self) -> str | List[str]:
        """
        str | List[str]: The table(s) to load.
        """
        return self._table

    @table.setter
    def table(self, table: str | List[str]):
        """
        Sets the table(s) to load.

        Args:
            table (str | List[str]): The table(s) to load.
                If multiple tables are provided, they must be provided as a list
        """
        self._table = table
        if isinstance(table, list):
            assert [build_table_uri_object(single_uri) for single_uri in table]
            self._table = table
            self._table_list = self._table
        else:
            assert build_table_uri_object(table)
            self._table = table
            self._table_list = [self._table]
        self._verify_valid_table_list()

    def _verify_valid_table_list(self):
        """
        Verifies that the tables in the list are valid.
        """
        for table in self._table_list:
            uri = build_table_uri_object(table)
            if not uri.table:
                raise SourceConfigurationError(ErrorCode.SOCE25, table)

    @property
    def _stream_require_ec(self) -> bool:
        """
        Indicates whether the stream method requires an execution context.

        Returns:
            bool: True if the stream method requires an execution context,
            False otherwise.
        """
        return True

    def stream(
        self, working_dir: str
    ) -> list[TableFrame | None | list[TableFrame | None]]:
        logger.debug("Triggering TableInput")
        # When loading tabsdata tables, we return tuples of (uri, table), so that
        # coming operations can use information on the request for further processing.
        result = _execute_table_importer(self, self._ec)
        logger.info("Loaded tables successfully")
        return result

    def __repr__(self) -> str:
        """
        Returns a string representation of the input.

        Returns:
            str: A string representation of the input.
        """
        return f"{self.__class__.__name__}(table={self.table})"


def _execute_table_importer(
    source: TableInput,
    execution_context: ExecutionContext,
) -> list[TableFrame | None | list[TableFrame | None]]:
    # Right now, source provides very little information, but we use it to do a small
    # sanity check and to ensure that everything is running properly
    context_request_input = execution_context.request.input
    logger.info(
        f"Importing tables '{context_request_input}' and matching them"
        f" with source '{source}'"
    )
    tableframe_list: list[TableFrame | None | list[TableFrame | None]] = []
    # Note: source.uri is a list of URIs, it can't be a single URI because when we
    # serialised it we stored it as such even if it was a single one.
    source_tables = source.table if isinstance(source.table, list) else [source.table]
    logger.debug(f"Source tables: {source_tables}")
    if len(context_request_input) != len(source_tables):
        logger.error(
            "Number of tables in the execution context input"
            f" ({len(context_request_input)}) does not match the "
            "number of"
            f" URIs in the source ({len(source_tables)}). No data imported."
        )
        raise ValueError(
            "Number of tables in the execution context input"
            f" ({len(context_request_input)}) does not match the "
            "number of"
            f" URIs in the source ({len(source_tables)}). No data imported."
        )
    for execution_context_input_entry, source_table_str in zip(
        context_request_input, source_tables
    ):
        if isinstance(execution_context_input_entry, Table):
            _verify_source_tables_match(execution_context_input_entry, source_table_str)
            tf = scan_tf_from_table(
                execution_context,
                execution_context_input_entry,
                fail_on_none_uri=False,
            )
            tableframe_list.append(tf)
        elif isinstance(execution_context_input_entry, TableVersions):
            logger.debug(
                f"Matching TableVersions '{execution_context_input_entry}' with source"
                f" '{source_table_str}'"
            )
            list_of_table_objects = execution_context_input_entry.list_of_table_objects
            versioned_tableframes_list: list[TableFrame | None] = []
            for table in list_of_table_objects:
                _verify_source_tables_match(table, source_table_str)
                tf = scan_tf_from_table(
                    execution_context,
                    table,
                    fail_on_none_uri=False,
                )
                versioned_tableframes_list.append(tf)
            tableframe_list.append(versioned_tableframes_list)
        else:
            logger.error(
                f"Invalid table type: {type(execution_context_input_entry)}. No data"
                " imported."
            )
            raise TypeError(
                f"Invalid table type: {type(execution_context_input_entry)}. No data"
                " imported."
            )
    logger.debug(f"TableFrame list obtained: {tableframe_list}")
    return tableframe_list


def _verify_source_tables_match(execution_context_table: Table, source_table_str: str):
    # For now, we do only this small check for the table name, but we could
    # add more checks in the future.
    logger.debug(
        f"Matching table '{execution_context_table}' with source '{source_table_str}'"
    )
    source_table_uri = build_table_uri_object(source_table_str)
    if execution_context_table.name != source_table_uri.table:
        logger.debug(
            f"Source table '{source_table_str}' converted to TableURI:"
            f" '{source_table_uri}'"
        )
        logger.warning(
            f"Execution context table name '{execution_context_table.name}' does not "
            f"match the source table name '{source_table_uri.table}'"
        )
    return
