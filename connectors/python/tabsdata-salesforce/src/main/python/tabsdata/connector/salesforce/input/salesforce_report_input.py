#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import Literal

from tabsdata._io.connections.named_connection.named_connection import NamedConn
from tabsdata._io.plugin import SourcePlugin
from tabsdata._utils.validation import _td_validate_call
from tabsdata.connector.salesforce._connection import SalesforceConn
from tabsdata.connector.salesforce._connector import (
    SalesforceReportSource,
)
from tabsdata.exceptions import ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class SalesforceReportSrc(SourcePlugin):
    """
    Source plugin for reading data from Salesforce reports.
    """

    @_td_validate_call(ErrorCode.SOCE102)
    def __init__(
        self,
        conn: SalesforceConn | NamedConn,
        report: str | list[str],
        column_name_strategy: Literal["columnName", "label"],
        find_report_by: Literal["id", "name"] | None = None,
        filter: tuple[str, str, str] | list[tuple[str, str, str]] | None = None,
        filter_logic: str | None = None,
        last_modified_column: str | None = None,
        initial_last_modified: str | None = None,
        api_version: str | None = None,
    ):
        """
        Initialize a SalesforceReportSrc instance.

        Args:
            conn: The Salesforce connection configuration.
            report: The report or list of reports to execute. Each entry must
                be either a report ID or a report name.
            column_name_strategy: Which column attribute to use as the column
                name in the output data. ``"columnName"`` uses the API name
                (e.g. ``"ACCOUNT.NAME"``), ``"label"`` uses the display label
                (e.g. ``"Account Name"``).
            find_report_by: Whether the ``report`` parameter contains report
                IDs or report names. If omitted, it is inferred from the
                value.
            filter: A single ``(field, operator, value)`` tuple or a list of
                such tuples to apply to the report.
            filter_logic: Optional expression combining filters by index
                (e.g. ``"(1 AND 2) OR 3"``). Requires ``filter`` to be set.
            last_modified_column: Column used for incremental loading.
                Must be paired with ``initial_last_modified``.
            initial_last_modified: Starting timestamp (with timezone) for
                incremental loading. Must be paired with
                ``last_modified_column``.
            api_version: Optional Salesforce API version.
        """
        # Stash all non-conn state first so the conn setter can rebuild the
        # inner SalesforceReportSource with a complete picture.
        self._report = report
        self._column_name_strategy = column_name_strategy
        self._find_report_by = find_report_by
        self._filter = filter
        self._filter_logic = filter_logic
        self._last_modified_column = last_modified_column
        self._initial_last_modified = initial_last_modified
        self._api_version = api_version
        self._salesforce_report_source: SalesforceReportSource | None = None
        # Assigning conn triggers the setter, which (re)builds the inner
        # SalesforceReportSource when conn is a resolved SalesforceConn.
        self.conn = conn

    def _build_source(self) -> SalesforceReportSource:
        kwargs = {}
        if self._api_version is not None:
            kwargs["api_version"] = self._api_version
        return SalesforceReportSource(
            credentials=self._conn.credentials,
            report=self._report,
            column_name_strategy=self._column_name_strategy,
            find_report_by=self._find_report_by,
            filter=self._filter,
            filter_logic=self._filter_logic,
            instance_url=self._conn.instance_url,
            last_modified_column=self._last_modified_column,
            initial_last_modified=self._initial_last_modified,
            **kwargs,
        )

    @property
    def conn(self) -> SalesforceConn | NamedConn:
        """
        The Salesforce connection configuration.
        """
        return self._conn

    @conn.setter
    @_td_validate_call(ErrorCode.SOCE102)
    def conn(self, conn: SalesforceConn | NamedConn):
        self._conn = conn
        # Rebuild the inner source so its credentials and instance_url
        # reflect the new conn. When conn is a NamedConn (not yet resolved
        # by the framework), skip rebuilding.
        if isinstance(conn, SalesforceConn):
            self._salesforce_report_source = self._build_source()
        else:
            self._salesforce_report_source = None

    # --- Passthrough properties with setters ---
    #
    # Each setter forwards to the inner SalesforceReportSource first (so its
    # validation runs and raises before we touch local state), then updates
    # the local fallback used when conn has not yet been resolved.

    @property
    def report(self) -> list[str]:
        """
        The report IDs or names to execute.
        """
        if self._salesforce_report_source is not None:
            return self._salesforce_report_source.report
        return [self._report] if isinstance(self._report, str) else self._report

    @report.setter
    def report(self, report: str | list[str]):
        if self._salesforce_report_source is not None:
            self._salesforce_report_source.report = report
        self._report = report

    @property
    def column_name_strategy(self) -> Literal["columnName", "label"]:
        """
        Which column attribute is used as the output column name.
        """
        if self._salesforce_report_source is not None:
            return self._salesforce_report_source.column_name_strategy
        return self._column_name_strategy

    @column_name_strategy.setter
    def column_name_strategy(
        self, column_name_strategy: Literal["columnName", "label"]
    ):
        if self._salesforce_report_source is not None:
            self._salesforce_report_source.column_name_strategy = column_name_strategy
        self._column_name_strategy = column_name_strategy

    @property
    def find_report_by(self) -> Literal["id", "name"] | None:
        """
        Whether the report parameter contains report IDs or names.
        """
        if self._salesforce_report_source is not None:
            return self._salesforce_report_source.find_report_by
        return self._find_report_by

    @find_report_by.setter
    def find_report_by(self, find_report_by: Literal["id", "name"] | None):
        if self._salesforce_report_source is not None:
            self._salesforce_report_source.find_report_by = find_report_by
        self._find_report_by = find_report_by

    @property
    def filter(
        self,
    ) -> list[tuple[str, str, str]] | None:
        """
        The filter(s) applied to the report.
        """
        if self._salesforce_report_source is not None:
            return self._salesforce_report_source.filter
        return self._filter

    @filter.setter
    def filter(
        self,
        filter: tuple[str, str, str] | list[tuple[str, str, str]] | None,
    ):
        if self._salesforce_report_source is not None:
            self._salesforce_report_source.filter = filter
        self._filter = filter

    @property
    def filter_logic(self) -> str | None:
        """
        The boolean expression combining the configured filters.
        """
        if self._salesforce_report_source is not None:
            return self._salesforce_report_source.filter_logic
        return self._filter_logic

    @filter_logic.setter
    def filter_logic(self, filter_logic: str | None):
        if self._salesforce_report_source is not None:
            self._salesforce_report_source.filter_logic = filter_logic
        self._filter_logic = filter_logic

    @property
    def last_modified_column(self) -> str | None:
        """
        Column used for incremental loading.
        """
        if self._salesforce_report_source is not None:
            return self._salesforce_report_source.last_modified_column
        return self._last_modified_column

    @last_modified_column.setter
    def last_modified_column(self, last_modified_column: str | None):
        if self._salesforce_report_source is not None:
            self._salesforce_report_source.last_modified_column = last_modified_column
        self._last_modified_column = last_modified_column

    @property
    def initial_last_modified(self) -> str | None:
        """
        Starting timestamp for incremental loading.
        """
        if self._salesforce_report_source is not None:
            return self._salesforce_report_source.initial_last_modified
        return self._initial_last_modified

    @initial_last_modified.setter
    def initial_last_modified(self, initial_last_modified: str | None):
        if self._salesforce_report_source is not None:
            self._salesforce_report_source.initial_last_modified = initial_last_modified
        self._initial_last_modified = initial_last_modified

    @property
    def api_version(self) -> str | None:
        """
        The Salesforce API version being used.
        """
        if self._salesforce_report_source is not None:
            return self._salesforce_report_source.api_version
        return self._api_version

    @api_version.setter
    def api_version(self, api_version: str | None):
        if self._salesforce_report_source is not None:
            self._salesforce_report_source.api_version = api_version
        self._api_version = api_version

    @property
    def initial_values(self) -> dict:
        """
        The initial values tracked across runs for incremental loading.
        """
        if self._salesforce_report_source is not None:
            return self._salesforce_report_source.initial_values
        return {}

    @initial_values.setter
    def initial_values(self, initial_values: dict | None):
        if self._salesforce_report_source is None:
            raise RuntimeError(
                "Cannot set initial_values before the conn has been resolved "
                "to a SalesforceConn."
            )
        self._salesforce_report_source.initial_values = initial_values or {}

    def chunk(self, working_dir: str) -> list[str]:
        """
        Execute the configured reports and return a list of parquet file
        paths (relative to ``working_dir``) with the results.
        """
        if self._salesforce_report_source is None:
            raise RuntimeError(
                "Cannot call chunk() before the conn has been resolved to a "
                "SalesforceConn."
            )
        return self._salesforce_report_source.chunk(working_dir)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"conn={self.conn}, "
            f"report={self.report}, "
            f"column_name_strategy={self.column_name_strategy!r}, "
            f"find_report_by={self.find_report_by!r}, "
            f"filter={self.filter}, "
            f"filter_logic={self.filter_logic!r}, "
            f"last_modified_column={self.last_modified_column!r}, "
            f"initial_last_modified={self.initial_last_modified!r}, "
            f"api_version={self.api_version!r})"
        )
