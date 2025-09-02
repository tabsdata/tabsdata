#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import csv
import gzip
import logging
import os
import time
from typing import Literal

import ijson
import polars as pl
import requests

from tabsdata._io.plugin import SourcePlugin
from tabsdata._secret import DirectSecret, EnvironmentSecret, HashiCorpSecret, Secret

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


SF_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"


class SalesforceSource(SourcePlugin):

    LAST_MODIFIED_COLUMN = "SystemModstamp"
    LAST_MODIFIED_TOKEN = "$lastModified"

    def __init__(
        self,
        username: str | HashiCorpSecret | DirectSecret | EnvironmentSecret,
        password: str | HashiCorpSecret | DirectSecret | EnvironmentSecret,
        security_token: str | HashiCorpSecret | DirectSecret | EnvironmentSecret,
        query: str | list[str],
        instance_url: str = None,
        include_deleted: bool = False,
        initial_last_modified: str = None,
        api_version: str = None,
    ):
        """
        Initializes the SalesforceSource with the given query(s) and the credentials
            required to access Salesforce.

        Args:
            username (str | HashiCorpSecret | DirectSecret | EnvironmentSecret): The
                username to access Salesforce.
            password (str | HashiCorpSecret | DirectSecret | EnvironmentSecret): The
                password to access Salesforce.
            security_token (str | HashiCorpSecret | DirectSecret | EnvironmentSecret):
                The security token to access Salesforce.
            query (str | list[str]): The query or queries to execute in Salesforce.
                It can be a single string or a list of strings.
            instance_url (str, optional): The URL of the instance to which we want to
                connect. Only necessary when the username and password are associated
                to more than one instance. Defaults to None.
            include_deleted (bool, optional): Whether to include deleted records in the
                query results. Defaults to False.
            initial_last_modified (str, optional): The initial last modified date to use
                in the queries. This is useful when we want to query only the records
                that have been modified since a certain date. Defaults to None. If
                provided, it must be a string in Salesforce datetime format (with
                informed timezone) and the query must contain the token
                $lastModified, which will be replaced by the latest 'last_modified'
                value in each execution.
            api_version (str, optional): The Salesforce API version to use. Defaults to
                None, which will default to the latest version supported by
                simple_salesforce.

        """
        try:
            from dateutil import parser
            from simple_salesforce import Salesforce, api  # noqa: F401
        except ImportError:
            raise ImportError(
                "The 'tabsdata_salesforce' package is missing some dependencies. You "
                "can get them by installing 'tabsdata['salesforce']'"
            )
        self.username = username
        self.password = password
        self.security_token = security_token
        self.query = query
        self.include_deleted = include_deleted
        self.instance_url = instance_url
        if initial_last_modified is not None:
            datetime = parser.parse(initial_last_modified)
            if not datetime.tzinfo:
                raise ValueError(
                    "The 'initial_last_modified' parameter must have a timezone for "
                    "Salesforce queries to work properly"
                )
            self.initial_values = {
                "initial_last_modified": (
                    parser.parse(initial_last_modified).strftime(SF_DATE_FORMAT)
                )
            }
        else:
            self.initial_values = {}
        for query in self.query:
            if self.LAST_MODIFIED_TOKEN in query and not self.initial_values:
                raise ValueError(
                    f"Query '{query}' contains the token '{self.LAST_MODIFIED_TOKEN}' "
                    "but no 'initial_last_modified' was provided"
                )
        self.api_version = api_version or api.DEFAULT_API_VERSION

    @property
    def query(self) -> list[str]:
        return self._query

    @query.setter
    def query(self, query: str | list[str]):
        if isinstance(query, str):
            self._query = [query]
        elif isinstance(query, list):
            self._query = query
        else:
            raise TypeError(
                "The 'query' parameter must be either a string or a list of strings, "
                f"got '{type(query)}' instead"
            )

    @property
    def username(
        self,
    ) -> HashiCorpSecret | DirectSecret | EnvironmentSecret:
        return self._username

    @username.setter
    def username(
        self,
        username: str | HashiCorpSecret | DirectSecret | EnvironmentSecret,
    ):
        if isinstance(username, Secret):
            self._username = username
        elif isinstance(username, str):
            self._username = DirectSecret(username)
        else:
            raise TypeError(
                "The 'username' parameter must be either a string or a "
                f"tabsdata secret object, got '{type(username)}' instead"
            )

    @property
    def password(
        self,
    ) -> HashiCorpSecret | DirectSecret | EnvironmentSecret:
        return self._password

    @password.setter
    def password(
        self,
        password: str | HashiCorpSecret | DirectSecret | EnvironmentSecret,
    ):
        if isinstance(password, Secret):
            self._password = password
        elif isinstance(password, str):
            self._password = DirectSecret(password)
        else:
            raise TypeError(
                "The 'password' parameter must be either a string or a "
                f"tabsdata secret object, got '{type(password)}' instead"
            )

    @property
    def security_token(
        self,
    ) -> HashiCorpSecret | DirectSecret | EnvironmentSecret:
        return self._security_token

    @security_token.setter
    def security_token(
        self,
        security_token: str | HashiCorpSecret | DirectSecret | EnvironmentSecret,
    ):
        if isinstance(security_token, Secret):
            self._security_token = security_token
        elif isinstance(security_token, str):
            self._security_token = DirectSecret(security_token)
        else:
            raise TypeError(
                "The 'security_token' parameter must be either a string or a "
                f"tabsdata secret object, got '{type(security_token)}' instead"
            )

    def chunk(self, working_dir: str) -> list[str]:
        sf = _log_into_salesforce(self)
        resulting_files = []
        using_initial_values = bool(self.initial_values)
        for number, query in enumerate(self.query):
            logger.debug(f"Executing query number {number}: {query}")
            self._trigger_single_input(
                number, query, resulting_files, sf, working_dir, using_initial_values
            )
            logger.debug(f"Finished query number {number}")
        if using_initial_values:
            self.initial_values.pop("initial_last_modified", None)
        logger.debug(f"Destination files: {resulting_files}")
        return resulting_files

    def _trigger_single_input(
        self, number, query, resulting_files, sf, working_dir, using_initial_values
    ):
        # If using initial values logic, replace the token with the value
        if using_initial_values:
            max_date, query = self._replace_values_in_query(number, query)
        else:
            max_date = None
        destination_file = f"{number}.parquet"
        destination_path = os.path.join(working_dir, destination_file)
        res = sf.query_all_iter(query, include_deleted=self.include_deleted)
        origin_file = os.path.join(working_dir, f"{number}.csv")
        with open(origin_file, "w", newline="") as f:
            writer = None
            for rec in res:
                rec.pop("attributes", None)
                if using_initial_values:
                    current_last_modified = rec.get(self.LAST_MODIFIED_COLUMN)
                    if current_last_modified:
                        max_date = _maximum_date(current_last_modified, max_date)
                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=rec.keys())
                    writer.writeheader()
                writer.writerow(rec)
        try:
            pl.scan_csv(origin_file, raise_if_empty=True).sink_parquet(destination_path)
            resulting_files.append(destination_file)
        except pl.exceptions.NoDataError:
            logger.warning(f"No data to write in {destination_file}")
            resulting_files.append(None)
        logger.info(f"Query number {number} finished")
        if using_initial_values:
            self.initial_values[f"initial_last_modified_{number}"] = max_date

    def _replace_values_in_query(self, number, query):
        if self.initial_values.get(f"initial_last_modified_{number}"):
            query = self._replace_last_modified_token(
                query,
                self.initial_values.get(f"initial_last_modified_{number}"),
            )
            max_date = self.initial_values.get(f"initial_last_modified_{number}")
        elif self.initial_values.get("initial_last_modified"):
            query = self._replace_last_modified_token(
                query, self.initial_values.get("initial_last_modified")
            )
            max_date = self.initial_values.get("initial_last_modified")
        else:
            raise ValueError(
                f"Missing initial last modified value for query number {number}"
            )
        logger.info(f"Using new last modified date: {max_date}")
        logger.info(f"Query with replaced token: {query}")
        return max_date, query

    def _replace_last_modified_token(self, query: str, new_value: str):
        return query.replace(self.LAST_MODIFIED_TOKEN, new_value)


def _maximum_date(date1: str, date2: str):

    from dateutil import parser

    dates = [parser.parse(date) for date in [date1, date2]]
    result = max(dates).strftime(SF_DATE_FORMAT)
    return result


class SalesforceReportSource(SourcePlugin):
    def __init__(
        self,
        username: str | HashiCorpSecret | DirectSecret | EnvironmentSecret,
        password: str | HashiCorpSecret | DirectSecret | EnvironmentSecret,
        security_token: str | HashiCorpSecret | DirectSecret | EnvironmentSecret,
        report: str | list[str],
        column_by: Literal["columnName", "label"],
        report_identifier: Literal["id", "name"] = None,
        filter: tuple[str, str, str] | list[tuple[str, str, str]] = None,
        filter_logic: str = None,
        instance_url: str = None,
        api_version: str = None,
        maximum_wait_time: int = 600,
        poll_interval: int = 5,
        chunk_size: int = 50000,
        last_modified_column: str = None,
        initial_last_modified: str = None,
        **kwargs,
    ):
        """
        Initializes the SalesforceReportSource with the report information and the
            credentials required to access Salesforce.

        Args:
            username (str | HashiCorpSecret | DirectSecret | EnvironmentSecret): The
                username to access Salesforce.
            password (str | HashiCorpSecret | DirectSecret | EnvironmentSecret): The
                password to access Salesforce.
            security_token (str | HashiCorpSecret | DirectSecret | EnvironmentSecret):
                The security token to access Salesforce.
            report (str | list[str]): The report or reports to execute in Salesforce.
                It can be a single string or a list of strings. The string must be
                either the report ID or the name of the report.
            column_by (Literal["columnName", "label"]): Indicates which column
                attribute to use as the
                column name in the output data. It can be one of the following:
                - "columnName": The API name of the column (e.g., "ACCOUNT.NAME").
                - "label": The label of the column (e.g., "Account Name").
            report_identifier (Literal["id", "name"], optional): Indicates whether the
                'report' parameter contains report IDs or report names. If not
                provided, it will be inferred from the value of the 'report' parameter.
            filter (tuple[str, str, str] | list[tuple[str, str, str]], optional):
                A filter or list of filters to apply to the report. Each filter is a
                tuple of three strings: (field, operator, value). For example:
                [("CreatedDate", "greaterThan", "2023-01-01T00:00:00.000Z")].
                Defaults to None.
            filter_logic (str, optional): A string representing the logic to apply to
                the filters. For example: "(1 AND 2) OR 3". The numbers correspond to
                the position of the filters in the 'filter' list (starting at 1).
                Defaults to None.
            instance_url (str, optional): The URL of the instance to which we want to
                connect. Only necessary when the username and password are associated
                to more than one instance. Defaults to None.
            api_version (str, optional): The Salesforce API version to use. Defaults to
                None, which will default to the latest version supported by
                simple_salesforce.
            maximum_wait_time (int, optional): The maximum time to wait for a report
                to be generated, in seconds. Defaults to 600 seconds (10 minutes).
            poll_interval (int, optional): The interval between each poll to check if
                the report is ready, in seconds. Defaults to 5 seconds.
            chunk_size (int, optional): The number of rows to process in each chunk
                when reading the report data. Defaults to 50000 rows.
            last_modified_column (str, optional): The name of the column to use for
                incremental loading based on the last modified date. If provided,
                the report must contain this column. Defaults to None.
            initial_last_modified (str, optional): The initial last modified date to
                use for incremental loading. This is useful when we want to load only
                the records that have been modified since a certain date. Defaults to
                None. If provided, it must be a string in Salesforce datetime format
                (with informed timezone).
        """
        try:
            from dateutil import parser  # noqa: F401
            from simple_salesforce import Salesforce, api  # noqa: F401
        except ImportError:
            raise ImportError(
                "The 'tabsdata_salesforce' package is missing some dependencies. You "
                "can get them by installing 'tabsdata['salesforce']'"
            )
        self.username = username
        self.password = password
        self.security_token = security_token
        self.instance_url = instance_url
        self.api_version = api_version or api.DEFAULT_API_VERSION
        self.report = report
        self.column_by = column_by
        self.report_identifier = report_identifier
        self.filter = filter
        self.filter_logic = filter_logic
        self.maximum_wait_time = int(maximum_wait_time)
        self.poll_interval = int(poll_interval)
        self.chunk_size = int(chunk_size)
        self.last_modified_column = last_modified_column
        self.initial_last_modified = initial_last_modified
        self.kwargs = kwargs
        self._support_restful_options = kwargs.get("support_restful_options", {})
        self._support_to_parquet = kwargs.get("support_to_parquet", {})
        self._support_report_instance_body = kwargs.get(
            "support_report_instance_body", {}
        )
        self._support_default_value_field = kwargs.get(
            "support_default_value_field", None
        )
        self._support_type_to_pandas = kwargs.get("support_type_to_pandas", None)
        self._support_type_to_value_field = kwargs.get(
            "support_type_to_value_field", None
        )

        if self.last_modified_column and not self.initial_last_modified:
            raise ValueError(
                "The 'initial_last_modified' parameter must be provided if the "
                "'last_modified_column' parameter is provided."
            )
        elif not self.last_modified_column and self.initial_last_modified:
            raise ValueError(
                "The 'last_modified_column' parameter must be provided if the "
                "'initial_last_modified' parameter is provided."
            )

        if initial_last_modified is not None:
            datetime = parser.parse(initial_last_modified)
            if not datetime.tzinfo:
                raise ValueError(
                    "The 'initial_last_modified' parameter must have a timezone for "
                    "Salesforce queries to work properly"
                )
            self.initial_values = {
                "initial_last_modified": (
                    parser.parse(initial_last_modified).strftime(SF_DATE_FORMAT)
                )
            }
        else:
            self.initial_values = {}

    @property
    def column_by(
        self,
    ) -> Literal["columnName", "label"]:
        return self._column_by

    @column_by.setter
    def column_by(
        self,
        column_by: Literal["columnName", "label"],
    ):
        valid_column_by = [
            SFR_COLUMN_NAME_KEY,
            SFR_LABEL_KEY,
        ]
        if column_by not in valid_column_by:
            raise ValueError(
                "The 'column_by' parameter must be one of"
                f" {', '.join(valid_column_by)}, got '{column_by}' instead"
            )
        else:
            self._column_by = column_by

    @property
    def report_identifier(self) -> Literal["id", "name"]:
        if not self._report_identifier:
            import re

            try:
                first_report = self.report[0]
                return "id" if re.match(SFR_ID_PATTERN, first_report) else "name"
            except IndexError:
                raise ValueError(
                    "The 'report' parameter is empty, so the "
                    "report_identifier cannot be inferred. Please provide a value for "
                    "'report' or 'report_identifier'."
                )
        return self._report_identifier

    @report_identifier.setter
    def report_identifier(self, report_identifier: Literal["id", "name"] | None):
        if report_identifier is None:
            self._report_identifier = None
        elif report_identifier not in ["id", "name"]:
            raise ValueError(
                "The 'report_identifier' parameter must be either 'id' or 'name', "
                f"got '{report_identifier}' instead"
            )
        else:
            self._report_identifier = report_identifier

    @property
    def username(
        self,
    ) -> HashiCorpSecret | DirectSecret | EnvironmentSecret:
        return self._username

    @username.setter
    def username(
        self,
        username: str | HashiCorpSecret | DirectSecret | EnvironmentSecret,
    ):
        if isinstance(username, Secret):
            self._username = username
        elif isinstance(username, str):
            self._username = DirectSecret(username)
        else:
            raise TypeError(
                "The 'username' parameter must be either a string or a "
                f"tabsdata secret object, got '{type(username)}' instead"
            )

    @property
    def password(
        self,
    ) -> HashiCorpSecret | DirectSecret | EnvironmentSecret:
        return self._password

    @password.setter
    def password(
        self,
        password: str | HashiCorpSecret | DirectSecret | EnvironmentSecret,
    ):
        if isinstance(password, Secret):
            self._password = password
        elif isinstance(password, str):
            self._password = DirectSecret(password)
        else:
            raise TypeError(
                "The 'password' parameter must be either a string or a "
                f"tabsdata secret object, got '{type(password)}' instead"
            )

    @property
    def security_token(
        self,
    ) -> HashiCorpSecret | DirectSecret | EnvironmentSecret:
        return self._security_token

    @security_token.setter
    def security_token(
        self,
        security_token: str | HashiCorpSecret | DirectSecret | EnvironmentSecret,
    ):
        if isinstance(security_token, Secret):
            self._security_token = security_token
        elif isinstance(security_token, str):
            self._security_token = DirectSecret(security_token)
        else:
            raise TypeError(
                "The 'security_token' parameter must be either a string or a "
                f"tabsdata secret object, got '{type(security_token)}' instead"
            )

    @property
    def report(self) -> list[str]:
        return self._report

    @report.setter
    def report(self, report: str | list[str]):
        if isinstance(report, str):
            self._report = [report]
        elif isinstance(report, list):
            self._report = report
        else:
            raise TypeError(
                "The 'report' parameter must be either a string or a list of strings, "
                f"got '{type(report)}' instead"
            )

    @property
    def filter(self) -> list[tuple[str, str, str]] | None:
        return self._filter

    @filter.setter
    def filter(self, filter: tuple[str, str, str] | list[tuple[str, str, str]]):
        if not filter:
            self._filter = filter
        elif isinstance(filter, tuple):
            if not (len(filter) == 3 and all(isinstance(i, str) for i in filter)):
                raise ValueError(
                    "The 'filter' parameter must be a tuple of three strings "
                    f"(field, operator, value) or a list of tuples, got '{filter}' "
                    "instead, which is a tuple but does not have three string "
                    "elements."
                )
            self._filter = [filter]
        elif isinstance(filter, list):
            for item in filter:
                if not (
                    isinstance(item, tuple)
                    and len(item) == 3
                    and all(isinstance(i, str) for i in item)
                ):
                    raise ValueError(
                        "The 'filter' parameter must be a tuple of three strings "
                        "(field, operator, value) or a list of tuples, got a list "
                        f"with element '{item}' instead."
                    )
            self._filter = filter
        else:
            raise ValueError(
                "The 'filter' parameter must be a tuple of three strings "
                f"(field, operator, value) or a list of tuples, got '{filter}' "
                "instead"
            )

    @property
    def filter_logic(self) -> str | None:
        if self._filter_logic is None and self.filter is not None:
            number_of_filters = len(self.filter)
            filter_logic = " AND ".join(str(i) for i in range(1, number_of_filters + 1))
            filter_logic = f"({filter_logic})"
            return filter_logic
        return self._filter_logic

    @filter_logic.setter
    def filter_logic(self, filter_logic: str | None):
        if filter_logic is not None and not isinstance(filter_logic, str):
            raise TypeError(
                "The 'filter_logic' parameter must be either a string or None, "
                f"got '{type(filter_logic)}' instead"
            )
        if filter_logic and not self.filter:
            raise ValueError(
                "The 'filter_logic' parameter cannot be set if no filters are "
                "provided in the 'filter' parameter. Please make sure to provide "
                "filters"
            )
        self._filter_logic = filter_logic

    def chunk(self, working_dir: str) -> list[str]:
        sf = _log_into_salesforce(self)
        resulting_files = []
        for number, report in enumerate(self.report):
            logger.debug(f"Obtaining report number {number}: {report}")
            self._trigger_single_input(number, report, resulting_files, sf, working_dir)
            logger.debug(f"Obtained report number {number}")
        if self.initial_values:
            self.initial_values.pop("initial_last_modified", None)
        logger.debug(f"Destination files: {resulting_files}")
        return resulting_files

    def _obtain_report_id(self, sf, report) -> str:
        if self._report_identifier is None:
            # We will try both name and ID (if it is a valid ID) and see if any of
            # them returns a single value.
            report_id_from_id = None
            if self.report_identifier == "id":
                try:
                    report_id_from_id = _obtain_report_id_and_verify_format(
                        sf, report, "id"
                    )
                except Exception:
                    report_id_from_id = None
            try:
                report_id_from_name = _obtain_report_id_and_verify_format(
                    sf, report, "name"
                )
            except Exception:
                report_id_from_name = None
            if report_id_from_id and report_id_from_name:
                logger.error(
                    f"Report '{report}' was found using both 'id' and 'name' as "
                    "identifier. Please provide the 'report_identifier' parameter to "
                    "disambiguate."
                )
                raise Exception(
                    f"Report '{report}' was found using both 'id' and 'name' as "
                    "identifier. Please provide the 'report_identifier' parameter to "
                    "disambiguate."
                )
            report_id = report_id_from_id or report_id_from_name
        else:
            # User provided report_identifier, use the one that he provided.
            report_id = _obtain_report_id_and_verify_format(
                sf, report, self.report_identifier
            )
        if not report_id:
            logger.error(
                f"Report '{report}' with identifier type "
                f"'{self.report_identifier}' not found. Please ensure the ID "
                "or name is correct, and that the identifier type is set "
                "appropriately."
            )
            raise Exception(
                f"Report '{report}' with identifier type "
                f"'{self.report_identifier}' not found. Please ensure the ID "
                "or name is correct, and that the identifier type is set "
                "appropriately."
            )
        return report_id

    def _obtain_max_date_and_filter(self, reverse_lookup_dict, number):
        if self.initial_values:
            column_name = reverse_lookup_dict[self.column_by].get(
                self.last_modified_column
            )
            max_date, new_filter = self._obtain_new_filter_for_offset(
                number, column_name
            )
        else:
            max_date = None
            new_filter = None
        return max_date, new_filter

    def _trigger_single_input(self, number, report, resulting_files, sf, working_dir):
        logger.debug(f"Processing report '{report}'")
        report_id = self._obtain_report_id(sf, report)
        (
            col_sys_name_and_order,
            reverse_lookup_dict,
            ordered_col_types,
            col_sys_to_other_and_type,
        ) = _obtain_report_metadata(sf, report_id)
        col_sys_name_and_order = [
            col_sys_to_other_and_type[col][self.column_by]
            for col in col_sys_name_and_order
        ]
        self.filter = (
            [
                (reverse_lookup_dict[self.column_by][column], operator, value)
                for column, operator, value in self.filter
            ]
            if self.filter
            else None
        )
        max_date, new_filter = self._obtain_max_date_and_filter(
            reverse_lookup_dict, number
        )
        logger.debug(f"Column order: {col_sys_name_and_order}")
        logger.debug(f"Filter: {self.filter}")
        logger.debug(f"Using column type: {self.column_by}")
        instance_id = _launch_report_instance(
            sf,
            report_id,
            self._support_report_instance_body,
            self.filter,
            self.filter_logic,
            new_filter,
        )
        raw_stream = _obtain_raw_report_stream(
            sf,
            report_id,
            instance_id,
            self.maximum_wait_time,
            self.poll_interval,
            self._support_restful_options,
        )

        result = self._process_raw_stream(
            raw_stream,
            number,
            working_dir,
            ordered_col_types,
            col_sys_name_and_order,
            max_date,
        )
        if result:
            resulting_files.append(result)
        else:
            logger.warning("No data to write")
            resulting_files.append(None)

    def _process_raw_stream(
        self,
        raw_stream,
        number,
        working_dir,
        ordered_col_types,
        col_sys_name_and_order,
        max_date,
    ):
        to_pandas_type = self._support_type_to_pandas or SFR_TYPE_TO_PANDAS_TYPE
        sfr_type_to_value_field = (
            self._support_type_to_value_field or SFR_TYPE_TO_VALUE_FIELD
        )
        destination_file = f"{number}.parquet"
        destination_path = os.path.join(working_dir, destination_file)
        chunk = []
        current_row = []
        parser = ijson.parse(raw_stream)
        chunk_size = self.chunk_size
        first_chunk = True
        current_value = None
        stored_a_chunk = False
        for prefix, event, value in parser:
            current_type, current_column, value_field = self._obtain_current_pointers(
                ordered_col_types,
                current_row,
                col_sys_name_and_order,
                sfr_type_to_value_field,
            )
            if prefix.endswith("rows.item") and event == "start_map":
                current_row = []
            elif prefix.endswith("rows.item") and event == "end_map":
                chunk.append(current_row.copy())
            if current_type:
                current_value, max_date = self._obtain_current_value_and_append(
                    prefix,
                    event,
                    value,
                    current_column,
                    current_value,
                    current_row,
                    max_date,
                    value_field,
                )
            if len(chunk) >= chunk_size:
                stored_a_chunk = True
                _process_and_sink_chunk(
                    chunk,
                    col_sys_name_and_order,
                    first_chunk,
                    destination_path,
                    ordered_col_types,
                    self._support_to_parquet,
                    to_pandas_type,
                )
                chunk = []
                first_chunk = False
        # We do this inconditionally, even if the chunk is empty, to create
        # an empty parquet file with the correct schema in case the result was empty
        if chunk:
            _process_and_sink_chunk(
                chunk,
                col_sys_name_and_order,
                first_chunk,
                destination_path,
                ordered_col_types,
                self._support_to_parquet,
                to_pandas_type,
            )
            stored_a_chunk = True
        if self.initial_values:
            self.initial_values[f"initial_last_modified_{number}"] = max_date
            logger.debug(f"New initial value for report {number}: {max_date}")
        return destination_file if stored_a_chunk else None

    def _obtain_current_value_and_append(
        self,
        prefix,
        event,
        value,
        current_column,
        current_value,
        current_row,
        max_date,
        value_field,
    ):
        if prefix.endswith("dataCells.item") and event == "end_map":
            if current_column == self.last_modified_column and current_value:
                previous_max_date = max_date
                max_date = _maximum_date(current_value, max_date)
                if max_date != previous_max_date:
                    logger.debug(
                        f"New max date found: {max_date} (previous: "
                        f"{previous_max_date})"
                    )
            current_row.append(current_value)
            current_value = None
        elif prefix.endswith(value_field):
            current_value = value if value != "-" else None
        return current_value, max_date

    def _obtain_current_pointers(
        self,
        ordered_col_types,
        current_row,
        col_sys_name_and_order,
        sfr_type_to_value_field,
    ):
        try:
            default_value_field = (
                self._support_default_value_field or SFR_DEFAULT_VALUE_FIELD
            )
            current_type = ordered_col_types[len(current_row)]
            current_column = col_sys_name_and_order[len(current_row)]
            value_field = sfr_type_to_value_field.get(current_type, default_value_field)
        except IndexError:
            # This means the row is fully processed, we wait until the end_map event
            # to append it to the chunk and reset the current_row
            current_type = None
            current_column = None
            value_field = None
        return current_type, current_column, value_field

    def _obtain_new_filter_for_offset(self, number, column_name: str):
        if initial_value := self.initial_values.get(f"initial_last_modified_{number}"):
            max_date = initial_value
        elif initial_value := self.initial_values.get("initial_last_modified"):
            max_date = initial_value
        else:
            raise ValueError(
                f"Missing initial last modified value for report number {number}"
            )
        new_filter = (column_name, "greaterThan", max_date)
        logger.info(f"Using new last modified date: {max_date}")
        logger.info(f"Generated new filter: {new_filter}")
        return max_date, new_filter


SFR_ID_PATTERN = r"^[a-zA-Z0-9]{15}$|^[a-zA-Z0-9]{18}$"

SFR_COLUMN_NAME_KEY = "columnName"
SFR_DATATYPE_KEY = "dataType"
SFR_DEFAULT_VALUE_FIELD = "dataCells.item.label"
SFT_DETAIL_COLUMN_INFO_KEY = "detailColumnInfo"
SFR_DETAIL_COLUMNS_KEY = "detailColumns"
SFR_REPORT_EXTENDED_METADATA_KEY = "reportExtendedMetadata"
SFR_LABEL_KEY = "label"
SFR_REPORT_METADATA_KEY = "reportMetadata"
SFR_VALID_FORMATS = ["Tabular"]

SFR_TYPE_TO_VALUE_FIELD = {
    "date": "dataCells.item.value",
    "datetime": "dataCells.item.value",
    "double": "dataCells.item.value",
    "int": "dataCells.item.value",
    "long": "dataCells.item.value",
    "string": "dataCells.item.label",
    "time": "dataCells.item.value",
    "url": "dataCells.item.value",
}

SFR_TYPE_TO_PANDAS_TYPE = {
    "boolean": "bool",
    "date": "datetime64[ns, UTC]",
    "datetime": "datetime64[ns, UTC]",
    "double": "float64",
    "int": "int64",
    "long": "int64",
    "string": "string",
    "time": "datetime64[ns, UTC]",
    "url": "string",
}


def _process_and_sink_chunk(
    chunk: list,
    column_names: list,
    first_chunk: bool,
    file_path: str,
    ordered_col_types: list,
    support_to_parquet: dict,
    to_pandas_type: dict,
):
    import pandas as pd

    columns = list(zip(*chunk))
    if not columns:
        # Empty chunk, create empty DataFrame with correct columns
        columns = [[] for _ in column_names]
    data = dict(zip(column_names, columns))
    df = pd.DataFrame(data)
    pandas_types = [to_pandas_type.get(t, "string") for t in ordered_col_types]
    df = df.astype(dict(zip(column_names, pandas_types)))

    logger.debug(f"Processing chunk with {len(df)} rows and columns {column_names}")
    logger.debug(f"Data types: {df.dtypes}")
    logger.debug(f"Writing to file: {file_path}, append: {not first_chunk}")
    df.to_parquet(
        file_path,
        engine="fastparquet",
        index=False,
        append=(not first_chunk),
        **support_to_parquet,
    )
    return


def _launch_report_instance(
    sf,
    report_id: str,
    support_report_instance_body: dict,
    filters,
    filter_logic,
    new_filter,
) -> str:
    url = f"analytics/reports/{report_id}/instances"
    logger.debug(f"Launching report instance at URL: {url}")
    logger.debug(f"With filters '{filters}' and filter logic '{filter_logic}'")
    constructed_filters = _to_sf_filters(filters) if filters else []
    constructed_boolean_filters = filter_logic
    if new_filter:
        logger.debug(f"Adding new filter for offset: {new_filter}")
        constructed_filters += _to_sf_filters([new_filter])
        if constructed_boolean_filters:
            constructed_boolean_filters = (
                f"({constructed_boolean_filters}) AND {len(constructed_filters)}"
            )
        else:
            # If we do not have constructed_boolean_filters, it means that the user
            # did not provide any filters, so we just use the new filter as the only
            # filter.
            constructed_boolean_filters = "1"
    if constructed_filters:
        body = {
            SFR_REPORT_METADATA_KEY: {
                "reportFormat": "TABULAR",
                "reportBooleanFilter": constructed_boolean_filters,
                "reportFilters": constructed_filters,
            }
        }
    else:
        body = {
            SFR_REPORT_METADATA_KEY: {
                "reportFormat": "TABULAR",
                "reportFilters": [],
            }
        }
    logger.debug(f"Launching report instance for report with ID: {report_id}")
    logger.debug(f"Report instance body: {body}")
    body.update(support_report_instance_body)

    result = sf.restful(url, method="POST", json=body)
    if not result:
        logger.warning("Failed to start report instance")
        logger.warning(f"Result: {result}")
        raise Exception("Failed to start report instance")
    instance_id = result["id"]
    logger.debug(f"Report instance started: {instance_id}")
    return instance_id


def _obtain_raw_report_stream(
    sf,
    report_id: str,
    instance_id: str,
    maximum_wait_time: int,
    poll_interval: int,
    support_restful_options: dict,
):
    url = f"analytics/reports/{report_id}/instances/{instance_id}"

    start_time = time.time()
    while time.time() - start_time < maximum_wait_time:
        response = sf.restful(url, **support_restful_options)
        attributes = response.get("attributes", {})
        status = attributes.get("status")
        logger.debug(f"Report status: {status}")
        if status == "Success":
            break
        elif status == "Error":
            status_message = attributes.get("errorMessage", "<Unknown status message>")
            logger.error(f"Completion error: {status_message}")
            raise Exception(f"Report generation failed: {status_message}")
        elif status in ["New", "Running"]:
            time.sleep(poll_interval)
        else:
            time.sleep(poll_interval)

    response = requests.get(
        f"{sf.base_url}{url}",
        headers={"Authorization": f"Bearer {sf.session_id}"},
        stream=True,
    )
    raw_stream = response.raw
    if response.headers.get("content-encoding") == "gzip":
        logger.debug("Response is gzip compressed, decompressing...")
        raw_stream = gzip.GzipFile(fileobj=response.raw)
    return raw_stream


def _to_sf_filters(filters: list[tuple[str, str, str]]) -> list[dict]:
    report_filters = []
    for column, operator, value in filters:
        report_filter = {"column": column, "operator": operator, "value": value}
        report_filters.append(report_filter)
    return report_filters


def _obtain_report_metadata(sf, report_id: str):
    url = f"{sf.base_url}analytics/reports/{report_id}/describe"
    logger.debug(f"Obtaining report metadata from URL: {url}")
    response = sf.session.get(url, headers=sf.headers)
    logger.debug(f"Response status code: {response.status_code}")
    response.raise_for_status()
    s = response.json()
    col_sys_name_and_order = s[SFR_REPORT_METADATA_KEY][SFR_DETAIL_COLUMNS_KEY]
    reverse_lookup_dict = {
        SFR_LABEL_KEY: {},
        SFR_COLUMN_NAME_KEY: {},
    }
    col_sys_to_other_and_type = {}
    for col_sys_name, value in s[SFR_REPORT_EXTENDED_METADATA_KEY][
        SFT_DETAIL_COLUMN_INFO_KEY
    ].items():
        datatype = value.get(SFR_DATATYPE_KEY)
        label = value.get(SFR_LABEL_KEY)
        reverse_lookup_dict[SFR_LABEL_KEY][label] = col_sys_name
        reverse_lookup_dict[SFR_COLUMN_NAME_KEY][col_sys_name] = col_sys_name
        col_sys_to_other_and_type[col_sys_name] = {
            SFR_LABEL_KEY: label,
            SFR_DATATYPE_KEY: datatype,
            SFR_COLUMN_NAME_KEY: col_sys_name,
        }
    ordered_col_types = [
        col_sys_to_other_and_type[col][SFR_DATATYPE_KEY]
        for col in col_sys_name_and_order
    ]
    logger.debug(f"Column system names: {col_sys_name_and_order}")
    logger.debug(f"Ordered column types: {ordered_col_types}")
    logger.debug(f"Reverse lookup dictionary: {reverse_lookup_dict}")
    logger.debug(f"Column system to other and type: {col_sys_to_other_and_type}")
    logger.debug("Finished obtaining report metadata")
    return (
        col_sys_name_and_order,
        reverse_lookup_dict,
        ordered_col_types,
        col_sys_to_other_and_type,
    )


def _obtain_report_id_and_verify_format(
    sf, report: str, report_identifier: Literal["id", "name"]
) -> str:
    from simple_salesforce import format_soql

    soql_query = """
               select Id,
                      Format
                 from Report
                where ( DeveloperName in {name} or Id in {id} ) and IsDeleted = false
           """
    logger.debug(f"Obtaining report ID for report '{report}'")
    id = [report] if report_identifier == "id" else [""]
    name = [report] if report_identifier == "name" else [""]
    soql_query = format_soql(soql_query, id=id, name=name)
    logger.debug(f"SOQL query after formatting: {soql_query}")
    results = sf.query(soql_query)

    report_id = None
    if len(results["records"]) > 1:
        raise ValueError(
            f"More than one report found for '{report}'. Please provide "
            "a more specific report name or the report ID. The reports "
            "found are: "
            f"{', '.join([record['Id'] for record in results['records']])}"
        )
    for record in results["records"]:
        report_id = record["Id"]
        if record["Format"] != "Tabular":
            raise ValueError(
                f"Report '{report}' is of format '{record['Format']}'. Only"
                f" '{', '.join(SFR_VALID_FORMATS)}' reports are"
                " supported."
            )

    if not report_id:
        logger.warning(
            f"No report found for '{report}' and identifier type '{report_identifier}'."
        )
    return report_id


def _log_into_salesforce(
    plugin: SalesforceSource | SalesforceReportSource,
):

    from simple_salesforce import Salesforce

    return Salesforce(
        username=plugin.username.secret_value,
        password=plugin.password.secret_value,
        security_token=plugin.security_token.secret_value,
        instance_url=plugin.instance_url,
        version=plugin.api_version,
    )
