#
# Copyright 2025 Tabs Data Inc.
#

import csv
import logging
import os
from typing import List

import polars as pl

from tabsdata._io.plugin import SourcePlugin
from tabsdata._secret import DirectSecret, EnvironmentSecret, HashiCorpSecret, Secret

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class SalesforceSource(SourcePlugin):

    LAST_MODIFIED_COLUMN = "SystemModstamp"
    DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
    LAST_MODIFIED_TOKEN = "$lastModified"

    def __init__(
        self,
        username: str | HashiCorpSecret | DirectSecret | EnvironmentSecret,
        password: str | HashiCorpSecret | DirectSecret | EnvironmentSecret,
        security_token: str | HashiCorpSecret | DirectSecret | EnvironmentSecret,
        query: str | List[str],
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
            query (str | List[str]): The query or queries to execute in Salesforce.
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
                    parser.parse(initial_last_modified).strftime(self.DATE_FORMAT)
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
    def query(self) -> List[str]:
        return self._query

    @query.setter
    def query(self, query: str | List[str]):
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

    def chunk(self, working_dir: str) -> List[str]:
        sf = self._log_into_salesforce()
        resulting_files = []
        using_initial_values = bool(self.initial_values)
        for number, query in enumerate(self.query):
            self._trigger_single_input(
                number, query, resulting_files, sf, working_dir, using_initial_values
            )
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
                        max_date = self._maximum_date(current_last_modified, max_date)
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

    def _maximum_date(self, date1: str, date2: str):

        from dateutil import parser

        dates = [parser.parse(date) for date in [date1, date2]]
        result = max(dates).strftime(self.DATE_FORMAT)
        return result

    def _log_into_salesforce(self):

        from simple_salesforce import Salesforce

        return Salesforce(
            username=self.username.secret_value,
            password=self.password.secret_value,
            security_token=self.security_token.secret_value,
            instance_url=self.instance_url,
            version=self.api_version,
        )
