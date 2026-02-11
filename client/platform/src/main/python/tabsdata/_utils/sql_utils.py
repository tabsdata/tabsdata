#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
import urllib.parse
from enum import Enum
from typing import TYPE_CHECKING, get_args

import polars as pl

from tabsdata._credentials import UserPasswordCredentials
from tabsdata._utils.tableframe.builders import from_polars
from tabsdata.exceptions import ErrorCode, SourceConfigurationError
from tabsdata.tableframe import TableFrame
from tabsdata.tableframe.typing import DataType

if TYPE_CHECKING:
    from tabsdata._io.inputs.sql_inputs import (
        MariaDBSource,
        MySQLSource,
        OracleSource,
        PostgresSource,
    )
    from tabsdata._io.outputs.sql_outputs import (
        MariaDBDestination,
        MySQLDestination,
        OracleDestination,
        PostgresDestination,
    )

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


MAX_USER_LENGTH = 128
MAX_PASSWORD_LENGTH = 1024


class SQLDescriptor:
    name: str
    driver: str

    def __init__(self, name: str, driver: str):
        self.name = name
        self.driver = driver


def obtain_uri(
    io_sql_configuration: (
        MariaDBSource
        | MariaDBDestination
        | MySQLSource
        | MySQLDestination
        | PostgresSource
        | PostgresDestination
        | OracleSource
        | OracleDestination
    ),
    log=False,
    add_credentials=False,
) -> str:
    uri = io_sql_configuration.uri
    if uri.startswith("postgres://") or uri.startswith("postgres+"):
        # Some libraries, like SQLAlchemy, use 'postgresql://' instead of 'postgres://'
        # This is used to prevent compatibility issues or deprecation issues when using
        # the 'postgres://' URI scheme
        uri = uri.replace("postgres", "postgresql", 1)
        (
            logger.debug(
                "Using 'postgresql' instead of 'postgres' to connect to the database"
            )
            if log
            else None
        )
    elif uri.startswith("mariadb"):
        # In polars, the 'mariadb://' URI scheme is not supported, so we replace it with
        # 'mysql://' URI scheme
        uri = uri.replace("mariadb", "mysql", 1)
        (
            logger.debug(
                "Using 'mysql' instead of 'mariadb' to connect to the database"
            )
            if log
            else None
        )
    uri = (
        add_credentials_to_uri(io_sql_configuration, log, uri)
        if add_credentials
        else uri
    )
    return uri


def add_credentials_to_uri(io_sql_configuration, log, uri):
    if io_sql_configuration.credentials:
        logger.debug("Using credentials to connect to the database") if log else None
        credentials = io_sql_configuration.credentials
        if isinstance(credentials, UserPasswordCredentials):
            user_to_be_checked = credentials.user.secret_value
            password_to_be_checked = credentials.password.secret_value
            validate_user_password(user_to_be_checked, password_to_be_checked)
            user = escape_special_characters(user_to_be_checked)
            password = escape_special_characters(password_to_be_checked)
            uri = uri.replace("://", f"://{user}:{password}@", 1)
        else:
            (
                logger.error(f"Credentials of type '{type(credentials)}' not supported")
                if log
                else None
            )
            raise TypeError(f"Credentials of type '{type(credentials)}' not supported")
    else:
        (
            logger.debug("No credentials provided to connect to the database")
            if log
            else None
        )
    return uri


def validate_user_password(user: str, password: str, log=False):
    if len(user) > MAX_USER_LENGTH or len(password) > MAX_PASSWORD_LENGTH:
        error_message = (
            "User or password length exceeds the maximum allowed length of "
            f"{MAX_USER_LENGTH} characters for the user and "
            f"{MAX_PASSWORD_LENGTH} for the password"
        )
        logger.error(error_message) if log else None
        raise ValueError(error_message)


def escape_special_characters(string: str) -> str:
    return urllib.parse.quote(string, safe="")


def add_driver_to_uri(uri: str, log=False) -> str:

    import sqlalchemy.dialects.mssql.pyodbc as mssqlconnector
    import sqlalchemy.dialects.mysql.mysqlconnector as mysqlconnector
    import sqlalchemy.dialects.oracle.oracledb as oracleconnector

    class SupportedSQL(Enum):
        MYSQL = SQLDescriptor(
            name=mysqlconnector.dialect.name, driver=mysqlconnector.dialect.driver
        )
        ORACLE = SQLDescriptor(
            name=oracleconnector.dialect.name, driver=oracleconnector.dialect.driver
        )
        MSSQL = SQLDescriptor(
            name=mssqlconnector.dialect.name, driver=mssqlconnector.dialect.driver
        )

    logger.debug("Adding driver to uri") if log else None

    for supported_sql in SupportedSQL:
        descriptor = supported_sql.value
        name = descriptor.name
        driver = descriptor.driver
        prefix = f"{descriptor.name}://"
        if uri.startswith(prefix):
            uri = uri.replace(prefix, f"{name}+{driver}://", 1)
            if log:
                logger.debug(f"Added driver '{driver}' to '{name}' uri")
            break
    else:
        if log:
            known_prefixes = ", ".join(f"{sql.value.name}://" for sql in SupportedSQL)
            logger.debug(
                "Driver not added. The uri does not match any database prefix that"
                f" requires adding a driver: {known_prefixes}"
            )
    return uri


def validate_schema_overrides_list(
    schema_overrides: list[dict[str, DataType]], queries: str | list[str]
):
    not_dict_items = [item for item in schema_overrides if not isinstance(item, dict)]
    if len(not_dict_items):
        raise SourceConfigurationError(
            ErrorCode.SOCE54, not_dict_items, [type(item) for item in not_dict_items]
        )

    not_str_keys = [
        key
        for item in schema_overrides
        if isinstance(item, dict)
        for key in item.keys()
        if not isinstance(key, str)
    ]
    if len(not_str_keys):
        raise SourceConfigurationError(
            ErrorCode.SOCE55, not_str_keys, [type(key) for key in not_str_keys]
        )

    not_datatype_values = [
        value
        for item in schema_overrides
        for value in item.values()
        if value not in get_args(DataType)
    ]
    if len(not_datatype_values):
        raise SourceConfigurationError(
            ErrorCode.SOCE56,
            not_datatype_values,
            [type(value) for value in not_datatype_values],
        )

    if len(schema_overrides) != len(
        queries if isinstance(queries, list) else [queries]
    ):
        raise SourceConfigurationError(
            ErrorCode.SOCE57,
            len(schema_overrides),
            len(queries) if isinstance(queries, list) else 1,
        )


def validate_schema_overrides_dict(
    schema_overrides: dict[str, DataType], queries: str | list[str]
):
    not_str_keys = [key for key in schema_overrides.keys() if not isinstance(key, str)]
    not_datatype_values = [
        value for value in schema_overrides.values() if value not in get_args(DataType)
    ]

    if len(not_str_keys):
        raise SourceConfigurationError(
            ErrorCode.SOCE55, not_str_keys, [type(key) for key in not_str_keys]
        )
    if len(not_datatype_values):
        raise SourceConfigurationError(
            ErrorCode.SOCE56,
            not_datatype_values,
            [type(value) for value in not_datatype_values],
        )

    if isinstance(queries, list) and len(queries) > 1:
        raise SourceConfigurationError(
            ErrorCode.SOCE58, len(queries), type(schema_overrides)
        )


def get_tableframes_from_sources(
    local_sources: list[list[str]],
) -> list[TableFrame]:
    """
    Given a list of sources, where each source is a list of parquet file paths,
    concatenate the files for each source into a TableFrame.

    Args:
        local_sources (list[list[str]]): List of sources, each a list of parquet file
        paths.
    Returns:
        list[TableFrame]: List of TableFrames, one per source.
    """
    table_frames: list[TableFrame] = []
    for idx, source in enumerate(local_sources):
        if isinstance(source, list):
            intermediate_lfs = [
                pl.scan_parquet(f) for f in source if isinstance(f, str)
            ]
            lf = pl.concat(intermediate_lfs)
            tf = from_polars(lf)
            table_frames.append(tf)
            logger.debug(f"Created TableFrame {repr(tf)} for query {idx}")
        else:
            raise TypeError(
                "Each query result must be a list(str) of file paths containing "
                f"chunks of records, got {type(source)} instead."
            )
    logger.debug(f"Returning {len(table_frames)} TableFrames")
    return table_frames
