#
# Copyright 2025 Tabs Data Inc.
#

import logging
import urllib.parse
from enum import Enum

import sqlalchemy.dialects.mysql.mysqlconnector as mysqlconnector
from sqlalchemy import create_engine

from tabsdata.credentials import UserPasswordCredentials
from tabsdata.io.input import (
    MariaDBSource,
    MySQLSource,
    OracleSource,
    PostgresSource,
)
from tabsdata.io.output import (
    MariaDBDestination,
    MySQLDestination,
    OracleDestination,
    Output,
    PostgresDestination,
)

formatter = logging.Formatter("%(levelname)s: %(message)s")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.WARNING)


MAX_USER_LENGTH = 16
MAX_PASSWORD_LENGTH = 16


class SQLDescriptor:
    name: str
    driver: str

    def __init__(self, name: str, driver: str):
        self.name = name
        self.driver = driver


class SupportedSQL(Enum):
    MYSQL = SQLDescriptor(
        name=mysqlconnector.dialect.name, driver=mysqlconnector.dialect.driver
    )


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
    logger.debug("Adding driver to uri") if log else None
    if uri.startswith(SupportedSQL.MYSQL.value.name + "://"):
        descriptor = SupportedSQL.MYSQL.value
        name = descriptor.name
        driver = descriptor.driver
        uri = uri.replace(f"{name}://", f"{name}+{driver}://", 1)
        logger.debug(f"Added driver '{driver}' to '{name}' uri") if log else None
    else:
        (
            logger.debug(
                "Driver not added to uri. URI did not start with a value"
                f" in{', '.join([sql_descriptor.value.name
                                 for sql_descriptor in SupportedSQL])}'"
                " or already had a driver"
            )
            if log
            else None
        )
    return uri


DRIVER_TYPE_AND_RECOMMENDATION_FOR_OUTPUT = {
    MySQLDestination: ("MySQL", "mysql-connector-python"),
    OracleDestination: ("Oracle", "cx_Oracle"),
    PostgresDestination: ("Postgres", "psycopg2-binary"),
    MariaDBDestination: ("MariaDB", "mysql-connector-python"),
}


def verify_output_sql_drivers(output: Output):
    if isinstance(
        output,
        (MySQLDestination, OracleDestination, PostgresDestination, MariaDBDestination),
    ):
        uri = obtain_uri(output, log=False, add_credentials=False)
        uri = add_driver_to_uri(uri, log=False)
        try:
            engine = create_engine(uri)
            engine.dispose()
        except Exception as e:
            driver_type, recommended_driver = DRIVER_TYPE_AND_RECOMMENDATION_FOR_OUTPUT[
                type(output)
            ]
            logger.warning("-" * 50)
            logger.warning(
                "The local Python environment does not have a suitable "
                f"{driver_type} driver installed. The function will likely "
                "fail to execute when running in the Tabsdata server."
            )
            logger.warning("")
            logger.warning("It is recommended to either:")
            logger.warning(
                f"  Install a {driver_type} driver in your local "
                "environment, for example: 'pip install "
                f"{recommended_driver}'; and then update the function by running "
                "'td function update'."
            )
            logger.warning(
                "  Or create a custom requirements.yaml file for the "
                f"function and add a {driver_type} driver to it; and then "
                "update the function by running 'td function update'."
            )
            logger.warning("")
            logger.warning(f"Original error: {e}")
            logger.warning("-" * 50)
    else:
        return
