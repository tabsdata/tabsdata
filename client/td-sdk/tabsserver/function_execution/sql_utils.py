#
# Copyright 2024 Tabs Data Inc.
#

import logging
import urllib.parse

from tabsdata.credentials import UserPasswordCredentials
from tabsdata.tabsdatafunction import (
    MariaDBDestination,
    MariaDBSource,
    MySQLDestination,
    MySQLSource,
    OracleDestination,
    OracleSource,
    PostgresDestination,
    PostgresSource,
)
from tabsdata.utils.sql_utils import SupportedSQL

logger = logging.getLogger(__name__)

MAX_USER_LENGTH = 16
MAX_PASSWORD_LENGTH = 16

MARIADB_COLLATION = "utf8mb4_unicode_520_ci"


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
) -> str:
    uri = io_sql_configuration.uri
    if uri.startswith("postgres://") or uri.startswith("postgres+"):
        # Some libraries, like SQLAlchemy, use 'postgresql://' instead of 'postgres://'
        # This is used to prevent compatibility issues or deprecation issues when using
        # the 'postgres://' URI scheme
        uri = uri.replace("postgres", "postgresql")
        logger.debug(
            "Using 'postgresql' instead of 'postgres' to connect to the database"
        )
    elif uri.startswith("mariadb"):
        # In polars, the 'mariadb://' URI scheme is not supported, so we replace it with
        # 'mysql://' URI scheme
        uri = uri.replace("mariadb", "mysql")
        logger.debug("Using 'mysql' instead of 'mariadb' to connect to the database")
    if io_sql_configuration.credentials:
        logger.debug("Using credentials to connect to the database")
        credentials = io_sql_configuration.credentials
        if isinstance(credentials, UserPasswordCredentials):
            user_to_be_checked = credentials.user.secret_value
            password_to_be_checked = credentials.password.secret_value
            validate_user_password(user_to_be_checked, password_to_be_checked)
            user = escape_special_characters(user_to_be_checked)
            password = escape_special_characters(password_to_be_checked)
            uri = uri.replace("://", f"://{user}:{password}@")
            return uri
        else:
            logger.error(f"Credentials of type '{type(credentials)}' not supported")
            raise TypeError(f"Credentials of type '{type(credentials)}' not supported")
    else:
        logger.debug("No credentials provided to connect to the database")
        return uri


def validate_user_password(user: str, password: str):
    if len(user) > MAX_USER_LENGTH or len(password) > MAX_PASSWORD_LENGTH:
        error_message = (
            "User or password length exceeds the maximum allowed length of "
            f"{MAX_USER_LENGTH} characters for the user and "
            f"{MAX_PASSWORD_LENGTH} for the password"
        )
        logger.error(error_message)
        raise ValueError(error_message)


def escape_special_characters(string: str) -> str:
    return urllib.parse.quote(string, safe="")


def add_driver_to_uri(uri: str) -> str:
    logger.debug("Adding driver to uri")
    if uri.startswith(SupportedSQL.MYSQL.value.name + "://"):
        descriptor = SupportedSQL.MYSQL.value
        name = descriptor.name
        driver = descriptor.driver
        uri = uri.replace(f"{name}://", f"{name}+{driver}://")
        logger.debug(f"Added driver '{driver}' to '{name}' uri")
    else:
        logger.debug(
            "Driver not added to uri. URI did not start with a value in"
            f" '{', '.join([sql_descriptor.value.name
                            for sql_descriptor in SupportedSQL])}'"
            " or already had a driver"
        )
    return uri


def add_mariadb_collation(uri: str) -> str:
    # Note: if the user has not provided a collation parameter, we must add it
    # to ensure that the driver works properly.
    if "collation" not in uri:
        logger.debug("Adding collation parameter to the MariaDB URI")
        if "?" in uri:
            uri = f"{uri}&collation={MARIADB_COLLATION}"
        else:
            uri = f"{uri}?collation={MARIADB_COLLATION}"
    else:
        logger.debug("Collation parameter already exists in the MariaDB URI")

    return uri
