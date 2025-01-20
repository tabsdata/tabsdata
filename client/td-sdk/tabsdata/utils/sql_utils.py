#
# Copyright 2025 Tabs Data Inc.
#

import logging
from enum import Enum

import sqlalchemy.dialects.mysql.mysqlconnector as mysqlconnector
from sqlalchemy import create_engine

from tabsdata.tabsdatafunction import (
    MariaDBDestination,
    MariaDBSource,
    MySQLDestination,
    MySQLSource,
    OracleDestination,
    OracleSource,
    Output,
    PostgresDestination,
    PostgresSource,
)

formatter = logging.Formatter("%(levelname)s: %(message)s")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.WARNING)


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


# TODO: Consider unifying this file and function_execution/sql_utils.py. For now,
#  there are enough differences to keep them separate.
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
    elif uri.startswith("mariadb"):
        # In polars, the 'mariadb://' URI scheme is not supported, so we replace it with
        # 'mysql://' URI scheme
        uri = uri.replace("mariadb", "mysql")
    return uri


def add_driver_to_uri(uri: str) -> str:
    if uri.startswith(SupportedSQL.MYSQL.value.name + "://"):
        descriptor = SupportedSQL.MYSQL.value
        name = descriptor.name
        driver = descriptor.driver
        uri = uri.replace(f"{name}://", f"{name}+{driver}://")
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
        uri = obtain_uri(output)
        uri = add_driver_to_uri(uri)
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
