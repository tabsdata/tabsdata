#
# Copyright 2025 Tabs Data Inc.
#

from tabsdata import (
    MariaDBDestination,
    MariaDBSource,
    MySQLDestination,
    MySQLSource,
    OracleDestination,
    OracleSource,
    PostgresDestination,
    PostgresSource,
    UserPasswordCredentials,
)
from tabsdata.utils.sql_utils import obtain_uri


def test_obtain_uri_postgres_source():
    uri = "postgres://user:password@localhost:5432/postgres_postgresql"
    io_config = PostgresSource(uri=uri, query="SELECT * FROM table")
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "postgresql://user:password@localhost:5432/postgres_postgresql"
    )
    uri = "postgresql://user:password@localhost:5432/postgres_postgresql"
    io_config.uri = uri
    assert obtain_uri(io_config, add_credentials=True) == uri
    uri = "postgres://localhost:5432/postgres_postgresql"
    io_config.uri = uri
    io_config.credentials = UserPasswordCredentials("user", "password")
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "postgresql://user:password@localhost:5432/postgres_postgresql"
    )


def test_obtain_uri_postgres_destination():
    uri = "postgres://user:password@localhost:5432/postgres_postgresql"
    io_config = PostgresDestination(uri=uri, destination_table="table")
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "postgresql://user:password@localhost:5432/postgres_postgresql"
    )
    uri = "postgresql://user:password@localhost:5432/postgres_postgresql"
    io_config.uri = uri
    assert obtain_uri(io_config, add_credentials=True) == uri
    uri = "postgres+psycopg2://user:password@localhost:5432/postgres_postgresql"
    io_config.uri = uri
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "postgresql+psycopg2://user:password@localhost:5432/postgres_postgresql"
    )
    uri = "postgresql+psycopg2://user:password@localhost:5432/postgres_postgresql"
    io_config.uri = uri
    assert obtain_uri(io_config, add_credentials=True) == uri
    uri = "postgres+psycopg2://localhost:5432/postgres_postgresql"
    io_config.uri = uri
    io_config.credentials = UserPasswordCredentials("user", "password")
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "postgresql+psycopg2://user:password@localhost:5432/postgres_postgresql"
    )


def test_obtain_uri_mariadb_source():
    uri = "mariadb://user:password@localhost:5432/mariadb_mariadbql"
    io_config = MariaDBSource(uri=uri, query="SELECT * FROM table")
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "mysql://user:password@localhost:5432/mariadb_mariadbql"
    )
    uri = "mariadb://user:password@localhost:5432/mariadb_mariadbql"
    io_config.uri = uri
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "mysql://user:password@localhost:5432/mariadb_mariadbql"
    )
    uri = "mariadb://localhost:5432/mariadb_mariadbql"
    io_config.uri = uri
    io_config.credentials = UserPasswordCredentials("user", "password")
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "mysql://user:password@localhost:5432/mariadb_mariadbql"
    )


def test_obtain_uri_mariadb_destination():
    uri = "mariadb://user:password@localhost:5432/mariadb_mariadbql"
    io_config = MariaDBDestination(uri=uri, destination_table="table")
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "mysql://user:password@localhost:5432/mariadb_mariadbql"
    )
    uri = "mariadb://user:password@localhost:5432/mariadb_mariadbql"
    io_config.uri = uri
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "mysql://user:password@localhost:5432/mariadb_mariadbql"
    )
    uri = "mariadb+psycopg2://user:password@localhost:5432/mariadb_mariadbql"
    io_config.uri = uri
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "mysql+psycopg2://user:password@localhost:5432/mariadb_mariadbql"
    )
    uri = "mariadb+psycopg2://user:password@localhost:5432/mariadb_mariadbql"
    io_config.uri = uri
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "mysql+psycopg2://user:password@localhost:5432/mariadb_mariadbql"
    )
    uri = "mariadb+psycopg2://localhost:5432/mariadb_mariadbql"
    io_config.uri = uri
    io_config.credentials = UserPasswordCredentials("user", "password")
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "mysql+psycopg2://user:password@localhost:5432/mariadb_mariadbql"
    )


def test_obtain_uri_oracle_source():
    uri = "oracle://user:password@localhost:5432/oracle_oracleql"
    io_config = OracleSource(uri=uri, query="SELECT * FROM table")
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "oracle://user:password@localhost:5432/oracle_oracleql"
    )
    uri = "oracle://user:password@localhost:5432/oracle_oracleql"
    io_config.uri = uri
    assert obtain_uri(io_config, add_credentials=True) == uri
    uri = "oracle://localhost:5432/oracle_oracleql"
    io_config.uri = uri
    io_config.credentials = UserPasswordCredentials("user", "password")
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "oracle://user:password@localhost:5432/oracle_oracleql"
    )


def test_obtain_uri_oracle_destination():
    uri = "oracle://user:password@localhost:5432/oracle_oracleql"
    io_config = OracleDestination(uri=uri, destination_table="table")
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "oracle://user:password@localhost:5432/oracle_oracleql"
    )
    uri = "oracle://user:password@localhost:5432/oracle_oracleql"
    io_config.uri = uri
    assert obtain_uri(io_config, add_credentials=True) == uri
    uri = "oracle+psycopg2://user:password@localhost:5432/oracle_oracleql"
    io_config.uri = uri
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "oracle+psycopg2://user:password@localhost:5432/oracle_oracleql"
    )
    uri = "oracle+psycopg2://user:password@localhost:5432/oracle_oracleql"
    io_config.uri = uri
    assert obtain_uri(io_config, add_credentials=True) == uri
    uri = "oracle+psycopg2://localhost:5432/oracle_oracleql"
    io_config.uri = uri
    io_config.credentials = UserPasswordCredentials("user", "password")
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "oracle+psycopg2://user:password@localhost:5432/oracle_oracleql"
    )


def test_obtain_uri_mysql_source():
    uri = "mysql://user:password@localhost:5432/mysql_mysqlql"
    io_config = MySQLSource(uri=uri, query="SELECT * FROM table")
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "mysql://user:password@localhost:5432/mysql_mysqlql"
    )
    uri = "mysql://user:password@localhost:5432/mysql_mysqlql"
    io_config.uri = uri
    assert obtain_uri(io_config, add_credentials=True) == uri
    uri = "mysql://localhost:5432/mysql_mysqlql"
    io_config.uri = uri
    io_config.credentials = UserPasswordCredentials("user", "password")
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "mysql://user:password@localhost:5432/mysql_mysqlql"
    )


def test_obtain_uri_mysql_destination():
    uri = "mysql://user:password@localhost:5432/mysql_mysqlql"
    io_config = MySQLDestination(uri=uri, destination_table="table")
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "mysql://user:password@localhost:5432/mysql_mysqlql"
    )
    uri = "mysql://user:password@localhost:5432/mysql_mysqlql"
    io_config.uri = uri
    assert obtain_uri(io_config, add_credentials=True) == uri
    uri = "mysql+psycopg2://user:password@localhost:5432/mysql_mysqlql"
    io_config.uri = uri
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "mysql+psycopg2://user:password@localhost:5432/mysql_mysqlql"
    )
    uri = "mysql+psycopg2://user:password@localhost:5432/mysql_mysqlql"
    io_config.uri = uri
    assert obtain_uri(io_config, add_credentials=True) == uri
    uri = "mysql+psycopg2://localhost:5432/mysql_mysqlql"
    io_config.uri = uri
    io_config.credentials = UserPasswordCredentials("user", "password")
    assert (
        obtain_uri(io_config, add_credentials=True)
        == "mysql+psycopg2://user:password@localhost:5432/mysql_mysqlql"
    )
