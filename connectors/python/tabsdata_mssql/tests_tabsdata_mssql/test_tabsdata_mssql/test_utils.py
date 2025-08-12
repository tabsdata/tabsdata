#
# Copyright 2025 Tabs Data Inc.
#

import logging

import pytest

# noinspection PyPackageRequirements
from pytest import MonkeyPatch
from tests_tabsdata_mssql.conftest import MSSQL_2022_PORT

# noinspection PyPackageRequirements
import tabsdata as td
from tabsdata_mssql._connector import _add_field_to_string, _obtain_connection_string
from tests_tabsdata.conftest import (
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_USER,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.mssql
def test_obtain_connection_string_regular_source():
    created_source = td.MSSQLSource(
        connection_string=(
            "DRIVER={ODBC Driver 18 for SQL"
            f" Server}};SERVER={DB_HOST},{MSSQL_2022_PORT};UID={DB_USER};PWD={DB_PASSWORD};"
            f"Database={DB_NAME};TrustServerCertificate=yes;"
        ),
        query="SELECT * FROM INVOICE_HEADER",
    )
    assert (
        _obtain_connection_string(created_source)
        == "DRIVER={ODBC Driver 18 for SQL"
        f" Server}};SERVER={DB_HOST},{MSSQL_2022_PORT};UID={DB_USER};PWD={DB_PASSWORD};"
        f"Database={DB_NAME};TrustServerCertificate=yes;"
    )


@pytest.mark.mssql
def test_obtain_connection_string_adds_finisher():
    created_source = td.MSSQLSource(
        connection_string=(
            "DRIVER={ODBC Driver 18 for SQL"
            f" Server}};SERVER={DB_HOST},{MSSQL_2022_PORT};UID={DB_USER};PWD={DB_PASSWORD};"
            f"Database={DB_NAME};TrustServerCertificate=yes"
        ),
        query="SELECT * FROM INVOICE_HEADER",
    )
    assert (
        _obtain_connection_string(created_source)
        == "DRIVER={ODBC Driver 18 for SQL"
        f" Server}};SERVER={DB_HOST},{MSSQL_2022_PORT};UID={DB_USER};PWD={DB_PASSWORD};"
        f"Database={DB_NAME};TrustServerCertificate=yes;"
    )


@pytest.mark.mssql
def test_obtain_connection_string_password_and_user():
    with MonkeyPatch.context() as mp:
        mp.setenv("DB_PASSWORD_TEST", DB_PASSWORD)
        created_source = td.MSSQLSource(
            connection_string=(
                "DRIVER={ODBC Driver 18 for SQL"
                f" Server}};SERVER={DB_HOST},{MSSQL_2022_PORT};"
                f"Database={DB_NAME};TrustServerCertificate=yes"
            ),
            credentials=td.UserPasswordCredentials(
                DB_USER, td.EnvironmentSecret("DB_PASSWORD_TEST")
            ),
            query="SELECT * FROM INVOICE_HEADER",
        )
        assert (
            _obtain_connection_string(created_source)
            == "DRIVER={ODBC Driver 18 for SQL"
            f" Server}};SERVER={DB_HOST},{MSSQL_2022_PORT};"
            f"Database={DB_NAME};TrustServerCertificate=yes;UID={DB_USER};"
            f"PWD={DB_PASSWORD};"
        )


@pytest.mark.mssql
def test_obtain_connection_string_support_extra_connection_string_secrets():
    with MonkeyPatch.context() as mp:
        mp.setenv("DB_PASSWORD_TEST", DB_PASSWORD)
        mp.setenv("VALUE2", "VALUE2_TEST")
        created_source = td.MSSQLSource(
            connection_string=(
                "DRIVER={ODBC Driver 18 for SQL"
                f" Server}};SERVER={DB_HOST},{MSSQL_2022_PORT};"
                f"Database={DB_NAME};TrustServerCertificate=yes"
            ),
            credentials=td.UserPasswordCredentials(
                DB_USER, td.EnvironmentSecret("DB_PASSWORD_TEST")
            ),
            query="SELECT * FROM INVOICE_HEADER",
            support_extra_connection_string_secrets=[
                ("FIELD1", "VALUE1"),
                ("FIELD2", td.EnvironmentSecret("VALUE2")),
            ],
        )
        assert (
            _obtain_connection_string(created_source)
            == "DRIVER={ODBC Driver 18 for SQL"
            f" Server}};SERVER={DB_HOST},{MSSQL_2022_PORT};"
            f"Database={DB_NAME};TrustServerCertificate=yes;UID={DB_USER};"
            f"PWD={DB_PASSWORD};FIELD1=VALUE1;FIELD2=VALUE2_TEST;"
        )


@pytest.mark.mssql
def test_obtain_connection_string_regular_destination():
    created_destination = td.MSSQLDestination(
        connection_string=(
            "DRIVER={ODBC Driver 18 for SQL"
            f" Server}};SERVER={DB_HOST},{MSSQL_2022_PORT};UID={DB_USER};PWD={DB_PASSWORD};"
            f"Database={DB_NAME};TrustServerCertificate=yes;"
        ),
        destination_table="FAKE_DESTINATION_TABLE",
    )
    assert (
        _obtain_connection_string(created_destination)
        == "DRIVER={ODBC Driver 18 for SQL"
        f" Server}};SERVER={DB_HOST},{MSSQL_2022_PORT};UID={DB_USER};PWD={DB_PASSWORD};"
        f"Database={DB_NAME};TrustServerCertificate=yes;"
    )


@pytest.mark.mssql
def test_obtain_connection_string_destination_adds_finisher():
    created_destination = td.MSSQLDestination(
        connection_string=(
            "DRIVER={ODBC Driver 18 for SQL"
            f" Server}};SERVER={DB_HOST},{MSSQL_2022_PORT};UID={DB_USER};PWD={DB_PASSWORD};"
            f"Database={DB_NAME};TrustServerCertificate=yes"
        ),
        destination_table="FAKE_DESTINATION_TABLE",
    )
    assert (
        _obtain_connection_string(created_destination)
        == "DRIVER={ODBC Driver 18 for SQL"
        f" Server}};SERVER={DB_HOST},{MSSQL_2022_PORT};UID={DB_USER};PWD={DB_PASSWORD};"
        f"Database={DB_NAME};TrustServerCertificate=yes;"
    )


@pytest.mark.mssql
def test_obtain_connection_string_destination_password_and_user():
    with MonkeyPatch.context() as mp:
        mp.setenv("DB_PASSWORD_TEST", DB_PASSWORD)
        created_destination = td.MSSQLDestination(
            connection_string=(
                "DRIVER={ODBC Driver 18 for SQL"
                f" Server}};SERVER={DB_HOST},{MSSQL_2022_PORT};"
                f"Database={DB_NAME};TrustServerCertificate=yes"
            ),
            credentials=td.UserPasswordCredentials(
                DB_USER, td.EnvironmentSecret("DB_PASSWORD_TEST")
            ),
            destination_table="FAKE_DESTINATION_TABLE",
        )
        assert (
            _obtain_connection_string(created_destination)
            == "DRIVER={ODBC Driver 18 for SQL"
            f" Server}};SERVER={DB_HOST},{MSSQL_2022_PORT};"
            f"Database={DB_NAME};TrustServerCertificate=yes;UID={DB_USER};"
            f"PWD={DB_PASSWORD};"
        )


@pytest.mark.mssql
def test_obtain_connection_string_destination_support_extra_connection_string_secrets():
    with MonkeyPatch.context() as mp:
        mp.setenv("DB_PASSWORD_TEST", DB_PASSWORD)
        mp.setenv("VALUE2", "VALUE2_TEST")
        created_destination = td.MSSQLDestination(
            connection_string=(
                "DRIVER={ODBC Driver 18 for SQL"
                f" Server}};SERVER={DB_HOST},{MSSQL_2022_PORT};"
                f"Database={DB_NAME};TrustServerCertificate=yes"
            ),
            credentials=td.UserPasswordCredentials(
                DB_USER, td.EnvironmentSecret("DB_PASSWORD_TEST")
            ),
            destination_table="FAKE_DESTINATION_TABLE",
            support_extra_connection_string_secrets=[
                ("FIELD1", "VALUE1"),
                ("FIELD2", td.EnvironmentSecret("VALUE2")),
            ],
        )
        assert (
            _obtain_connection_string(created_destination)
            == "DRIVER={ODBC Driver 18 for SQL"
            f" Server}};SERVER={DB_HOST},{MSSQL_2022_PORT};"
            f"Database={DB_NAME};TrustServerCertificate=yes;UID={DB_USER};"
            f"PWD={DB_PASSWORD};FIELD1=VALUE1;FIELD2=VALUE2_TEST;"
        )


@pytest.mark.mssql
def test_add_field_to_string_empty():
    result = _add_field_to_string(
        "NewField",
        "NewValue",
        "",
    )
    assert result == "NewField=NewValue;"


@pytest.mark.mssql
def test_add_field_to_string_str():
    result = _add_field_to_string(
        "NewField",
        "NewValue",
        "beginning",
    )
    assert result == "beginningNewField=NewValue;"


@pytest.mark.mssql
def test_add_field_to_string_secret():
    with MonkeyPatch.context() as mp:
        mp.setenv("NEW_FIELD_VALUE", "NewValue")
        result = _add_field_to_string(
            "NewField",
            td.EnvironmentSecret("NEW_FIELD_VALUE"),
            "beginning",
        )
        assert result == "beginningNewField=NewValue;"
