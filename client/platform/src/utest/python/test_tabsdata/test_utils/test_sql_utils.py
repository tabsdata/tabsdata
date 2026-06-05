#
# Copyright 2025 Tabs Data Inc.
#

import polars as pl
import pytest

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
from tabsdata._utils.sql_utils import get_tableframes_from_sources, obtain_uri

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


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


# ── get_tableframes_from_sources ─────────────────────────────────────────────


def _write_parquet(tmp_path, name: str, ids: list[int], names: list[str]) -> str:
    """Write a small parquet chunk and return its path as a string."""
    path = tmp_path / name
    pl.DataFrame({"id": ids, "name": names}).write_parquet(path)
    return str(path)


def _user_rows(tf) -> list[dict]:
    """Rows of a TableFrame as dicts, with tabsdata system ($td.*) columns
    stripped."""
    pdf = tf.to_pandas()
    keep = [c for c in pdf.columns if not c.startswith("$td.")]
    return pdf[keep].to_dict(orient="records")


def test_get_tableframes_empty_sources_returns_empty_list():
    """No sources → the loop never runs → empty list returned."""
    assert get_tableframes_from_sources([]) == []


def test_get_tableframes_single_source_single_file(tmp_path):
    f = _write_parquet(tmp_path, "q0.parquet", [1, 2], ["a", "b"])
    result = get_tableframes_from_sources([[f]])
    assert len(result) == 1
    assert _user_rows(result[0]) == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
    ]


def test_get_tableframes_single_source_multiple_chunks_concatenated(tmp_path):
    """Multiple parquet chunks within one source are concatenated into one
    TableFrame (the pl.concat path)."""
    f0 = _write_parquet(tmp_path, "q0_c0.parquet", [1], ["a"])
    f1 = _write_parquet(tmp_path, "q0_c1.parquet", [2], ["b"])
    result = get_tableframes_from_sources([[f0, f1]])
    assert len(result) == 1
    rows = _user_rows(result[0])
    assert sorted(r["id"] for r in rows) == [1, 2]


def test_get_tableframes_multiple_sources_yield_multiple_frames(tmp_path):
    f0 = _write_parquet(tmp_path, "q0.parquet", [1], ["a"])
    f1 = _write_parquet(tmp_path, "q1.parquet", [9], ["z"])
    result = get_tableframes_from_sources([[f0], [f1]])
    assert len(result) == 2
    assert _user_rows(result[0]) == [{"id": 1, "name": "a"}]
    assert _user_rows(result[1]) == [{"id": 9, "name": "z"}]


def test_get_tableframes_non_string_paths_are_filtered_out(tmp_path):
    """Non-str entries in a source are skipped by the `isinstance(f, str)`
    filter; only the real parquet path is scanned."""
    f = _write_parquet(tmp_path, "q0.parquet", [1], ["a"])
    result = get_tableframes_from_sources([[f, 123, None]])
    assert len(result) == 1
    assert _user_rows(result[0]) == [{"id": 1, "name": "a"}]


def test_get_tableframes_non_list_source_raises_type_error(tmp_path):
    """A source that isn't a list hits the else branch → TypeError."""
    f = _write_parquet(tmp_path, "q0.parquet", [1], ["a"])
    with pytest.raises(TypeError, match="must be a list"):
        get_tableframes_from_sources([[f], "not-a-list"])


def test_get_tableframes_empty_source_appends_none():
    """An empty source (zero parquet chunks — e.g. an empty query result)
    yields no intermediate lazyframes → None is appended for that slot."""
    assert get_tableframes_from_sources([[]]) == [None]


def test_get_tableframes_all_non_string_source_appends_none():
    """A source whose every element is filtered out by isinstance(f, str)
    yields an empty intermediate list → None appended, same as an empty
    source."""
    assert get_tableframes_from_sources([[123, None]]) == [None]


def test_get_tableframes_mixed_empty_and_valid_sources(tmp_path):
    """A None slot and a real TableFrame slot coexist, position-preserved."""
    f = _write_parquet(tmp_path, "q1.parquet", [7], ["g"])
    result = get_tableframes_from_sources([[], [f]])
    assert len(result) == 2
    assert result[0] is None
    assert _user_rows(result[1]) == [{"id": 7, "name": "g"}]
