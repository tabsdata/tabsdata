#
# Copyright 2026 Tabs Data Inc.
#

"""
Unit tests for named_connection_utils.load_named_conn.

Tests cover:
- Environment variable validation
- File resolution and loading
- Wide array of connection types (MySQL, Postgres, MariaDB, Oracle, S3, GCS, MSSQL)
- Env-var substitution in connection configs
- Error cases (missing env var, missing file, name mismatch, unknown type)
"""

import json
import os

import pytest

from tabsdata._io.connections.file_connections.gcp_connection import GCSConn
from tabsdata._io.connections.file_connections.s3_connection import S3Conn
from tabsdata._io.connections.mariadb_connection import MariaDBConn
from tabsdata._io.connections.mysql_connection import MySQLConn
from tabsdata._io.connections.named_connection.named_connection import NamedConn
from tabsdata._io.connections.oracle_connection import OracleConn
from tabsdata._io.connections.postgres_connection import PostgresConn
from tabsdata._tabsserver.function.named_connection_utils import (
    CONNECTIONS_FOLDER,
    TD_URI_REPOSITORY,
    load_named_conn,
    resolve_named_conn_if_needed,
)
from tabsdata.connector.mssql.connection.mssql_connection import MSSQLConn

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_connection_json(repo_dir, name, cx_dict):
    """Write a connection JSON file to the test repository."""
    connections_dir = os.path.join(repo_dir, CONNECTIONS_FOLDER)
    os.makedirs(connections_dir, exist_ok=True)
    path = os.path.join(connections_dir, f"{name}.json")
    with open(path, "w") as f:
        json.dump(cx_dict, f)
    return path


# ---------------------------------------------------------------------------
# Environment variable validation
# ---------------------------------------------------------------------------


class TestLoadNamedConnEnvVar:
    def test_raises_when_env_var_not_set(self, monkeypatch):
        monkeypatch.delenv(TD_URI_REPOSITORY, raising=False)
        nc = NamedConn(name="my_conn")
        with pytest.raises(ValueError, match="TD_URI_REPOSITORY"):
            load_named_conn(nc)

    def test_raises_when_env_var_empty(self, monkeypatch):
        monkeypatch.setenv(TD_URI_REPOSITORY, "")
        nc = NamedConn(name="my_conn")
        with pytest.raises(ValueError, match="TD_URI_REPOSITORY"):
            load_named_conn(nc)


# ---------------------------------------------------------------------------
# File resolution errors
# ---------------------------------------------------------------------------


class TestLoadNamedConnFileErrors:
    def test_raises_when_file_not_found(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        nc = NamedConn(name="nonexistent_conn")
        with pytest.raises(FileNotFoundError, match="nonexistent_conn"):
            load_named_conn(nc)

    def test_raises_when_json_invalid(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        connections_dir = os.path.join(str(tmp_path), CONNECTIONS_FOLDER)
        os.makedirs(connections_dir)
        path = os.path.join(connections_dir, "bad_json.json")
        with open(path, "w") as f:
            f.write("not valid json {{{")
        nc = NamedConn(name="bad_json")
        with pytest.raises(json.JSONDecodeError):
            load_named_conn(nc)

    def test_raises_when_name_mismatch(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "wrong_name",
            "type": "MySQLConn",
            "conn_configs": {"uri": "mysql://localhost/db"},
        }
        _write_connection_json(str(tmp_path), "my_conn", cx_dict)
        nc = NamedConn(name="my_conn")
        with pytest.raises(ValueError, match="does not match"):
            load_named_conn(nc)

    def test_raises_when_unknown_type(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "my_conn",
            "type": "FakeConn",
            "conn_configs": {"uri": "fake://localhost"},
        }
        _write_connection_json(str(tmp_path), "my_conn", cx_dict)
        nc = NamedConn(name="my_conn")
        with pytest.raises(ValueError, match="Unknown connection type"):
            load_named_conn(nc)


# ---------------------------------------------------------------------------
# File lookup uses lowercase name
# ---------------------------------------------------------------------------


class TestLoadNamedConnLowercaseLookup:
    def test_uppercase_name_resolves_to_lowercase_file(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "My_Conn",
            "type": "MySQLConn",
            "conn_configs": {"uri": "mysql://localhost/mydb"},
        }
        # File is written with lowercase name
        _write_connection_json(str(tmp_path), "my_conn", cx_dict)
        conn = load_named_conn(NamedConn(name="My_Conn"))
        assert isinstance(conn, MySQLConn)

    def test_mixed_case_name_resolves_to_lowercase_file(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "MyDatabase",
            "type": "PostgresConn",
            "conn_configs": {"uri": "postgresql://localhost/mydb"},
        }
        _write_connection_json(str(tmp_path), "mydatabase", cx_dict)
        conn = load_named_conn(NamedConn(name="MyDatabase"))
        assert isinstance(conn, PostgresConn)


# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------


class TestLoadNamedConnMySQL:
    def test_plain(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "mysql_conn",
            "type": "MySQLConn",
            "conn_configs": {"uri": "mysql://localhost/mydb"},
        }
        _write_connection_json(str(tmp_path), "mysql_conn", cx_dict)
        conn = load_named_conn(NamedConn(name="mysql_conn"))
        assert isinstance(conn, MySQLConn)
        assert conn.uri == "mysql://localhost/mydb"

    def test_with_credentials(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "mysql_creds",
            "type": "MySQLConn",
            "conn_configs": {
                "uri": "mysql://localhost/mydb",
                "credentials": {"user": "admin", "password": "secret"},
            },
        }
        _write_connection_json(str(tmp_path), "mysql_creds", cx_dict)
        conn = load_named_conn(NamedConn(name="mysql_creds"))
        assert isinstance(conn, MySQLConn)
        assert conn.credentials is not None

    def test_with_env_credentials(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        monkeypatch.setenv("TEST_MYSQL_USER", "env_user")
        monkeypatch.setenv("TEST_MYSQL_PASS", "env_pass")
        cx_dict = {
            "name": "mysql_env",
            "type": "MySQLConn",
            "conn_configs": {
                "uri": "mysql://localhost/mydb",
                "credentials": {
                    "env:user": "TEST_MYSQL_USER",
                    "env:password": "TEST_MYSQL_PASS",
                },
            },
        }
        _write_connection_json(str(tmp_path), "mysql_env", cx_dict)
        conn = load_named_conn(NamedConn(name="mysql_env"))
        assert isinstance(conn, MySQLConn)
        assert conn.credentials is not None

    def test_with_env_uri(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        monkeypatch.setenv("TEST_MYSQL_URI", "mysql://envhost/envdb")
        cx_dict = {
            "name": "mysql_env_uri",
            "type": "MySQLConn",
            "conn_configs": {"env:uri": "TEST_MYSQL_URI"},
        }
        _write_connection_json(str(tmp_path), "mysql_env_uri", cx_dict)
        conn = load_named_conn(NamedConn(name="mysql_env_uri"))
        assert isinstance(conn, MySQLConn)
        assert conn.uri == "mysql://envhost/envdb"

    def test_missing_env_var_raises(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        monkeypatch.delenv("NONEXISTENT_VAR_12345", raising=False)
        cx_dict = {
            "name": "mysql_bad_env",
            "type": "MySQLConn",
            "conn_configs": {"env:uri": "NONEXISTENT_VAR_12345"},
        }
        _write_connection_json(str(tmp_path), "mysql_bad_env", cx_dict)
        with pytest.raises(ValueError, match="Environment variable"):
            load_named_conn(NamedConn(name="mysql_bad_env"))


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------


class TestLoadNamedConnPostgres:
    def test_plain(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "pg_conn",
            "type": "PostgresConn",
            "conn_configs": {"uri": "postgresql://localhost/mydb"},
        }
        _write_connection_json(str(tmp_path), "pg_conn", cx_dict)
        conn = load_named_conn(NamedConn(name="pg_conn"))
        assert isinstance(conn, PostgresConn)
        assert conn.uri == "postgresql://localhost/mydb"

    def test_with_credentials(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "pg_creds",
            "type": "PostgresConn",
            "conn_configs": {
                "uri": "postgresql://localhost/mydb",
                "credentials": {"user": "pgadmin", "password": "pgpass"},
            },
        }
        _write_connection_json(str(tmp_path), "pg_creds", cx_dict)
        conn = load_named_conn(NamedConn(name="pg_creds"))
        assert isinstance(conn, PostgresConn)
        assert conn.credentials is not None


# ---------------------------------------------------------------------------
# MariaDB
# ---------------------------------------------------------------------------


class TestLoadNamedConnMariaDB:
    def test_plain(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "maria_conn",
            "type": "MariaDBConn",
            "conn_configs": {"uri": "mariadb://localhost/mydb"},
        }
        _write_connection_json(str(tmp_path), "maria_conn", cx_dict)
        conn = load_named_conn(NamedConn(name="maria_conn"))
        assert isinstance(conn, MariaDBConn)
        assert conn.uri == "mariadb://localhost/mydb"

    def test_with_credentials(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "maria_creds",
            "type": "MariaDBConn",
            "conn_configs": {
                "uri": "mariadb://localhost/mydb",
                "credentials": {"user": "maria", "password": "mariapass"},
            },
        }
        _write_connection_json(str(tmp_path), "maria_creds", cx_dict)
        conn = load_named_conn(NamedConn(name="maria_creds"))
        assert isinstance(conn, MariaDBConn)
        assert conn.credentials is not None


# ---------------------------------------------------------------------------
# Oracle
# ---------------------------------------------------------------------------


class TestLoadNamedConnOracle:
    def test_plain(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "oracle_conn",
            "type": "OracleConn",
            "conn_configs": {"uri": "oracle://localhost:1521/orcl"},
        }
        _write_connection_json(str(tmp_path), "oracle_conn", cx_dict)
        conn = load_named_conn(NamedConn(name="oracle_conn"))
        assert isinstance(conn, OracleConn)
        assert conn.uri == "oracle://localhost:1521/orcl"

    def test_with_credentials(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "oracle_creds",
            "type": "OracleConn",
            "conn_configs": {
                "uri": "oracle://localhost:1521/orcl",
                "credentials": {"user": "sys", "password": "oraclepass"},
            },
        }
        _write_connection_json(str(tmp_path), "oracle_creds", cx_dict)
        conn = load_named_conn(NamedConn(name="oracle_creds"))
        assert isinstance(conn, OracleConn)
        assert conn.credentials is not None


# ---------------------------------------------------------------------------
# MSSQL
# ---------------------------------------------------------------------------


class TestLoadNamedConnMSSQL:
    def test_plain(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "mssql_conn",
            "type": "MSSQLConn",
            "conn_configs": {
                "connection_string_or_uri": "mssql://localhost/mydb",
            },
        }
        _write_connection_json(str(tmp_path), "mssql_conn", cx_dict)
        conn = load_named_conn(NamedConn(name="mssql_conn"))
        assert isinstance(conn, MSSQLConn)

    def test_with_credentials(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "mssql_creds",
            "type": "MSSQLConn",
            "conn_configs": {
                "connection_string_or_uri": "mssql://localhost/mydb",
                "credentials": {"user": "sa", "password": "mssqlpass"},
            },
        }
        _write_connection_json(str(tmp_path), "mssql_creds", cx_dict)
        conn = load_named_conn(NamedConn(name="mssql_creds"))
        assert isinstance(conn, MSSQLConn)
        assert conn.credentials is not None

    def test_with_env_credentials(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        monkeypatch.setenv("TEST_MSSQL_USER", "env_sa")
        monkeypatch.setenv("TEST_MSSQL_PASS", "env_mssqlpass")
        cx_dict = {
            "name": "mssql_env",
            "type": "MSSQLConn",
            "conn_configs": {
                "connection_string_or_uri": "mssql://localhost/mydb",
                "credentials": {
                    "env:user": "TEST_MSSQL_USER",
                    "env:password": "TEST_MSSQL_PASS",
                },
            },
        }
        _write_connection_json(str(tmp_path), "mssql_env", cx_dict)
        conn = load_named_conn(NamedConn(name="mssql_env"))
        assert isinstance(conn, MSSQLConn)
        assert conn.credentials is not None


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------


class TestLoadNamedConnS3:
    def test_plain(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "s3_conn",
            "type": "S3Conn",
            "conn_configs": {
                "bucket_uri": "s3://mybucket",
                "credentials": {
                    "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                    "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                },
            },
        }
        _write_connection_json(str(tmp_path), "s3_conn", cx_dict)
        conn = load_named_conn(NamedConn(name="s3_conn"))
        assert isinstance(conn, S3Conn)

    def test_with_region(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "s3_region",
            "type": "S3Conn",
            "conn_configs": {
                "bucket_uri": "s3://mybucket",
                "credentials": {
                    "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                    "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                },
                "region": "eu-west-1",
            },
        }
        _write_connection_json(str(tmp_path), "s3_region", cx_dict)
        conn = load_named_conn(NamedConn(name="s3_region"))
        assert isinstance(conn, S3Conn)
        assert conn.region == "eu-west-1"

    def test_with_env_credentials(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        monkeypatch.setenv("TEST_AWS_KEY", "AKIAIOSFODNN7EXAMPLE")
        monkeypatch.setenv(
            "TEST_AWS_SECRET", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        )
        cx_dict = {
            "name": "s3_env",
            "type": "S3Conn",
            "conn_configs": {
                "bucket_uri": "s3://mybucket",
                "credentials": {
                    "env:aws_access_key_id": "TEST_AWS_KEY",
                    "env:aws_secret_access_key": "TEST_AWS_SECRET",
                },
            },
        }
        _write_connection_json(str(tmp_path), "s3_env", cx_dict)
        conn = load_named_conn(NamedConn(name="s3_env"))
        assert isinstance(conn, S3Conn)


# ---------------------------------------------------------------------------
# GCS
# ---------------------------------------------------------------------------


class TestLoadNamedConnGCS:
    def test_plain(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "gcs_conn",
            "type": "GCSConn",
            "conn_configs": {
                "bucket_uri": "gs://mybucket",
                "credentials": {
                    "service_account_key": '{"type": "service_account"}',
                },
            },
        }
        _write_connection_json(str(tmp_path), "gcs_conn", cx_dict)
        conn = load_named_conn(NamedConn(name="gcs_conn"))
        assert isinstance(conn, GCSConn)

    def test_with_env_credentials(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        monkeypatch.setenv("TEST_GCS_KEY", '{"type": "service_account"}')
        cx_dict = {
            "name": "gcs_env",
            "type": "GCSConn",
            "conn_configs": {
                "bucket_uri": "gs://mybucket",
                "credentials": {
                    "env:service_account_key": "TEST_GCS_KEY",
                },
            },
        }
        _write_connection_json(str(tmp_path), "gcs_env", cx_dict)
        conn = load_named_conn(NamedConn(name="gcs_env"))
        assert isinstance(conn, GCSConn)


# ---------------------------------------------------------------------------
# resolve_named_conn_if_needed
# ---------------------------------------------------------------------------


class TestResolveNamedConnIfNeeded:
    def test_resolves_named_conn_on_source_plugin(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "my_mysql",
            "type": "MySQLConn",
            "conn_configs": {"uri": "mysql://localhost/mydb"},
        }
        _write_connection_json(str(tmp_path), "my_mysql", cx_dict)

        plugin = type("FakeSource", (), {"conn": NamedConn(name="my_mysql")})()
        resolve_named_conn_if_needed(plugin)
        assert isinstance(plugin.conn, MySQLConn)
        assert plugin.conn.uri == "mysql://localhost/mydb"

    def test_resolves_named_conn_on_destination_plugin(self, monkeypatch, tmp_path):
        monkeypatch.setenv(TD_URI_REPOSITORY, str(tmp_path))
        cx_dict = {
            "name": "my_pg",
            "type": "PostgresConn",
            "conn_configs": {"uri": "postgresql://localhost/mydb"},
        }
        _write_connection_json(str(tmp_path), "my_pg", cx_dict)

        plugin = type("FakeDest", (), {"conn": NamedConn(name="my_pg")})()
        resolve_named_conn_if_needed(plugin)
        assert isinstance(plugin.conn, PostgresConn)

    def test_noop_when_conn_is_already_concrete(self):
        original_conn = MySQLConn(uri="mysql://localhost/mydb")
        plugin = type("FakePlugin", (), {"conn": original_conn})()
        resolve_named_conn_if_needed(plugin)
        assert plugin.conn is original_conn

    def test_noop_when_plugin_has_no_conn(self):
        plugin = type("FakeTableOutput", (), {})()
        resolve_named_conn_if_needed(plugin)  # should not raise
        assert not hasattr(plugin, "conn")

    def test_noop_when_conn_is_none(self):
        plugin = type("FakePlugin", (), {"conn": None})()
        resolve_named_conn_if_needed(plugin)
        assert plugin.conn is None
