#
# Copyright 2026 Tabsdata Inc.
#

import json
from pathlib import Path

import pytest

from tabsdata._credentials import (
    AzureAccountKeyCredentials,
    GCPServiceAccountKeyCredentials,
    HMACCredentials,
    S3AccessKeyCredentials,
    UserPasswordCredentials,
)
from tabsdata._io.cloud_staging.common.storage_config import (
    AzureConfig,
    GCSConfig,
    S3Config,
)
from tabsdata._io.connections.clickhouse_connection import ClickHouseConn
from tabsdata._io.connections.connections import Conn
from tabsdata._io.connections.db2_connection import Db2Conn
from tabsdata._io.connections.druid_connection import DruidConn
from tabsdata.connector.bigquery._connector import BigQueryConn
from tabsdata.connector.cdc.db2._connection import Db2CdcConn
from tabsdata.connector.cdc.mysql._connection import MySQLCdcConn
from tabsdata.connector.cdc.postgres._connection import PostgresCdcConn
from tabsdata.connector.kafka._connector import (
    AwsGlueSchemaRegistry,
    ConfluentSchemaRegistry,
    KafkaConn,
    build_schema_registry_from_dict,
)
from tabsdata._io.connections.file_connections.azure_connection import AzureConn
from tabsdata._io.connections.file_connections.gcp_connection import GCSConn
from tabsdata._io.connections.file_connections.s3_connection import S3Conn
from tabsdata._io.inputs.file.azure_input import AzureSrc
from tabsdata._io.inputs.file.gcs_input import GCSSrc
from tabsdata._io.inputs.file.s3_input import S3Src
from tabsdata._io.outputs.file.azure_output import AzureDest
from tabsdata._io.outputs.file.gcs_output import GCSDest
from tabsdata._io.outputs.file.s3_output import S3Dest
from tabsdata._io.connections.mariadb_connection import MariaDBConn
from tabsdata._io.connections.mysql_connection import MySQLConn
from tabsdata._io.connections.named_connection.named_connection import NamedConn
from tabsdata._io.connections.oracle_connection import OracleConn
from tabsdata._io.connections.postgres_connection import PostgresConn
from tabsdata._io.connections.starrocks_connection import StarRocksConn
from tabsdata.connector.mssql.connection.mssql_connection import MSSQLConn

# --- NamedConn basic tests ---


@pytest.mark.unit
def test_named_conn_name():
    nc = NamedConn(name="my_conn")
    assert nc.name == "my_conn"


@pytest.mark.unit
def test_named_conn_is_conn():
    nc = NamedConn(name="my_conn")
    assert isinstance(nc, Conn)


# --- _load label validation ---


@pytest.mark.unit
def test_load_label_mismatch():
    nc = NamedConn(name="my_conn")
    cx_dict = {
        "kind": "mysqlSrcConn",
        "apiVersion": "1.0",
        "name": "wrong_name",
        "conn_configs": {"uri": "mysql://localhost/db"},
    }
    with pytest.raises(ValueError, match="does not match"):
        nc._load(cx_dict)


@pytest.mark.unit
def test_load_unknown_type():
    nc = NamedConn(name="my_conn")
    cx_dict = {
        "kind": "nonexistentSrcConn",
        "apiVersion": "1.0",
        "name": "my_conn",
        "conn_configs": {},
    }
    with pytest.raises(ValueError, match="Unknown connection kind"):
        nc._load(cx_dict)


# --- _load with MySQLConn ---


@pytest.mark.unit
def test_load_mysql_conn_plain():
    nc = NamedConn(name="mysql_conn")
    cx_dict = {
        "kind": "mysqlSrcConn",
        "apiVersion": "1.0",
        "name": "mysql_conn",
        "conn_configs": {
            "uri": "mysql://localhost/mydb",
        },
    }
    conn = nc._load(cx_dict)
    assert isinstance(conn, MySQLConn)
    assert conn.uri == "mysql://localhost/mydb"
    assert conn.credentials is None


@pytest.mark.unit
def test_load_mysql_conn_with_credentials():
    nc = NamedConn(name="mysql_conn_1")
    cx_dict = {
        "kind": "mysqlSrcConn",
        "apiVersion": "1.0",
        "name": "mysql_conn_1",
        "conn_configs": {
            "uri": "mysql://localhost/mydb",
            "credentials": {
                "user": "admin",
                "password": "secret",
            },
        },
    }
    conn = nc._load(cx_dict)
    assert isinstance(conn, MySQLConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


@pytest.mark.unit
def test_load_mysql_conn_with_env_credentials(monkeypatch):
    monkeypatch.setenv("TEST_MYSQL_USER", "env_admin")
    monkeypatch.setenv("TEST_MYSQL_PASSWORD", "env_secret")
    nc = NamedConn(name="mysql_conn_2")
    cx_dict = {
        "kind": "mysqlSrcConn",
        "apiVersion": "1.0",
        "name": "mysql_conn_2",
        "conn_configs": {
            "uri": "mysql://localhost/mydb",
            "credentials": {
                "env:user": "TEST_MYSQL_USER",
                "env:password": "TEST_MYSQL_PASSWORD",
            },
        },
    }
    conn = nc._load(cx_dict)
    assert isinstance(conn, MySQLConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)
    assert conn.credentials.user.secret_value == "env_admin"
    assert conn.credentials.password.secret_value == "env_secret"


@pytest.mark.unit
def test_load_mysql_conn_with_env_uri(monkeypatch):
    monkeypatch.setenv("TEST_MYSQL_URI", "mysql://envhost/envdb")
    nc = NamedConn(name="mysql_conn_3")
    cx_dict = {
        "kind": "mysqlSrcConn",
        "apiVersion": "1.0",
        "name": "mysql_conn_3",
        "conn_configs": {
            "env:uri": "TEST_MYSQL_URI",
        },
    }
    conn = nc._load(cx_dict)
    assert isinstance(conn, MySQLConn)
    assert conn.uri == "mysql://envhost/envdb"


@pytest.mark.unit
def test_load_mysql_conn_all_env_vars(monkeypatch):
    monkeypatch.setenv("TEST_MYSQL_URI_ALL", "mysql://envhost/envdb")
    monkeypatch.setenv("TEST_MYSQL_USER_ALL", "env_admin")
    monkeypatch.setenv("TEST_MYSQL_PASSWORD_ALL", "env_secret")
    monkeypatch.setenv("TEST_ENFORCE_CONN_PARAMS_ALL", "true")
    monkeypatch.setenv("TEST_CX_SRC_TIMEOUT", "30")
    nc = NamedConn(name="mysql_conn_all_env")
    cx_dict = {
        "kind": "mysqlSrcConn",
        "apiVersion": "1.0",
        "name": "mysql_conn_all_env",
        "conn_configs": {
            "env:uri": "TEST_MYSQL_URI_ALL",
            "credentials": {
                "env:user": "TEST_MYSQL_USER_ALL",
                "env:password": "TEST_MYSQL_PASSWORD_ALL",
            },
            "env:enforce_connection_params": "TEST_ENFORCE_CONN_PARAMS_ALL",
            "cx_src_configs_mysql": {
                "env:timeout": "TEST_CX_SRC_TIMEOUT",
            },
        },
    }
    conn = nc._load(cx_dict)
    assert isinstance(conn, MySQLConn)
    assert conn.uri == "mysql://envhost/envdb"
    assert isinstance(conn.credentials, UserPasswordCredentials)
    assert conn.credentials.user.secret_value == "env_admin"
    assert conn.credentials.password.secret_value == "env_secret"
    assert conn.enforce_connection_params is True
    assert conn.cx_src_configs_mysql["timeout"] == "30"


@pytest.mark.unit
def test_load_mysql_conn_missing_env_var():
    nc = NamedConn(name="mysql_conn_4")
    cx_dict = {
        "kind": "mysqlSrcConn",
        "apiVersion": "1.0",
        "name": "mysql_conn_4",
        "conn_configs": {
            "env:uri": "NONEXISTENT_ENV_VAR_12345",
        },
    }
    with pytest.raises(ValueError, match="Environment variable"):
        nc._load(cx_dict)


# --- _load with PostgresConn ---


@pytest.mark.unit
def test_load_postgres_conn_plain():
    nc = NamedConn(name="pg_conn")
    cx_dict = {
        "kind": "postgresSrcConn",
        "apiVersion": "1.0",
        "name": "pg_conn",
        "conn_configs": {
            "uri": "postgresql://localhost/mydb",
        },
    }
    conn = nc._load(cx_dict)
    assert isinstance(conn, PostgresConn)
    assert conn.uri == "postgresql://localhost/mydb"


@pytest.mark.unit
def test_load_postgres_conn_with_credentials():
    nc = NamedConn(name="pg_conn_1")
    cx_dict = {
        "kind": "postgresSrcConn",
        "apiVersion": "1.0",
        "name": "pg_conn_1",
        "conn_configs": {
            "uri": "postgresql://localhost/mydb",
            "credentials": {
                "user": "pgadmin",
                "password": "pgpass",
            },
        },
    }
    conn = nc._load(cx_dict)
    assert isinstance(conn, PostgresConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


@pytest.mark.unit
def test_load_postgres_conn_with_env_credentials(monkeypatch):
    monkeypatch.setenv("TEST_PG_USER", "env_pgadmin")
    monkeypatch.setenv("TEST_PG_PASSWORD", "env_pgpass")
    nc = NamedConn(name="pg_conn_2")
    cx_dict = {
        "kind": "postgresSrcConn",
        "apiVersion": "1.0",
        "name": "pg_conn_2",
        "conn_configs": {
            "uri": "postgresql://localhost/mydb",
            "credentials": {
                "env:user": "TEST_PG_USER",
                "env:password": "TEST_PG_PASSWORD",
            },
        },
    }
    conn = nc._load(cx_dict)
    assert isinstance(conn, PostgresConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


# --- _load with MSSQLConn ---


@pytest.mark.unit
def test_load_mssql_conn_plain():
    nc = NamedConn(name="mssql_conn")
    cx_dict = {
        "kind": "mssqlSrcConn",
        "apiVersion": "1.0",
        "name": "mssql_conn",
        "conn_configs": {
            "connection_string_or_uri": "mssql://localhost/mydb",
        },
    }
    conn = nc._load(cx_dict)
    assert isinstance(conn, MSSQLConn)
    assert conn.connection_string_or_uri == "mssql://localhost/mydb"


@pytest.mark.unit
def test_load_mssql_conn_with_credentials():
    nc = NamedConn(name="mssql_conn_1")
    cx_dict = {
        "kind": "mssqlSrcConn",
        "apiVersion": "1.0",
        "name": "mssql_conn_1",
        "conn_configs": {
            "connection_string_or_uri": "mssql://localhost/mydb",
            "credentials": {
                "user": "sa",
                "password": "mssqlpass",
            },
        },
    }
    conn = nc._load(cx_dict)
    assert isinstance(conn, MSSQLConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


@pytest.mark.unit
def test_load_mssql_conn_with_env_credentials(monkeypatch):
    monkeypatch.setenv("TEST_MSSQL_USER", "env_sa")
    monkeypatch.setenv("TEST_MSSQL_PASSWORD", "env_mssqlpass")
    nc = NamedConn(name="mssql_conn_2")
    cx_dict = {
        "kind": "mssqlSrcConn",
        "apiVersion": "1.0",
        "name": "mssql_conn_2",
        "conn_configs": {
            "connection_string_or_uri": "mssql://localhost/mydb",
            "credentials": {
                "env:user": "TEST_MSSQL_USER",
                "env:password": "TEST_MSSQL_PASSWORD",
            },
        },
    }
    conn = nc._load(cx_dict)
    assert isinstance(conn, MSSQLConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


# --- _load with S3Conn ---


@pytest.mark.unit
def test_load_s3_conn_plain():
    nc = NamedConn(name="s3_conn")
    cx_dict = {
        "kind": "s3SrcConn",
        "apiVersion": "1.0",
        "name": "s3_conn",
        "conn_configs": {
            "bucket_uri": "s3://mybucket",
            "credentials": {
                "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            },
        },
    }
    conn = nc._load(cx_dict)
    assert isinstance(conn, S3Conn)
    assert conn.bucket_uri == "s3://mybucket"
    assert isinstance(conn.credentials, S3AccessKeyCredentials)


@pytest.mark.unit
def test_load_s3_conn_with_env_credentials(monkeypatch):
    monkeypatch.setenv("TEST_AWS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
    monkeypatch.setenv(
        "TEST_AWS_SECRET_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    )
    nc = NamedConn(name="s3_conn_1")
    cx_dict = {
        "kind": "s3SrcConn",
        "apiVersion": "1.0",
        "name": "s3_conn_1",
        "conn_configs": {
            "bucket_uri": "s3://mybucket",
            "credentials": {
                "env:aws_access_key_id": "TEST_AWS_KEY_ID",
                "env:aws_secret_access_key": "TEST_AWS_SECRET_KEY",
            },
        },
    }
    conn = nc._load(cx_dict)
    assert isinstance(conn, S3Conn)
    assert isinstance(conn.credentials, S3AccessKeyCredentials)
    assert conn.credentials.aws_access_key_id.secret_value == "AKIAIOSFODNN7EXAMPLE"
    assert (
        conn.credentials.aws_secret_access_key.secret_value
        == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    )


@pytest.mark.unit
def test_load_s3_conn_with_region():
    nc = NamedConn(name="s3_conn_2")
    cx_dict = {
        "kind": "s3SrcConn",
        "apiVersion": "1.0",
        "name": "s3_conn_2",
        "conn_configs": {
            "bucket_uri": "s3://mybucket",
            "credentials": {
                "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            },
            "region": "us-east-1",
        },
    }
    conn = nc._load(cx_dict)
    assert isinstance(conn, S3Conn)
    assert conn.region == "us-east-1"


# --- _load with AzureConn ---


@pytest.mark.unit
def test_load_azure_conn_all_env_vars(monkeypatch):
    monkeypatch.setenv("TEST_AZURE_CONTAINER_URI", "az://envcontainer")
    monkeypatch.setenv("TEST_AZURE_ACCOUNT_NAME", "env_account")
    monkeypatch.setenv("TEST_AZURE_ACCOUNT_KEY", "env_key")
    nc = NamedConn(name="azure_conn_all_env")
    cx_dict = {
        "kind": "azureSrcConn",
        "apiVersion": "1.0",
        "name": "azure_conn_all_env",
        "conn_configs": {
            "env:container_uri": "TEST_AZURE_CONTAINER_URI",
            "credentials": {
                "env:account_name": "TEST_AZURE_ACCOUNT_NAME",
                "env:account_key": "TEST_AZURE_ACCOUNT_KEY",
            },
        },
    }
    conn = nc._load(cx_dict)
    assert isinstance(conn, AzureConn)
    assert conn.container_uri == "az://envcontainer"
    assert isinstance(conn.credentials, AzureAccountKeyCredentials)
    assert conn.credentials.account_name.secret_value == "env_account"
    assert conn.credentials.account_key.secret_value == "env_key"


@pytest.mark.unit
def test_azuresrc_with_named_conn_replaced_by_resolved_azure_conn(monkeypatch):
    monkeypatch.setenv("TEST_AZURESRC_CONTAINER_URI", "az://envcontainer")
    monkeypatch.setenv("TEST_AZURESRC_ACCOUNT_NAME", "env_account")
    monkeypatch.setenv("TEST_AZURESRC_ACCOUNT_KEY", "env_key")

    nc = NamedConn(name="azure_src_conn")
    src = AzureSrc(conn=nc, path="data/input.parquet")
    assert src.conn is nc

    cx_dict = {
        "kind": "azureSrcConn",
        "apiVersion": "1.0",
        "name": "azure_src_conn",
        "conn_configs": {
            "env:container_uri": "TEST_AZURESRC_CONTAINER_URI",
            "credentials": {
                "env:account_name": "TEST_AZURESRC_ACCOUNT_NAME",
                "env:account_key": "TEST_AZURESRC_ACCOUNT_KEY",
            },
        },
    }
    resolved = nc._load(cx_dict)
    src.conn = resolved

    assert isinstance(src.conn, AzureConn)
    assert src.conn.container_uri == "az://envcontainer"
    assert isinstance(src.conn.credentials, AzureAccountKeyCredentials)
    assert src.conn.credentials.account_name.secret_value == "env_account"
    assert src.conn.credentials.account_key.secret_value == "env_key"


@pytest.mark.unit
def test_azuredest_with_named_conn_replaced_by_resolved_azure_conn(monkeypatch):
    monkeypatch.setenv("TEST_AZUREDEST_CONTAINER_URI", "az://envcontainer")
    monkeypatch.setenv("TEST_AZUREDEST_ACCOUNT_NAME", "env_account")
    monkeypatch.setenv("TEST_AZUREDEST_ACCOUNT_KEY", "env_key")

    nc = NamedConn(name="azure_dest_conn")
    dest = AzureDest(conn=nc, path="data/output.parquet")
    assert dest.conn is nc

    cx_dict = {
        "kind": "azureDestConn",
        "apiVersion": "1.0",
        "name": "azure_dest_conn",
        "conn_configs": {
            "env:container_uri": "TEST_AZUREDEST_CONTAINER_URI",
            "credentials": {
                "env:account_name": "TEST_AZUREDEST_ACCOUNT_NAME",
                "env:account_key": "TEST_AZUREDEST_ACCOUNT_KEY",
            },
        },
    }
    resolved = nc._load(cx_dict)
    dest.conn = resolved

    assert isinstance(dest.conn, AzureConn)
    assert dest.conn.container_uri == "az://envcontainer"
    assert isinstance(dest.conn.credentials, AzureAccountKeyCredentials)
    assert dest.conn.credentials.account_name.secret_value == "env_account"
    assert dest.conn.credentials.account_key.secret_value == "env_key"
    assert dest._full_uri == ["az://envcontainer/data/output.parquet"]


@pytest.mark.unit
def test_s3src_with_named_conn_replaced_by_resolved_s3_conn(monkeypatch):
    monkeypatch.setenv("TEST_S3SRC_BUCKET_URI", "s3://envbucket")
    monkeypatch.setenv("TEST_S3SRC_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
    monkeypatch.setenv(
        "TEST_S3SRC_SECRET_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    )

    nc = NamedConn(name="s3_src_conn")
    src = S3Src(conn=nc, path="data/input.parquet")
    assert src.conn is nc

    cx_dict = {
        "kind": "s3SrcConn",
        "apiVersion": "1.0",
        "name": "s3_src_conn",
        "conn_configs": {
            "env:bucket_uri": "TEST_S3SRC_BUCKET_URI",
            "credentials": {
                "env:aws_access_key_id": "TEST_S3SRC_KEY_ID",
                "env:aws_secret_access_key": "TEST_S3SRC_SECRET_KEY",
            },
        },
    }
    resolved = nc._load(cx_dict)
    src.conn = resolved

    assert isinstance(src.conn, S3Conn)
    assert src.conn.bucket_uri == "s3://envbucket"
    assert isinstance(src.conn.credentials, S3AccessKeyCredentials)
    assert src.conn.credentials.aws_access_key_id.secret_value == "AKIAIOSFODNN7EXAMPLE"
    assert (
        src.conn.credentials.aws_secret_access_key.secret_value
        == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    )
    assert src._uri_list == ["s3://envbucket/data/input.parquet"]


@pytest.mark.unit
def test_s3dest_with_named_conn_replaced_by_resolved_s3_conn(monkeypatch):
    monkeypatch.setenv("TEST_S3DEST_BUCKET_URI", "s3://envbucket")
    monkeypatch.setenv("TEST_S3DEST_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
    monkeypatch.setenv(
        "TEST_S3DEST_SECRET_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    )

    nc = NamedConn(name="s3_dest_conn")
    dest = S3Dest(conn=nc, path="data/output.parquet")
    assert dest.conn is nc

    cx_dict = {
        "kind": "s3DestConn",
        "apiVersion": "1.0",
        "name": "s3_dest_conn",
        "conn_configs": {
            "env:bucket_uri": "TEST_S3DEST_BUCKET_URI",
            "credentials": {
                "env:aws_access_key_id": "TEST_S3DEST_KEY_ID",
                "env:aws_secret_access_key": "TEST_S3DEST_SECRET_KEY",
            },
        },
    }
    resolved = nc._load(cx_dict)
    dest.conn = resolved

    assert isinstance(dest.conn, S3Conn)
    assert dest.conn.bucket_uri == "s3://envbucket"
    assert isinstance(dest.conn.credentials, S3AccessKeyCredentials)
    assert (
        dest.conn.credentials.aws_access_key_id.secret_value == "AKIAIOSFODNN7EXAMPLE"
    )
    assert (
        dest.conn.credentials.aws_secret_access_key.secret_value
        == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    )
    assert dest._full_uri == ["s3://envbucket/data/output.parquet"]


@pytest.mark.unit
def test_gcssrc_with_named_conn_replaced_by_resolved_gcs_conn(monkeypatch):
    monkeypatch.setenv("TEST_GCSSRC_BUCKET_URI", "gs://envbucket")
    monkeypatch.setenv("TEST_GCSSRC_SERVICE_ACCOUNT_KEY", "env_key_content")

    nc = NamedConn(name="gcs_src_conn")
    src = GCSSrc(conn=nc, path="data/input.parquet")
    assert src.conn is nc

    cx_dict = {
        "kind": "gcsSrcConn",
        "apiVersion": "1.0",
        "name": "gcs_src_conn",
        "conn_configs": {
            "env:bucket_uri": "TEST_GCSSRC_BUCKET_URI",
            "credentials": {
                "env:service_account_key": "TEST_GCSSRC_SERVICE_ACCOUNT_KEY",
            },
        },
    }
    resolved = nc._load(cx_dict)
    src.conn = resolved

    assert isinstance(src.conn, GCSConn)
    assert src.conn.bucket_uri == "gs://envbucket"
    assert isinstance(src.conn.credentials, GCPServiceAccountKeyCredentials)
    assert src.conn.credentials.service_account_key.secret_value == "env_key_content"
    assert src._uri_list == ["gs://envbucket/data/input.parquet"]


@pytest.mark.unit
def test_gcsdest_with_named_conn_replaced_by_resolved_gcs_conn(monkeypatch):
    monkeypatch.setenv("TEST_GCSDEST_BUCKET_URI", "gs://envbucket")
    monkeypatch.setenv("TEST_GCSDEST_SERVICE_ACCOUNT_KEY", "env_key_content")

    nc = NamedConn(name="gcs_dest_conn")
    dest = GCSDest(conn=nc, path="data/output.parquet")
    assert dest.conn is nc

    cx_dict = {
        "kind": "gcsDestConn",
        "apiVersion": "1.0",
        "name": "gcs_dest_conn",
        "conn_configs": {
            "env:bucket_uri": "TEST_GCSDEST_BUCKET_URI",
            "credentials": {
                "env:service_account_key": "TEST_GCSDEST_SERVICE_ACCOUNT_KEY",
            },
        },
    }
    resolved = nc._load(cx_dict)
    dest.conn = resolved

    assert isinstance(dest.conn, GCSConn)
    assert dest.conn.bucket_uri == "gs://envbucket"
    assert isinstance(dest.conn.credentials, GCPServiceAccountKeyCredentials)
    assert (
        dest.conn.credentials.service_account_key.secret_value == "env_key_content"
    )
    assert dest._full_uri == ["gs://envbucket/data/output.parquet"]


# --- env var resolution edge cases ---


@pytest.mark.unit
def test_load_missing_nested_env_var():
    nc = NamedConn(name="mysql_conn_5")
    cx_dict = {
        "kind": "mysqlSrcConn",
        "apiVersion": "1.0",
        "name": "mysql_conn_5",
        "conn_configs": {
            "uri": "mysql://localhost/mydb",
            "credentials": {
                "env:user": "NONEXISTENT_USER_VAR_12345",
                "env:password": "NONEXISTENT_PASS_VAR_12345",
            },
        },
    }
    with pytest.raises(ValueError, match="Environment variable"):
        nc._load(cx_dict)


@pytest.mark.unit
def test_load_empty_conn_configs():
    nc = NamedConn(name="mysql_conn_6")
    cx_dict = {
        "kind": "mysqlSrcConn",
        "apiVersion": "1.0",
        "name": "mysql_conn_6",
        "conn_configs": {},
    }
    # MySQLConn requires uri, so this should fail at construction
    with pytest.raises(Exception):
        nc._load(cx_dict)


# =============================================================================
# _load_from_dict tests
# =============================================================================


# --- MySQLConn._load_from_dict ---


@pytest.mark.unit
def test_mysql_conn_load_from_dict_plain():
    cx_dict = {
        "kind": "mysqlSrcConn",
        "apiVersion": "1.0",
        "name": "mysql_conn_7",
        "conn_configs": {"uri": "mysql://localhost/mydb"},
    }
    conn = MySQLConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, MySQLConn)
    assert conn.uri == "mysql://localhost/mydb"
    assert conn.credentials is None


@pytest.mark.unit
def test_mysql_conn_load_from_dict_with_credentials():
    cx_dict = {
        "kind": "mysqlSrcConn",
        "apiVersion": "1.0",
        "name": "mysql_conn_8",
        "conn_configs": {
            "uri": "mysql://localhost/mydb",
            "credentials": {"user": "admin", "password": "secret"},
        },
    }
    conn = MySQLConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, MySQLConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


# --- MariaDBConn._load_from_dict ---


@pytest.mark.unit
def test_mariadb_conn_load_from_dict_plain():
    cx_dict = {
        "kind": "mariadbSrcConn",
        "apiVersion": "1.0",
        "name": "mariadb_conn",
        "conn_configs": {"uri": "mariadb://localhost/mydb"},
    }
    conn = MariaDBConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, MariaDBConn)
    assert conn.uri == "mariadb://localhost/mydb"
    assert conn.credentials is None


@pytest.mark.unit
def test_mariadb_conn_load_from_dict_with_credentials():
    cx_dict = {
        "kind": "mariadbSrcConn",
        "apiVersion": "1.0",
        "name": "mariadb_conn_1",
        "conn_configs": {
            "uri": "mariadb://localhost/mydb",
            "credentials": {"user": "admin", "password": "secret"},
        },
    }
    conn = MariaDBConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, MariaDBConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


# --- PostgresConn._load_from_dict ---


@pytest.mark.unit
def test_postgres_conn_load_from_dict_plain():
    cx_dict = {
        "kind": "postgresSrcConn",
        "apiVersion": "1.0",
        "name": "pg_conn_3",
        "conn_configs": {"uri": "postgresql://localhost/mydb"},
    }
    conn = PostgresConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, PostgresConn)
    assert conn.uri == "postgresql://localhost/mydb"
    assert conn.credentials is None


@pytest.mark.unit
def test_postgres_conn_load_from_dict_with_credentials():
    cx_dict = {
        "kind": "postgresSrcConn",
        "apiVersion": "1.0",
        "name": "pg_conn_4",
        "conn_configs": {
            "uri": "postgresql://localhost/mydb",
            "credentials": {"user": "pgadmin", "password": "pgpass"},
        },
    }
    conn = PostgresConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, PostgresConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


# --- OracleConn._load_from_dict ---


@pytest.mark.unit
def test_oracle_conn_load_from_dict_plain():
    cx_dict = {
        "kind": "oracleSrcConn",
        "apiVersion": "1.0",
        "name": "oracle_conn",
        "conn_configs": {"uri": "oracle://localhost/mydb"},
    }
    conn = OracleConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, OracleConn)
    assert conn.uri == "oracle://localhost/mydb"
    assert conn.credentials is None


@pytest.mark.unit
def test_oracle_conn_load_from_dict_with_credentials():
    cx_dict = {
        "kind": "oracleSrcConn",
        "apiVersion": "1.0",
        "name": "oracle_conn_1",
        "conn_configs": {
            "uri": "oracle://localhost/mydb",
            "credentials": {"user": "oraadmin", "password": "orapass"},
        },
    }
    conn = OracleConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, OracleConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


# --- MSSQLConn._load_from_dict ---


@pytest.mark.unit
def test_mssql_conn_load_from_dict_plain():
    cx_dict = {
        "kind": "mssqlSrcConn",
        "apiVersion": "1.0",
        "name": "mssql_conn_3",
        "conn_configs": {
            "connection_string_or_uri": "mssql://localhost/mydb",
        },
    }
    conn = MSSQLConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, MSSQLConn)
    assert conn.connection_string_or_uri == "mssql://localhost/mydb"
    assert conn.credentials is None


@pytest.mark.unit
def test_mssql_conn_load_from_dict_with_credentials():
    cx_dict = {
        "kind": "mssqlSrcConn",
        "apiVersion": "1.0",
        "name": "mssql_conn_4",
        "conn_configs": {
            "connection_string_or_uri": "mssql://localhost/mydb",
            "credentials": {"user": "sa", "password": "mssqlpass"},
        },
    }
    conn = MSSQLConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, MSSQLConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


# --- Db2Conn._load_from_dict ---


@pytest.mark.unit
def test_db2_conn_load_from_dict_plain():
    cx_dict = {
        "kind": "db2SrcConn",
        "apiVersion": "1.0",
        "name": "db2_conn",
        "conn_configs": {"uri": "db2://localhost/mydb"},
    }
    conn = Db2Conn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, Db2Conn)
    assert conn.uri == "db2://localhost/mydb"
    assert conn.credentials is None


@pytest.mark.unit
def test_db2_conn_load_from_dict_with_credentials():
    cx_dict = {
        "kind": "db2SrcConn",
        "apiVersion": "1.0",
        "name": "db2_conn_1",
        "conn_configs": {
            "uri": "db2://localhost/mydb",
            "credentials": {"user": "db2admin", "password": "db2pass"},
        },
    }
    conn = Db2Conn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, Db2Conn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


# --- S3Conn._load_from_dict ---


@pytest.mark.unit
def test_s3_conn_load_from_dict():
    cx_dict = {
        "kind": "s3SrcConn",
        "apiVersion": "1.0",
        "name": "s3_conn_3",
        "conn_configs": {
            "bucket_uri": "s3://mybucket",
            "credentials": {
                "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            },
        },
    }
    conn = S3Conn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, S3Conn)
    assert conn.bucket_uri == "s3://mybucket"
    assert isinstance(conn.credentials, S3AccessKeyCredentials)


@pytest.mark.unit
def test_s3_conn_load_from_dict_with_region():
    cx_dict = {
        "kind": "s3SrcConn",
        "apiVersion": "1.0",
        "name": "s3_conn_4",
        "conn_configs": {
            "bucket_uri": "s3://mybucket",
            "credentials": {
                "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            },
            "region": "us-west-2",
        },
    }
    conn = S3Conn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, S3Conn)
    assert conn.region == "us-west-2"


# --- GCSConn._load_from_dict ---


@pytest.mark.unit
def test_gcs_conn_load_from_dict():
    cx_dict = {
        "kind": "gcsSrcConn",
        "apiVersion": "1.0",
        "name": "gcs_conn",
        "conn_configs": {
            "bucket_uri": "gs://mybucket",
            "credentials": {
                "service_account_key": "test_key_content",
            },
        },
    }
    conn = GCSConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, GCSConn)
    assert conn.bucket_uri == "gs://mybucket"
    assert isinstance(conn.credentials, GCPServiceAccountKeyCredentials)


# --- AzureConn._load_from_dict ---


@pytest.mark.unit
def test_azure_conn_load_from_dict():
    cx_dict = {
        "kind": "azureSrcConn",
        "apiVersion": "1.0",
        "name": "azure_conn",
        "conn_configs": {
            "container_uri": "az://mycontainer",
            "credentials": {
                "account_name": "test_account",
                "account_key": "test_key",
            },
        },
    }
    conn = AzureConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, AzureConn)
    assert conn.container_uri == "az://mycontainer"
    assert isinstance(conn.credentials, AzureAccountKeyCredentials)


# --- ClickHouseConn._load_from_dict ---


@pytest.mark.unit
def test_clickhouse_conn_load_from_dict_s3_staging():
    cx_dict = {
        "kind": "clickhouseSrcConn",
        "apiVersion": "1.0",
        "name": "clickhouse_conn",
        "conn_configs": {
            "host": "clickhouse.example.com",
            "staging": {
                "provider": "s3",
                "bucket": "ch-staging",
                "region": "us-east-1",
                "base_path": "staging/ch",
                "credentials": {
                    "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                    "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                },
            },
        },
    }
    conn = ClickHouseConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, ClickHouseConn)
    assert conn.host == "clickhouse.example.com"
    assert isinstance(conn.staging, S3Config)
    assert conn.staging.bucket == "ch-staging"
    assert isinstance(conn.staging.credentials, S3AccessKeyCredentials)


@pytest.mark.unit
def test_clickhouse_conn_load_from_dict_azure_staging():
    cx_dict = {
        "kind": "clickhouseSrcConn",
        "apiVersion": "1.0",
        "name": "clickhouse_conn_1",
        "conn_configs": {
            "host": "clickhouse.example.com",
            "staging": {
                "provider": "azure",
                "container": "ch-staging",
                "base_path": "staging/ch",
                "credentials": {
                    "account_name": "test_account",
                    "account_key": "test_key",
                },
            },
        },
    }
    conn = ClickHouseConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, ClickHouseConn)
    assert isinstance(conn.staging, AzureConfig)
    assert isinstance(conn.staging.credentials, AzureAccountKeyCredentials)


@pytest.mark.unit
def test_clickhouse_conn_load_from_dict_gcs_staging():
    cx_dict = {
        "kind": "clickhouseSrcConn",
        "apiVersion": "1.0",
        "name": "clickhouse_conn_2",
        "conn_configs": {
            "host": "clickhouse.example.com",
            "staging": {
                "provider": "gcs",
                "bucket": "ch-staging",
                "base_path": "staging/ch",
                "upload_credentials": {
                    "service_account_key": "test_key_content",
                },
                "hmac_credentials": {
                    "access_key": "GOOG_ACCESS_KEY",
                    "secret_key": "GOOG_SECRET_KEY",
                },
            },
        },
    }
    conn = ClickHouseConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, ClickHouseConn)
    assert isinstance(conn.staging, GCSConfig)
    assert isinstance(conn.staging.upload_credentials, GCPServiceAccountKeyCredentials)
    assert isinstance(conn.staging.hmac_credentials, HMACCredentials)


@pytest.mark.unit
def test_clickhouse_conn_load_from_dict_with_credentials():
    cx_dict = {
        "kind": "clickhouseSrcConn",
        "apiVersion": "1.0",
        "name": "clickhouse_conn_3",
        "conn_configs": {
            "host": "clickhouse.example.com",
            "staging": {
                "provider": "s3",
                "bucket": "ch-staging",
                "region": "us-east-1",
                "base_path": "staging/ch",
                "credentials": {
                    "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                    "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                },
            },
            "credentials": {"user": "admin", "password": "secret"},
        },
    }
    conn = ClickHouseConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, ClickHouseConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


# --- DruidConn._load_from_dict ---


@pytest.mark.unit
def test_druid_conn_load_from_dict_s3_staging():
    cx_dict = {
        "kind": "druidSrcConn",
        "apiVersion": "1.0",
        "name": "druid_conn",
        "conn_configs": {
            "overlord_url": "http://druid-overlord:8090",
            "staging": {
                "provider": "s3",
                "bucket": "druid-staging",
                "region": "us-east-1",
                "base_path": "staging/druid",
                "credentials": {
                    "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                    "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                },
            },
        },
    }
    conn = DruidConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, DruidConn)
    assert conn.overlord_url == "http://druid-overlord:8090"
    assert isinstance(conn.staging, S3Config)


@pytest.mark.unit
def test_druid_conn_load_from_dict_with_credentials():
    cx_dict = {
        "kind": "druidSrcConn",
        "apiVersion": "1.0",
        "name": "druid_conn_1",
        "conn_configs": {
            "overlord_url": "http://druid-overlord:8090",
            "staging": {
                "provider": "s3",
                "bucket": "druid-staging",
                "region": "us-east-1",
                "base_path": "staging/druid",
                "credentials": {
                    "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                    "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                },
            },
            "credentials": {"user": "admin", "password": "secret"},
        },
    }
    conn = DruidConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, DruidConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


# --- StarRocksConn._load_from_dict ---


@pytest.mark.unit
def test_starrocks_conn_load_from_dict_s3_staging():
    cx_dict = {
        "kind": "starrocksSrcConn",
        "apiVersion": "1.0",
        "name": "starrocks_conn",
        "conn_configs": {
            "host": "starrocks-fe.example.com",
            "staging": {
                "provider": "s3",
                "bucket": "sr-staging",
                "region": "us-east-1",
                "base_path": "staging/sr",
                "credentials": {
                    "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                    "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                },
            },
        },
    }
    conn = StarRocksConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, StarRocksConn)
    assert conn.host == "starrocks-fe.example.com"
    assert isinstance(conn.staging, S3Config)


@pytest.mark.unit
def test_starrocks_conn_load_from_dict_with_credentials():
    cx_dict = {
        "kind": "starrocksSrcConn",
        "apiVersion": "1.0",
        "name": "starrocks_conn_1",
        "conn_configs": {
            "host": "starrocks-fe.example.com",
            "staging": {
                "provider": "s3",
                "bucket": "sr-staging",
                "region": "us-east-1",
                "base_path": "staging/sr",
                "credentials": {
                    "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                    "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                },
            },
            "credentials": {"user": "root", "password": "srpass"},
        },
    }
    conn = StarRocksConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, StarRocksConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


# --- build_staging_from_dict edge cases ---


@pytest.mark.unit
def test_staging_from_dict_missing_provider():
    from tabsdata._io.cloud_staging.common.storage_config import (
        build_staging_from_dict,
    )

    with pytest.raises(ValueError, match="provider"):
        build_staging_from_dict({"bucket": "test"})


@pytest.mark.unit
def test_staging_from_dict_unknown_provider():
    from tabsdata._io.cloud_staging.common.storage_config import (
        build_staging_from_dict,
    )

    with pytest.raises(ValueError, match="Unknown staging provider"):
        build_staging_from_dict({"provider": "hdfs", "bucket": "test"})


# --- BigQueryConn._load_from_dict ---


@pytest.mark.unit
def test_bigquery_conn_load_from_dict_plain():
    cx_dict = {
        "kind": "bigquerySrcConn",
        "apiVersion": "1.0",
        "name": "bigquery_conn",
        "conn_configs": {
            "gcs_folder": "gs://my-staging-bucket/bq",
            "credentials": {"service_account_key": "test_key_content"},
            "project": "my-project",
            "dataset": "my_dataset",
        },
    }
    conn = BigQueryConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, BigQueryConn)
    assert conn.gcs_folder == "gs://my-staging-bucket/bq"
    assert isinstance(conn.credentials, GCPServiceAccountKeyCredentials)
    assert conn.project == "my-project"
    assert conn.dataset == "my_dataset"


@pytest.mark.unit
def test_bigquery_conn_load_from_dict_no_project():
    cx_dict = {
        "kind": "bigquerySrcConn",
        "apiVersion": "1.0",
        "name": "bigquery_conn_1",
        "conn_configs": {
            "gcs_folder": "gs://my-staging-bucket/bq",
            "credentials": {"service_account_key": "test_key_content"},
        },
    }
    conn = BigQueryConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, BigQueryConn)
    assert conn.project is None
    assert conn.dataset is None


# --- Db2CdcConn._load_from_dict ---


@pytest.mark.unit
def test_db2_cdc_conn_load_from_dict_plain():
    cx_dict = {
        "kind": "db2cdcSrcConn",
        "apiVersion": "1.0",
        "name": "db2_cdc_conn",
        "conn_configs": {"uri": "db2://localhost/mydb"},
    }
    conn = Db2CdcConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, Db2CdcConn)
    assert conn.uri == "db2://localhost/mydb"
    assert conn.credentials is None


@pytest.mark.unit
def test_db2_cdc_conn_load_from_dict_with_credentials():
    cx_dict = {
        "kind": "db2cdcSrcConn",
        "apiVersion": "1.0",
        "name": "db2_cdc_conn_1",
        "conn_configs": {
            "uri": "db2://localhost/mydb",
            "credentials": {"user": "db2admin", "password": "db2pass"},
        },
    }
    conn = Db2CdcConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, Db2CdcConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


# --- MySQLCdcConn._load_from_dict ---


@pytest.mark.unit
def test_mysql_cdc_conn_load_from_dict_plain():
    cx_dict = {
        "kind": "mysqlcdcSrcConn",
        "apiVersion": "1.0",
        "name": "mysql_cdc_conn",
        "conn_configs": {"uri": "mysql://localhost/mydb"},
    }
    conn = MySQLCdcConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, MySQLCdcConn)
    assert conn.uri == "mysql://localhost/mydb"
    assert conn.credentials is None


@pytest.mark.unit
def test_mysql_cdc_conn_load_from_dict_with_credentials():
    cx_dict = {
        "kind": "mysqlcdcSrcConn",
        "apiVersion": "1.0",
        "name": "mysql_cdc_conn_1",
        "conn_configs": {
            "uri": "mysql://localhost/mydb",
            "credentials": {"user": "admin", "password": "secret"},
        },
    }
    conn = MySQLCdcConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, MySQLCdcConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


# --- PostgresCdcConn._load_from_dict ---


@pytest.mark.unit
def test_postgres_cdc_conn_load_from_dict_plain():
    cx_dict = {
        "kind": "postgrescdcSrcConn",
        "apiVersion": "1.0",
        "name": "postgres_cdc_conn",
        "conn_configs": {
            "uri": "postgresql://localhost/mydb",
        },
    }
    conn = PostgresCdcConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, PostgresCdcConn)
    assert conn.uri == "postgresql://localhost/mydb"
    assert conn.credentials is None


@pytest.mark.unit
def test_postgres_cdc_conn_load_from_dict_with_credentials():
    cx_dict = {
        "kind": "postgrescdcSrcConn",
        "apiVersion": "1.0",
        "name": "postgres_cdc_conn_1",
        "conn_configs": {
            "uri": "postgresql://localhost/mydb",
            "credentials": {"user": "pgadmin", "password": "pgpass"},
        },
    }
    conn = PostgresCdcConn._load_from_dict(cx_dict["conn_configs"])
    assert isinstance(conn, PostgresCdcConn)
    assert isinstance(conn.credentials, UserPasswordCredentials)


# --- KafkaConn._load_from_dict ---


@pytest.mark.unit
def test_kafka_conn_load_from_dict_plain():
    conn = KafkaConn._load_from_dict(
        {
            "servers": "broker1:9092,broker2:9092",
            "credentials": {"user": "kafka_user", "password": "kafka_pass"},
        }
    )
    assert isinstance(conn, KafkaConn)
    assert conn.servers == "broker1:9092,broker2:9092"
    assert isinstance(conn.credentials, UserPasswordCredentials)
    assert conn.schema_registry is None


@pytest.mark.unit
def test_kafka_conn_load_from_dict_servers_list():
    conn = KafkaConn._load_from_dict(
        {
            "servers": ["broker1:9092", "broker2:9092"],
            "credentials": {"user": "kafka_user", "password": "kafka_pass"},
            "group_id": "my-group",
        }
    )
    assert isinstance(conn, KafkaConn)
    assert conn.servers == ["broker1:9092", "broker2:9092"]
    assert conn.group_id == "my-group"


@pytest.mark.unit
def test_kafka_conn_load_from_dict_with_confluent_sr():
    conn = KafkaConn._load_from_dict(
        {
            "servers": "broker1:9092",
            "credentials": {"user": "kafka_user", "password": "kafka_pass"},
            "schema_registry": {
                "type": "confluent",
                "url": "https://sr.example.com:8081",
                "credentials": {"user": "sr_user", "password": "sr_pass"},
            },
        }
    )
    assert isinstance(conn, KafkaConn)
    assert isinstance(conn.schema_registry, ConfluentSchemaRegistry)
    assert conn.schema_registry.url == "https://sr.example.com:8081"
    assert isinstance(conn.schema_registry.credentials, UserPasswordCredentials)


@pytest.mark.unit
def test_kafka_conn_load_from_dict_with_confluent_sr_no_credentials():
    conn = KafkaConn._load_from_dict(
        {
            "servers": "broker1:9092",
            "credentials": {"user": "kafka_user", "password": "kafka_pass"},
            "schema_registry": {
                "type": "confluent",
                "url": "https://sr.example.com:8081",
            },
        }
    )
    assert isinstance(conn, KafkaConn)
    assert isinstance(conn.schema_registry, ConfluentSchemaRegistry)
    assert conn.schema_registry.credentials is None


@pytest.mark.unit
def test_kafka_conn_load_from_dict_with_aws_glue_sr():
    conn = KafkaConn._load_from_dict(
        {
            "servers": "broker1:9092",
            "credentials": {"user": "kafka_user", "password": "kafka_pass"},
            "schema_registry": {
                "type": "aws_glue",
                "region_name": "us-east-1",
                "credentials": {
                    "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                    "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                },
                "registry_name": "my_registry",
                "schema_name": "my_schema",
            },
        }
    )
    assert isinstance(conn, KafkaConn)
    assert isinstance(conn.schema_registry, AwsGlueSchemaRegistry)
    assert conn.schema_registry.region_name == "us-east-1"
    assert conn.schema_registry.registry_name == "my_registry"
    assert conn.schema_registry.schema_name == "my_schema"
    assert isinstance(conn.schema_registry.credentials, S3AccessKeyCredentials)


# --- build_schema_registry_from_dict edge cases ---


@pytest.mark.unit
def test_schema_registry_from_dict_missing_type():
    with pytest.raises(ValueError, match="type"):
        build_schema_registry_from_dict({"url": "https://sr.example.com:8081"})


@pytest.mark.unit
def test_schema_registry_from_dict_unknown_type():
    with pytest.raises(ValueError, match="Unknown schema registry type"):
        build_schema_registry_from_dict(
            {
                "type": "apicurio",
                "url": "https://sr.example.com:8081",
            }
        )


def test_collect_env_refs_nested():
    from tabsdata._io.connections.named_connection.named_connection import (
        collect_env_refs,
    )

    configs = {
        "uri": "mysql://h/db",
        "env:token": "TOP_TOKEN",
        "credentials": {"env:user": "DB_USER", "env:password": "DB_PASSWORD"},
    }
    assert sorted(collect_env_refs(configs)) == [
        "DB_PASSWORD",
        "DB_USER",
        "TOP_TOKEN",
    ]
    assert collect_env_refs(None) == []
    assert collect_env_refs({"plain": "x"}) == []


def test_secrets_from_env(monkeypatch):
    from tabsdata._io.connections.named_connection.named_connection import (
        secrets_from_env,
    )

    monkeypatch.setenv("DB_USER", "svc")
    monkeypatch.setenv("DB_PASSWORD", "hunter2")
    document = {
        "kind": "mysqlSrcConn",
        "name": "x",
        "conn_configs": {
            "uri": "mysql://h/db",
            "credentials": {"env:user": "DB_USER", "env:password": "DB_PASSWORD"},
        },
    }
    assert secrets_from_env(document) == [
        {"key": "DB_PASSWORD", "value": "hunter2"},
        {"key": "DB_USER", "value": "svc"},
    ]


def test_secrets_from_env_missing_raises(monkeypatch):
    from tabsdata._io.connections.named_connection.named_connection import (
        secrets_from_env,
    )

    monkeypatch.delenv("MISSING_KEY", raising=False)
    document = {"conn_configs": {"env:p": "MISSING_KEY"}}
    with pytest.raises(ValueError, match="MISSING_KEY"):
        secrets_from_env(document)
