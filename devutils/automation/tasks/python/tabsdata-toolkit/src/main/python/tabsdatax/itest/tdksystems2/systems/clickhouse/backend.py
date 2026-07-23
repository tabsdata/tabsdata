#
# Copyright 2026 Tabsdata Inc.
#

"""ClickHouse backend — a docker-container OLAP database.

A domain is a database with a dedicated user. Only dialect hooks live here; the
container lifecycle and data plane come from :class:`SqlContainerBackend` over
the ``clickhousedb`` SQLAlchemy dialect (clickhouse-connect, HTTP on 8123). Only
the ``world`` model ships a ClickHouse schema in v1, and it has no delta
entities, so ``modify-data`` is not offered (``supports_changes=False``).
"""

from __future__ import annotations

from tabsdatax.itest.tdksystems2.systems._common import container_info
from tabsdatax.itest.tdksystems2.systems._common.sql import SqlContainerBackend


class Clickhouse(SqlContainerBackend):
    slug = "clickhouse"
    label = "ClickHouse"
    domain_word = "database"
    max_entity_name_length = 255
    login_maxlen = 255  # the owner user is named after the database
    supports_changes = False  # OLAP; the only model (world) has no delta entities

    image = container_info.CLICKHOUSE.ref
    image_platform = container_info.CLICKHOUSE.platform
    internal_port = 8123
    root_user = "default"
    driver = "clickhousedb"
    admin_db = "default"
    extra_run_flags = ["--ulimit", "nofile=262144:262144"]
    system_schemas = frozenset({"system", "information_schema", "default"})

    def env_vars(self, password: str) -> dict[str, str]:
        # DEFAULT_ACCESS_MANAGEMENT lets the default user run CREATE USER / GRANT.
        return {
            "CLICKHOUSE_PASSWORD": password,
            "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
        }

    def ready_exec_cmd(self, password: str) -> list[str]:
        return [
            "clickhouse-client",
            f"--user={self.root_user}",
            f"--password={password}",
            "-q",
            "SELECT 1",
        ]

    def create_domain_sql(self, db: str, user: str, password: str) -> list[str]:
        return [
            f'CREATE DATABASE "{db}"',
            f'CREATE USER "{user}" IDENTIFIED WITH plaintext_password '
            f"BY '{password}'",
            f'GRANT ALL ON "{db}".* TO "{user}"',
            # Cloud-staging ingest uses s3()/azureBlobStorage() table functions,
            # which need global source grants (not the db-scoped ALL above).
            f'GRANT S3, CREATE TEMPORARY TABLE ON *.* TO "{user}"',
        ]

    def delete_domain_sql(self, name: str) -> list[str]:
        return [
            f'DROP USER IF EXISTS "{self._login(name)}"',
            f'DROP DATABASE IF EXISTS "{name}"',
        ]

    def list_schemas_sql(self) -> str:
        return (
            "SELECT name FROM system.databases WHERE name NOT IN "
            "('system','INFORMATION_SCHEMA','information_schema','default') "
            "ORDER BY name"
        )

    def drop_all_tables(self, uri: str) -> None:
        # The clickhousedb dialect can't reflect (its SHOW TABLES isn't a proper
        # executable), so drop the domain's tables directly instead of the base's
        # SQLAlchemy MetaData.reflect.
        from sqlalchemy import create_engine

        engine = create_engine(uri)
        try:
            with engine.connect() as conn:
                tables = [
                    row[0] for row in conn.exec_driver_sql("SHOW TABLES").fetchall()
                ]
                for table in tables:
                    conn.exec_driver_sql(f'DROP TABLE IF EXISTS "{table}"')
        finally:
            engine.dispose()
