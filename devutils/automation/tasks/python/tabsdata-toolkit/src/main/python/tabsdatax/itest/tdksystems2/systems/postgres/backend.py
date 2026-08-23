#
# Copyright 2026 Tabsdata Inc.
#

"""PostgreSQL backend — a docker-run container with a per-domain database.

A domain is a database with a dedicated owner role of the same name. Admin runs
as the ``postgres`` superuser against the ``postgres`` maintenance database;
``CREATE`` / ``DROP DATABASE`` can't run inside a transaction, so admin SQL is
executed in autocommit. Only the PostgreSQL dialect hooks live here.
"""

from __future__ import annotations

from tabsdatax.itest.tdksystems2.systems._common import container_info
from tabsdatax.itest.tdksystems2.systems._common.sql import SqlContainerBackend


class Postgres(SqlContainerBackend):
    slug = "postgres"
    label = "PostgreSQL"
    domain_word = "database"
    entity_aliases = ("SCHEMA",)  # fixtures also expose the domain db as `schema`
    max_entity_name_length = 63  # NAMEDATALEN - 1
    login_maxlen = 63  # the owner role is named after the database

    image = container_info.POSTGRES.ref
    image_platform = container_info.POSTGRES.platform
    internal_port = 5432
    root_user = "postgres"
    driver = "postgresql+psycopg2"
    admin_db = "postgres"
    admin_autocommit = True
    system_schemas = frozenset({"postgres", "template0", "template1"})

    # Matches the ceiling td-database's own embedded server boots with. The Rust
    # db tests run 32-way parallel, each holding an admin, a serving and a
    # teardown pool, which overruns the image default of 100.
    cmd_args = ["-c", "max_connections=500"]

    def env_vars(self, password: str) -> dict[str, str]:
        return {"POSTGRES_PASSWORD": password}

    def ready_exec_cmd(self, password: str) -> list[str]:
        return ["pg_isready", "-U", self.root_user, "-h", "localhost", "-q"]

    def create_domain_sql(self, db: str, user: str, password: str) -> list[str]:
        return [
            f'CREATE DATABASE "{db}"',
            f"CREATE USER \"{user}\" WITH PASSWORD '{password}'",
            f'ALTER DATABASE "{db}" OWNER TO "{user}"',
            f'GRANT ALL PRIVILEGES ON DATABASE "{db}" TO "{user}"',
        ]

    def delete_domain_sql(self, name: str) -> list[str]:
        # WITH (FORCE) (PG 13+) drops even with a lingering client session.
        return [
            f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)',
            f'DROP USER IF EXISTS "{self._login(name)}"',
        ]

    def list_schemas_sql(self) -> str:
        return (
            "SELECT datname FROM pg_database "
            "WHERE datistemplate = false AND datname <> 'postgres' "
            "ORDER BY datname"
        )
