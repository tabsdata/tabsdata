#
# Copyright 2026 Tabsdata Inc.
#

"""PostgreSQL — community 17.x. Single-DB-with-dedicated-user domain model."""

from __future__ import annotations

import os
import platform

from ...shell import docker_exec_check, docker_run
from .._container import AdminConn, ContainerBase, DomainResult


class PostgresSystem(ContainerBase):
    slug = "postgres"
    label = "PostgreSQL"
    domain_desc = "a database (with a dedicated user that owns it)"
    domain_flag = "schema"
    supports_sql = True
    # CREATE / DROP DATABASE can't run inside a transaction block.
    admin_autocommit = True

    image = f"postgres:{os.environ.get('POSTGRES_IMAGE_VERSION', '17')}"
    image_platform = (
        "linux/arm64" if platform.machine() in ("arm64", "aarch64") else "linux/amd64"
    )
    internal_port = 5432
    system_user = "postgres"
    password_env_name = "POSTGRES_PASSWORD"
    extra_env: list[tuple[str, str]] = []
    extra_flags: list[str] = []

    def _docker_run(self, container: str, host_port: int, password: str) -> str:
        return docker_run(self._build_docker_args(container, host_port, password))

    def check_ready(self, container: str, password: str) -> bool:
        return docker_exec_check(
            container,
            "pg_isready",
            "-U",
            self.system_user,
            "-h",
            "localhost",
            "-q",
        )

    def _admin_url(self, admin: AdminConn) -> str:
        return (
            f"postgresql+psycopg2://{admin.user}:{admin.password}"
            f"@{admin.host}:{admin.port}/postgres"
        )

    def domain_result(self, admin: AdminConn, name: str, password: str) -> DomainResult:
        uri = (
            f"postgresql+psycopg2://{name}:{password}@{admin.host}:{admin.port}/{name}"
        )
        return DomainResult(db=name, user=name, password=password, sqlalchemy_uri=uri)

    def do_create(self, admin: AdminConn, result: DomainResult) -> None:
        self._run_admin_sql(
            admin,
            [
                f'CREATE DATABASE "{result.db}"',
                f"CREATE USER \"{result.user}\" WITH PASSWORD '{result.password}'",
                f'ALTER DATABASE "{result.db}" OWNER TO "{result.user}"',
                f'GRANT ALL PRIVILEGES ON DATABASE "{result.db}" TO "{result.user}"',
            ],
        )

    def do_delete(self, admin: AdminConn, name: str) -> None:
        self._run_admin_sql(
            admin,
            [
                f'DROP DATABASE IF EXISTS "{name}"',
                f'DROP USER IF EXISTS "{name}"',
            ],
        )

    def do_list(self, admin: AdminConn) -> list[str]:
        return self._query_admin_names(
            admin,
            (
                "SELECT datname FROM pg_database "
                "WHERE datistemplate = false AND datname <> 'postgres' "
                "ORDER BY datname"
            ),
        )
