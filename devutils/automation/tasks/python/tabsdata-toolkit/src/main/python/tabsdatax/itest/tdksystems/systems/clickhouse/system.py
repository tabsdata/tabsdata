#
# Copyright 2026 Tabsdata Inc.
#

"""ClickHouse container system — hooks only; the lifecycle is in ContainerBase."""

from __future__ import annotations

import os
import platform

from ...shell import docker_exec_check, docker_run
from .._container import AdminConn, ContainerBase, DomainResult


class ClickhouseSystem(ContainerBase):
    slug = "clickhouse"
    label = "ClickHouse"
    domain_desc = "a database (with a dedicated user that owns it)"
    domain_flag = "database"
    supports_sql = True
    uses_file_staging = True

    image = (
        "clickhouse/clickhouse-server:"
        f"{os.environ.get('CLICKHOUSE_IMAGE_VERSION', '24.8')}"
    )
    image_platform = (
        "linux/arm64" if platform.machine() in ("arm64", "aarch64") else "linux/amd64"
    )
    internal_port = 8123
    system_user = "default"
    password_env_name = "CLICKHOUSE_PASSWORD"
    # Grant the default user access-management rights so admin scripts can run
    # CREATE USER / GRANT statements.
    extra_env: list[tuple[str, str]] = [("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")]
    extra_flags: list[str] = ["--ulimit", "nofile=262144:262144"]

    def _docker_run(self, container: str, host_port: int, password: str) -> str:
        return docker_run(self._build_docker_args(container, host_port, password))

    def check_ready(self, container: str, password: str) -> bool:
        return docker_exec_check(
            container,
            "clickhouse-client",
            f"--user={self.system_user}",
            f"--password={password}",
            "-q",
            "SELECT 1",
        )

    def _admin_url(self, admin: AdminConn) -> str:
        return (
            f"clickhouse+http://{admin.user}:{admin.password}"
            f"@{admin.host}:{admin.port}/default"
        )

    def domain_result(self, admin: AdminConn, name: str, password: str) -> DomainResult:
        uri = f"clickhouse+http://{name}:{password}@{admin.host}:{admin.port}/{name}"
        return DomainResult(db=name, user=name, password=password, sqlalchemy_uri=uri)

    def do_create(self, admin: AdminConn, result: DomainResult) -> None:
        self._run_admin_sql(
            admin,
            [
                f'CREATE DATABASE "{result.db}"',
                f'CREATE USER "{result.user}" IDENTIFIED WITH '
                f"plaintext_password BY '{result.password}'",
                f'GRANT ALL ON "{result.db}".* TO "{result.user}"',
            ],
        )

    def do_delete(self, admin: AdminConn, name: str) -> None:
        self._run_admin_sql(
            admin,
            [
                f'DROP USER IF EXISTS "{name}"',
                f'DROP DATABASE IF EXISTS "{name}"',
            ],
        )

    def do_list(self, admin: AdminConn) -> list[str]:
        return self._query_admin_names(
            admin,
            (
                "SELECT name FROM system.databases WHERE name NOT IN "
                "('system','INFORMATION_SCHEMA','information_schema','default') "
                "ORDER BY name"
            ),
        )
