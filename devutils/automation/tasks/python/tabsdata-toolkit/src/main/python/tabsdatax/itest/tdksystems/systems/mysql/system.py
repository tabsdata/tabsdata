#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL container system — hooks only; the lifecycle is in ContainerBase.

CDC is enabled by default (row-format binlog, full metadata, fixed server-id)
so downstream change-data-capture tools can subscribe to the binlog. The
per-domain user is the database name, trimmed to MySQL's 32-char login limit.
"""

from __future__ import annotations

import os
import platform
import sys

from ...shell import docker_exec_check, docker_run
from .._container import AdminConn, ContainerBase, DomainResult

_MYSQL_USER_MAXLEN = 32


class MysqlSystem(ContainerBase):
    slug = "mysql"
    label = "MySQL"
    domain_desc = "a database (with a dedicated user that owns it)"

    image = f"mysql:{os.environ.get('MYSQL_IMAGE_VERSION', '8.4')}"
    image_platform = (
        "linux/arm64" if platform.machine() in ("arm64", "aarch64") else "linux/amd64"
    )
    internal_port = 3306
    system_user = "root"
    password_env_name = "MYSQL_ROOT_PASSWORD"
    cmd_args = [
        "--server-id=1",
        "--log-bin=mysql-bin",
        "--binlog-format=ROW",
        "--binlog-row-metadata=FULL",
        "--binlog-expire-logs-seconds=259200",
    ]

    # ---- container hooks ----

    def _docker_run(self, container: str, host_port: int, password: str) -> str:
        return docker_run(self._build_docker_args(container, host_port, password))

    def check_ready(self, container: str, password: str) -> bool:
        return docker_exec_check(
            container,
            "mysqladmin",
            "ping",
            "-h",
            "localhost",
            f"-u{self.system_user}",
            f"--password={password}",
            "--silent",
        )

    # ---- domain admin hooks ----

    def _admin_url(self, admin: AdminConn) -> str:
        return (
            f"mysql+mysqlconnector://{admin.user}:{admin.password}"
            f"@{admin.host}:{admin.port}"
        )

    def _login(self, name: str, *, warn: bool = False) -> str:
        if len(name) <= _MYSQL_USER_MAXLEN:
            return name
        trimmed = name[:_MYSQL_USER_MAXLEN]
        if warn:
            print(
                f"WARNING: MySQL login names cap at {_MYSQL_USER_MAXLEN} chars; "
                f"trimming '{name}' to '{trimmed}' (risk of collision if two "
                f"domain names share the first {_MYSQL_USER_MAXLEN} chars).",
                file=sys.stderr,
            )
        return trimmed

    def domain_result(self, admin: AdminConn, name: str, password: str) -> DomainResult:
        login = self._login(name, warn=True)
        uri = f"mysql+mysqlconnector://{login}:{password}@{admin.host}:{admin.port}/{name}"
        return DomainResult(db=name, user=login, password=password, sqlalchemy_uri=uri)

    def do_create(self, admin: AdminConn, result: DomainResult) -> None:
        self._run_admin_sql(
            admin,
            [
                f"CREATE DATABASE `{result.db}`",
                f"CREATE USER '{result.user}'@'%' IDENTIFIED BY '{result.password}'",
                f"GRANT ALL PRIVILEGES ON `{result.db}`.* TO '{result.user}'@'%'",
                f"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* "
                f"TO '{result.user}'@'%'",
                "FLUSH PRIVILEGES",
            ],
        )

    def do_delete(self, admin: AdminConn, name: str) -> None:
        self._run_admin_sql(
            admin,
            [
                f"DROP DATABASE IF EXISTS `{name}`",
                f"DROP USER IF EXISTS '{self._login(name)}'@'%'",
                "FLUSH PRIVILEGES",
            ],
        )

    def do_list(self, admin: AdminConn) -> list[str]:
        return self._query_admin_names(
            admin,
            (
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name NOT IN "
                "('information_schema','mysql','performance_schema','sys')"
            ),
        )
