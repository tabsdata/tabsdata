"""MySQL container system — hooks only; the lifecycle is in ContainerBase.

CDC is enabled by default (row-format binlog, full metadata, fixed server-id)
so downstream change-data-capture tools can subscribe to the binlog. The
per-domain user is the database name, trimmed to MySQL's 32-char login limit.
"""
from __future__ import annotations

import os
import platform
import sys

from .._container import ContainerBase, DomainResult
from ...errors import DatagenError
from ...shell import docker_exec, docker_exec_check, docker_run, gen_password

_MYSQL_USER_MAXLEN = 32


class MysqlSystem(ContainerBase):
    slug = "mysql"
    label = "MySQL"
    domain_desc = "a database (with a dedicated user that owns it)"

    image = f"mysql:{os.environ.get('MYSQL_IMAGE_VERSION', '8.4')}"
    image_platform = ("linux/arm64"
                      if platform.machine() in ("arm64", "aarch64")
                      else "linux/amd64")
    internal_port = 3306
    system_user = "root"
    password_env_name = "MYSQL_ROOT_PASSWORD"
    cmd_args = [
        "--server-id=1", "--log-bin=mysql-bin", "--binlog-format=ROW",
        "--binlog-row-metadata=FULL", "--binlog-expire-logs-seconds=259200",
    ]

    # ---- container hooks ----

    def _docker_run(self, container: str, host_port: int, password: str) -> str:
        return docker_run(self._build_docker_args(container, host_port, password))

    def check_ready(self, container: str, password: str) -> bool:
        return docker_exec_check(
            container, "mysqladmin", "ping", "-h", "localhost",
            f"-u{self.system_user}", f"--password={password}", "--silent")

    # ---- domain admin hooks ----

    def _exec_sql(self, container: str, password: str, sql: str) -> str:
        cp = docker_exec(container, "mysql", f"-u{self.system_user}",
                         f"--password={password}", "-N", "-B", input=sql)
        if cp.returncode != 0:
            raise DatagenError(f"mysql command failed: {cp.stderr.strip()}")
        return cp.stdout

    def _login(self, name: str, *, warn: bool = False) -> str:
        if len(name) <= _MYSQL_USER_MAXLEN:
            return name
        trimmed = name[:_MYSQL_USER_MAXLEN]
        if warn:
            print(f"WARNING: MySQL login names cap at {_MYSQL_USER_MAXLEN} chars; "
                  f"trimming '{name}' to '{trimmed}' (risk of collision if two "
                  f"domain names share the first {_MYSQL_USER_MAXLEN} chars).",
                  file=sys.stderr)
        return trimmed

    def do_create(self, container: str, password: str, name: str) -> DomainResult:
        pw = gen_password()
        login = self._login(name, warn=True)
        self._exec_sql(container, password, (
            f"CREATE DATABASE `{name}`;\n"
            f"CREATE USER '{login}'@'%' IDENTIFIED BY '{pw}';\n"
            f"GRANT ALL PRIVILEGES ON `{name}`.* TO '{login}'@'%';\n"
            f"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{login}'@'%';\n"
            f"FLUSH PRIVILEGES;\n"))
        port = os.environ.get("PORT", "3306")
        uri = f"mysql+mysqlconnector://{login}:{pw}@localhost:{port}/{name}"
        return DomainResult(db=name, user=login, password=pw, sqlalchemy_uri=uri)

    def do_delete(self, container: str, password: str, name: str) -> None:
        self._exec_sql(container, password, (
            f"DROP DATABASE IF EXISTS `{name}`;\n"
            f"DROP USER IF EXISTS '{self._login(name)}'@'%';\n"
            f"FLUSH PRIVILEGES;\n"))

    def do_list(self, container: str, password: str) -> list[str]:
        out = self._exec_sql(container, password, (
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN "
            "('information_schema','mysql','performance_schema','sys');\n"))
        return [ln.strip() for ln in out.splitlines() if ln.strip()]
