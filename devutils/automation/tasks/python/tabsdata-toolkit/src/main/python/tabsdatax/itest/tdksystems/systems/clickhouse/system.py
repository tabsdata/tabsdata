"""ClickHouse container system — hooks only; the lifecycle is in ContainerBase."""
from __future__ import annotations

import os
import platform

from .._container import ContainerBase, DomainResult
from ...errors import DatagenError
from ...shell import docker_exec, docker_exec_check, docker_run, gen_password


class ClickhouseSystem(ContainerBase):
    slug = "clickhouse"
    label = "ClickHouse"
    domain_desc = "a database (with a dedicated user that owns it)"
    domain_flag = "database"
    supports_sql = True
    uses_file_staging = True

    image = f"clickhouse/clickhouse-server:{os.environ.get('CLICKHOUSE_IMAGE_VERSION', '24.8')}"
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
            container, "clickhouse-client",
            f"--user={self.system_user}", f"--password={password}",
            "-q", "SELECT 1",
        )

    def _exec(self, container: str, password: str, sql: str) -> str:
        cp = docker_exec(
            container, "clickhouse-client",
            f"--user={self.system_user}", f"--password={password}", "--multiquery",
            input=sql,
        )
        if cp.returncode != 0:
            raise DatagenError(f"clickhouse-client failed: {cp.stderr.strip()}")
        return cp.stdout

    def do_create(self, container: str, password: str, name: str) -> DomainResult:
        pw = gen_password()
        self._exec(container, password, (
            f'CREATE DATABASE "{name}";\n'
            f'CREATE USER "{name}" IDENTIFIED WITH plaintext_password BY \'{pw}\';\n'
            f'GRANT ALL ON "{name}".* TO "{name}";\n'
        ))
        port = os.environ.get("PORT", "8123")
        uri = f"clickhouse+http://{name}:{pw}@localhost:{port}/{name}"
        return DomainResult(db=name, user=name, password=pw, sqlalchemy_uri=uri)

    def do_delete(self, container: str, password: str, name: str) -> None:
        self._exec(container, password, (
            f'DROP USER IF EXISTS "{name}";\n'
            f'DROP DATABASE IF EXISTS "{name}";\n'
        ))

    def do_list(self, container: str, password: str) -> list[str]:
        out = self._exec(container, password, (
            "SELECT name FROM system.databases "
            "WHERE name NOT IN ('system','INFORMATION_SCHEMA','information_schema','default') "
            "ORDER BY name;\n"
        ))
        return [line for line in (l.strip() for l in out.splitlines()) if line]
