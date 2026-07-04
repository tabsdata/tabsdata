"""PostgreSQL — community 17.x. Single-DB-with-dedicated-user domain model."""
from __future__ import annotations

import os
import platform

from .._container import ContainerBase, DomainResult
from ...errors import DatagenError
from ...shell import docker_exec, docker_exec_check, docker_run, gen_password


class PostgresSystem(ContainerBase):
    slug = "postgres"
    label = "PostgreSQL"
    domain_desc = "a database (with a dedicated user that owns it)"
    domain_flag = "schema"
    supports_sql = True

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
            container, "pg_isready", "-U", self.system_user, "-h", "localhost", "-q",
        )

    def _exec(self, container: str, password: str, sql: str) -> str:
        cp = docker_exec(
            container, "psql", "-U", self.system_user, "-d", "postgres",
            "-v", "ON_ERROR_STOP=1", "-qAt",
            input=sql, env={"PGPASSWORD": password},
        )
        if cp.returncode != 0:
            raise DatagenError(f"psql command failed: {cp.stderr.strip()}")
        return cp.stdout

    def do_create(self, container: str, password: str, name: str) -> DomainResult:
        pw = gen_password()
        self._exec(container, password, (
            f'CREATE DATABASE "{name}";\n'
            f'CREATE USER "{name}" WITH PASSWORD \'{pw}\';\n'
            f'ALTER DATABASE "{name}" OWNER TO "{name}";\n'
            f'GRANT ALL PRIVILEGES ON DATABASE "{name}" TO "{name}";\n'
        ))
        port = os.environ.get("PORT", "5432")
        uri = f"postgresql+psycopg2://{name}:{pw}@localhost:{port}/{name}"
        return DomainResult(db=name, user=name, password=pw, sqlalchemy_uri=uri)

    def do_delete(self, container: str, password: str, name: str) -> None:
        self._exec(container, password, (
            f'DROP DATABASE IF EXISTS "{name}";\n'
            f'DROP USER IF EXISTS "{name}";\n'
        ))

    def do_list(self, container: str, password: str) -> list[str]:
        out = self._exec(container, password, (
            "SELECT datname FROM pg_database "
            "WHERE datistemplate = false AND datname <> 'postgres' "
            "ORDER BY datname;\n"
        ))
        return [line for line in (l.strip() for l in out.splitlines()) if line]
