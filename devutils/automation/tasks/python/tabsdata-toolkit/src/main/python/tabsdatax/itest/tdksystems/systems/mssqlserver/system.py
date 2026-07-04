"""Microsoft SQL Server. Single-DB-with-login domain model."""
from __future__ import annotations

import os

from .._container import ContainerBase, DomainResult
from ...errors import DatagenError
from ...shell import docker_exec, docker_exec_check, docker_run, gen_password


class MssqlSystem(ContainerBase):
    slug = "mssqlserver"
    label = "Microsoft SQL Server"
    domain_desc = "a database (with a dedicated user that owns it)"
    domain_flag = "schema"
    supports_sql = True

    image = f"mcr.microsoft.com/mssql/server:{os.environ.get('MSSQL_IMAGE_VERSION', '2022-CU14-ubuntu-22.04')}"
    image_platform = "linux/amd64"
    internal_port = 1433
    system_user = "sa"
    password_env_name = "MSSQL_SA_PASSWORD"
    extra_env: list[tuple[str, str]] = [("ACCEPT_EULA", "Y")]
    extra_flags: list[str] = []

    def _docker_run(self, container: str, host_port: int, password: str) -> str:
        return docker_run(self._build_docker_args(container, host_port, password))

    def check_ready(self, container: str, password: str) -> bool:
        return docker_exec_check(
            container, "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost",
            "-U", self.system_user, "-P", password,
            "-Q", "SELECT 1", "-C", "-N", "-l", "5", "-b",
        )

    def _exec(self, container: str, password: str, sql: str) -> str:
        cp = docker_exec(
            container, "/opt/mssql-tools18/bin/sqlcmd",
            "-S", "localhost", "-U", self.system_user, "-P", password,
            "-C", "-N", "-b", "-h-1", "-W",
            input=sql,
        )
        if cp.returncode != 0:
            raise DatagenError(f"sqlcmd failed: {cp.stderr.strip() or cp.stdout.strip()}")
        return cp.stdout

    def do_create(self, container: str, password: str, name: str) -> DomainResult:
        pw = gen_password()
        self._exec(container, password, (
            f"CREATE DATABASE [{name}];\nGO\n"
            f"CREATE LOGIN [{name}] WITH PASSWORD = '{pw}', CHECK_POLICY = OFF;\nGO\n"
            f"USE [{name}];\nGO\n"
            f"CREATE USER [{name}] FOR LOGIN [{name}];\n"
            f"ALTER ROLE db_owner ADD MEMBER [{name}];\nGO\n"
        ))
        port = os.environ.get("PORT", "1433")
        uri = (f"mssql+pyodbc://{name}:{pw}@localhost:{port}/{name}"
               f"?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")
        return DomainResult(db=name, user=name, password=pw, sqlalchemy_uri=uri)

    def do_delete(self, container: str, password: str, name: str) -> None:
        self._exec(container, password, (
            f"ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;\nGO\n"
            f"DROP DATABASE [{name}];\nGO\n"
            f"DROP LOGIN [{name}];\nGO\n"
        ))

    def do_list(self, container: str, password: str) -> list[str]:
        out = self._exec(container, password, (
            "SET NOCOUNT ON;\n"
            "SELECT name FROM sys.databases "
            "WHERE name NOT IN ('master','tempdb','model','msdb') ORDER BY name;\nGO\n"
        ))
        return [line for line in (l.strip() for l in out.splitlines()) if line]
