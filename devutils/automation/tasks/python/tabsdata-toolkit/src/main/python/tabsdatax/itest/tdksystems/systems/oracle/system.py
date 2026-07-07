"""Oracle Database Free 23c. Domain == user/schema (FREEPDB1)."""
from __future__ import annotations

import os

from .._container import AdminConn, ContainerBase, DomainResult
from ...shell import docker_exec, docker_run, gen_password


ORACLE_SERVICE = "FREEPDB1"


class OracleSystem(ContainerBase):
    slug = "oracle"
    label = "Oracle Database Free"
    domain_desc = "a user/schema (Oracle treats them as one entity)"
    domain_flag = "database"
    supports_sql = True

    image = f"container-registry.oracle.com/database/free:{os.environ.get('ORACLE_IMAGE_VERSION', '23.6.0.0-lite')}"
    image_platform = "linux/amd64"
    internal_port = 1521
    system_user = "system"
    password_env_name = "ORACLE_PWD"
    extra_env: list[tuple[str, str]] = []
    extra_flags: list[str] = []

    def _docker_run(self, container: str, host_port: int, password: str) -> str:
        return docker_run(self._build_docker_args(container, host_port, password))

    def check_ready(self, container: str, password: str) -> bool:
        # checkDBStatus.sh only verifies the SMON process is alive — it returns
        # OK before setPassword.sh has propagated ORACLE_PWD to SYSTEM, so a
        # subsequent sqlplus connect can hit ORA-01017. Require a real auth
        # round-trip so readiness implies the admin password actually works.
        cp = docker_exec(
            container, "sqlplus", "-s", "-L",
            f"{self.system_user}/{password}@//localhost:1521/{ORACLE_SERVICE}",
            input="WHENEVER SQLERROR EXIT FAILURE;\nSELECT 1 FROM DUAL;\nEXIT;\n",
        )
        return cp.returncode == 0

    def _admin_url(self, admin: AdminConn) -> str:
        return (f"oracle+oracledb://{admin.user}:{admin.password}"
                f"@{admin.host}:{admin.port}/?service_name={ORACLE_SERVICE}")

    def do_create(self, admin: AdminConn, name: str) -> DomainResult:
        pw = gen_password()
        # sqlplus directives (WHENEVER / SET / EXIT) are dropped — only the DDL
        # runs over the driver. Oracle DDL auto-commits.
        self._run_admin_sql(admin, [
            f'CREATE USER {name} IDENTIFIED BY "{pw}"',
            f"GRANT CONNECT, RESOURCE TO {name}",
            f"GRANT UNLIMITED TABLESPACE TO {name}",
        ])
        uri = (f"oracle+oracledb://{name}:{pw}@{admin.host}:{admin.port}"
               f"/?service_name={ORACLE_SERVICE}")
        return DomainResult(db=name, user=name, password=pw, sqlalchemy_uri=uri)

    def do_delete(self, admin: AdminConn, name: str) -> None:
        self._run_admin_sql(admin, [f"DROP USER {name} CASCADE"])

    def do_list(self, admin: AdminConn) -> list[str]:
        return self._query_admin_names(admin, (
            "SELECT username FROM all_users WHERE oracle_maintained = 'N' "
            "ORDER BY username"))
