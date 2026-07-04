"""StarRocks all-in-one image. MySQL-protocol-compatible on 9030."""
from __future__ import annotations

import os
import time

from .._container import ContainerBase, DomainResult
from ...errors import DatagenError
from ...shell import (
    docker_exec, docker_exec_check, docker_run, gen_password,
)


class StarrocksSystem(ContainerBase):
    slug = "starrocks"
    label = "StarRocks"
    domain_desc = "a database (with a dedicated user that owns it)"
    domain_flag = "database"
    supports_sql = True
    uses_file_staging = True

    image = f"starrocks/allin1-ubuntu:{os.environ.get('STARROCKS_IMAGE_VERSION', '4.0.8')}"
    image_platform = "linux/amd64"
    internal_port = 9030
    system_user = "root"
    password_env_name = ""  # allin1 doesn't accept a root password env
    extra_env: list[tuple[str, str]] = []
    extra_flags: list[str] = []

    def generate_password(self) -> str:
        # allin1 requires the empty root password (BE registers internally with it).
        return ""

    def _docker_run(self, container: str, host_port: int, password: str) -> str:
        return docker_run(self._build_docker_args(container, host_port, password))

    def _readiness_fatal(self, container: str, password: str) -> str | None:
        # The BE (backend) needs an x86-64 CPU with AVX2; under emulation on
        # Apple Silicon (arm64) it crash-loops and supervisord marks it FATAL.
        # Detect that and fail fast rather than waiting out the full timeout.
        cp = docker_exec(container, "supervisorctl", "status", "beservice")
        if "FATAL" in (cp.stdout or "") or "EXITED" in (cp.stdout or ""):
            return (
                "the StarRocks BE failed to start (beservice FATAL) — it needs "
                "an x86-64 CPU with AVX2, which isn't available under emulation "
                "on Apple Silicon (arm64). StarRocks can't run on this host."
            )
        return None

    def check_ready(self, container: str, password: str) -> bool:
        # FE accepts SELECT 1, AND every BE in SHOW BACKENDS reports Alive=true.
        if not docker_exec_check(
            container, "mysql", "-h", "127.0.0.1", "-P", "9030", "-u", "root",
            f"--password={password}", "-e", "SELECT 1",
        ):
            return False
        cp = docker_exec(
            container, "mysql", "-h", "127.0.0.1", "-P", "9030", "-u", "root",
            f"--password={password}", "-BNe", "SHOW BACKENDS",
        )
        if cp.returncode != 0 or not cp.stdout.strip():
            return False
        for line in cp.stdout.strip().splitlines():
            cols = line.split("\t")
            if len(cols) < 9 or cols[8] != "true":
                return False
        return True

    def _post_start(self, container: str, password: str) -> None:
        # allin1 has only one BE; default replication=3 makes every CREATE TABLE fail.
        # Wait briefly for FE config endpoint to accept the admin command.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            cp = docker_exec(
                container, "mysql", "-h", "127.0.0.1", "-P", "9030", "-u", "root",
                f"--password={password}",
                input='admin set frontend config ("default_replication_num" = "1");\n',
            )
            if cp.returncode == 0:
                return
            time.sleep(2)
        raise DatagenError("could not set default_replication_num=1 on StarRocks FE")

    def _exec(self, container: str, password: str, sql: str) -> str:
        cp = docker_exec(
            container, "mysql", "-h", "127.0.0.1", "-P", "9030",
            "-u", self.system_user, f"--password={password}",
            input=sql,
        )
        if cp.returncode != 0:
            raise DatagenError(f"mysql command failed: {cp.stderr.strip()}")
        return cp.stdout

    def do_create(self, container: str, password: str, name: str) -> DomainResult:
        pw = gen_password()
        self._exec(container, password, (
            f"CREATE DATABASE `{name}`;\n"
            f"CREATE USER '{name}'@'%' IDENTIFIED BY '{pw}';\n"
            f"GRANT ALL ON `{name}`.* TO '{name}'@'%';\n"
        ))
        port = os.environ.get("PORT", "9030")
        uri = f"mysql+mysqlconnector://{name}:{pw}@localhost:{port}/{name}"
        return DomainResult(db=name, user=name, password=pw, sqlalchemy_uri=uri)

    def do_delete(self, container: str, password: str, name: str) -> None:
        self._exec(container, password, (
            f"DROP DATABASE IF EXISTS `{name}`;\n"
            f"DROP USER IF EXISTS '{name}'@'%';\n"
        ))

    def do_list(self, container: str, password: str) -> list[str]:
        out = self._exec(container, password, (
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN ('information_schema','_statistics_','sys') "
            "ORDER BY schema_name;\n"
        ))
        return [line for line in (l.strip() for l in out.splitlines()) if line]
