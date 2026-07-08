#
# Copyright 2026 Tabsdata Inc.
#

"""StarRocks all-in-one image. MySQL-protocol-compatible on 9030."""

from __future__ import annotations

import os
import platform
import time

from ...errors import DatagenError
from ...shell import (
    docker_exec,
    docker_exec_check,
    docker_run,
    gen_password,
)
from .._container import AdminConn, ContainerBase, DomainResult


class StarrocksSystem(ContainerBase):
    slug = "starrocks"
    label = "StarRocks"
    domain_desc = "a database (with a dedicated user that owns it)"
    domain_flag = "database"
    supports_sql = True
    uses_file_staging = True

    # Run the native image for the host arch. The x86-64 BE needs AVX2, so
    # forcing linux/amd64 under emulation on Apple Silicon (arm64) crash-loops
    # it; the arm64 build has no such dependency (as tabsdata-conn-starrocks
    # relies on). Pinned to the latest 3.x (3.4.8), which ships an arm64 variant.
    image = (
        f"starrocks/allin1-ubuntu:{os.environ.get('STARROCKS_IMAGE_VERSION', '3.4.8')}"
    )
    image_platform = (
        "linux/arm64" if platform.machine() in ("arm64", "aarch64") else "linux/amd64"
    )
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
        # Fail fast if the BE (backend) crash-loops (supervisord marks it FATAL)
        # rather than waiting out the full timeout. The usual cause is running
        # the x86-64 BE under emulation (it needs AVX2): this image now
        # auto-selects the native arch, so on Apple Silicon check that the
        # arm64 variant was pulled (not forced amd64 via STARROCKS/-platform).
        cp = docker_exec(container, "supervisorctl", "status", "beservice")
        if "FATAL" in (cp.stdout or "") or "EXITED" in (cp.stdout or ""):
            return (
                "the StarRocks BE failed to start (beservice FATAL). The x86-64 "
                "BE needs AVX2, so it can't run under x86 emulation on Apple "
                "Silicon — this image auto-selects the native arm64 build; check "
                f"`docker logs {container}` for the underlying cause."
            )
        return None

    def check_ready(self, container: str, password: str) -> bool:
        # FE accepts SELECT 1, AND every BE in SHOW BACKENDS reports Alive=true.
        if not docker_exec_check(
            container,
            "mysql",
            "-h",
            "127.0.0.1",
            "-P",
            "9030",
            "-u",
            "root",
            f"--password={password}",
            "-e",
            "SELECT 1",
        ):
            return False
        cp = docker_exec(
            container,
            "mysql",
            "-h",
            "127.0.0.1",
            "-P",
            "9030",
            "-u",
            "root",
            f"--password={password}",
            "-BNe",
            "SHOW BACKENDS",
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
                container,
                "mysql",
                "-h",
                "127.0.0.1",
                "-P",
                "9030",
                "-u",
                "root",
                f"--password={password}",
                input='admin set frontend config ("default_replication_num" = "1");\n',
            )
            if cp.returncode == 0:
                return
            time.sleep(2)
        raise DatagenError("could not set default_replication_num=1 on StarRocks FE")

    def _admin_url(self, admin: AdminConn) -> str:
        return (
            f"mysql+mysqlconnector://{admin.user}:{admin.password}"
            f"@{admin.host}:{admin.port}"
        )

    def do_create(self, admin: AdminConn, name: str) -> DomainResult:
        pw = gen_password()
        self._run_admin_sql(
            admin,
            [
                f"CREATE DATABASE `{name}`",
                f"CREATE USER '{name}'@'%' IDENTIFIED BY '{pw}'",
                f"GRANT ALL ON `{name}`.* TO '{name}'@'%'",
            ],
        )
        uri = f"mysql+mysqlconnector://{name}:{pw}@{admin.host}:{admin.port}/{name}"
        return DomainResult(db=name, user=name, password=pw, sqlalchemy_uri=uri)

    def do_delete(self, admin: AdminConn, name: str) -> None:
        self._run_admin_sql(
            admin,
            [
                f"DROP DATABASE IF EXISTS `{name}`",
                f"DROP USER IF EXISTS '{name}'@'%'",
            ],
        )

    def do_list(self, admin: AdminConn) -> list[str]:
        return self._query_admin_names(
            admin,
            (
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name NOT IN ('information_schema','_statistics_','sys') "
                "ORDER BY schema_name"
            ),
        )
