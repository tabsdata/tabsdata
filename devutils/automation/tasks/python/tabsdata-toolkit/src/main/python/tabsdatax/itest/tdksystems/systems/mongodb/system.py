"""MongoDB. Domain == database with a dbOwner user; no SQL."""
from __future__ import annotations

import os
import platform
import subprocess

from .._container import ContainerBase, DomainResult
from ...errors import DatagenError
from ...shell import docker_run, gen_password


class MongodbSystem(ContainerBase):
    slug = "mongodb"
    label = "MongoDB"
    domain_desc = "a database (with a dedicated user that owns it)"
    domain_flag = "database"
    supports_sql = False
    kind = "mongodb"

    image = f"mongo:{os.environ.get('MONGODB_IMAGE_VERSION', '6.0.21')}"
    image_platform = (
        "linux/arm64" if platform.machine() in ("arm64", "aarch64") else "linux/amd64"
    )
    internal_port = 27017
    system_user = "root"
    password_env_name = "MONGO_INITDB_ROOT_PASSWORD"
    extra_env: list[tuple[str, str]] = [("MONGO_INITDB_ROOT_USERNAME", "root")]
    extra_flags: list[str] = []

    def _docker_run(self, container: str, host_port: int, password: str) -> str:
        return docker_run(self._build_docker_args(container, host_port, password))

    def check_ready(self, container: str, password: str) -> bool:
        return subprocess.run(
            ["docker", "exec", container, "mongosh", "--quiet",
             "-u", self.system_user, "-p", password,
             "--authenticationDatabase", "admin",
             "--eval", "db.adminCommand({ping:1})"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        ).returncode == 0

    def _eval(self, container: str, password: str, db: str, js: str) -> str:
        cp = subprocess.run(
            ["docker", "exec", container, "mongosh", "--quiet",
             "-u", self.system_user, "-p", password,
             "--authenticationDatabase", "admin", db, "--eval", js],
            capture_output=True, text=True,
        )
        if cp.returncode != 0:
            raise DatagenError(f"mongosh failed: {cp.stderr.strip()}")
        return cp.stdout

    def do_create(self, container: str, password: str, name: str) -> DomainResult:
        pw = gen_password()
        # Create user + materialise DB so it shows in listDatabases.
        self._eval(container, password, name, (
            f"db.createUser({{ user: '{name}', pwd: '{pw}', roles: ['dbOwner'] }});\n"
            f"db.createCollection('_init');\n"
        ))
        port = os.environ.get("PORT", "27017")
        uri = f"mongodb://{name}:{pw}@localhost:{port}/{name}"
        return DomainResult(db=name, user=name, password=pw, sqlalchemy_uri=uri)

    def do_delete(self, container: str, password: str, name: str) -> None:
        self._eval(container, password, name, (
            f"try {{ db.dropUser('{name}'); }} catch (e) {{ print('dropUser:', e.message); }}\n"
            "db.dropDatabase();\n"
        ))

    def do_list(self, container: str, password: str) -> list[str]:
        out = self._eval(container, password, "admin", (
            "db.adminCommand({ listDatabases: 1 }).databases"
            ".map(d => d.name)"
            ".filter(n => !['admin','local','config'].includes(n))"
            ".forEach(n => print(n));"
        ))
        return [line for line in (l.strip() for l in out.splitlines()) if line]
