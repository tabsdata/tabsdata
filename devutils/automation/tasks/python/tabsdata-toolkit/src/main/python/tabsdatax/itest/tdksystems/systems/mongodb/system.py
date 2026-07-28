#
# Copyright 2026 Tabsdata Inc.
#

"""MongoDB. Domain == database with a dbOwner user; no SQL."""

from __future__ import annotations

import os
import platform
import subprocess

from ...errors import DatagenError
from ...shell import docker_run
from .._container import AdminConn, ContainerBase, DomainContext, DomainResult


class MongoDBSystem(ContainerBase):
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
        return (
            subprocess.run(
                [
                    "docker",
                    "exec",
                    container,
                    "mongosh",
                    "--quiet",
                    "-u",
                    self.system_user,
                    "-p",
                    password,
                    "--authenticationDatabase",
                    "admin",
                    "--eval",
                    "db.adminCommand({ping:1})",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            ).returncode
            == 0
        )

    def _client(self, admin: AdminConn):
        from pymongo import MongoClient

        return MongoClient(
            host=admin.host,
            port=int(admin.port),
            username=admin.user,
            password=admin.password,
            authSource="admin",
            serverSelectionTimeoutMS=10000,
        )

    def domain_result(self, admin: AdminConn, name: str, password: str) -> DomainResult:
        uri = f"mongodb://{name}:{password}@{admin.host}:{admin.port}/{name}"
        return DomainResult(db=name, user=name, password=password, sqlalchemy_uri=uri)

    def do_create(self, admin: AdminConn, result: DomainResult) -> None:
        client = self._client(admin)
        try:
            db = client[result.db]
            db.command(
                "createUser", result.user, pwd=result.password, roles=["dbOwner"]
            )
            db.create_collection("_init")  # materialise DB so it lists
        except Exception as exc:
            raise DatagenError(f"mongodb create '{result.db}' failed: {exc}") from exc
        finally:
            client.close()

    def do_delete(self, admin: AdminConn, name: str) -> None:
        client = self._client(admin)
        try:
            db = client[name]
            try:
                db.command("dropUser", name)
            except Exception as exc:  # user may not exist — best-effort
                print(f">>> mongodb dropUser {name}: {exc}")
            client.drop_database(name)
        except Exception as exc:
            raise DatagenError(f"mongodb delete '{name}' failed: {exc}") from exc
        finally:
            client.close()

    def do_list(self, admin: AdminConn) -> list[str]:
        client = self._client(admin)
        try:
            names = client.list_database_names()
        except Exception as exc:
            raise DatagenError(f"mongodb list failed: {exc}") from exc
        finally:
            client.close()
        return [n for n in names if n not in ("admin", "local", "config")]

    def reset_domain(self, ctx: DomainContext) -> None:
        """Drop every (non-system) collection in the domain database, REUSING
        the database, its user and password — the entity container is emptied
        without minting new credentials; `recreate` then replays the data."""
        client = self._client(self._admin_from_domain_meta(ctx.meta))
        # The database is the (possibly prefixed) external entity name recorded
        # as DB, not the logical descriptor name.
        dbname = ctx.meta.get("DB") or ctx.name
        try:
            db = client[dbname]
            for coll in db.list_collection_names():
                if not coll.startswith("system."):
                    db.drop_collection(coll)
        except Exception as exc:  # noqa: BLE001
            raise DatagenError(f"mongodb reset '{dbname}' failed: {exc}") from exc
        finally:
            client.close()
