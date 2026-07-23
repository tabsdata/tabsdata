#
# Copyright 2026 Tabsdata Inc.
#

"""MongoDB backend — a docker container; a domain is a database + dbOwner user.

Reuses the container lifecycle from :class:`SqlContainerBackend` (docker run /
readiness / destroy / restart via the env + mongosh hooks) but the domain plane
is pymongo, not SQL. As in v1 there is no tdx data plane (MongoDB is schemaless
and the connector-under-test loads its own documents), so only
create / destroy / status / purge are exposed.
"""

from __future__ import annotations

from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.systems._common import container_info
from tabsdatax.itest.tdksystems2.systems._common.sql import Admin, SqlContainerBackend

_SYSTEM_DBS = frozenset({"admin", "local", "config"})


class Mongodb(SqlContainerBackend):
    slug = "mongodb"
    label = "MongoDB"
    kind = "mongodb"
    domain_word = "database"
    max_entity_name_length = 63
    supports_models = False
    supports_data = False
    supports_changes = False

    image = container_info.MONGODB.ref
    image_platform = container_info.MONGODB.platform
    internal_port = 27017
    root_user = "root"

    def env_vars(self, password: str) -> dict[str, str]:
        return {
            "MONGO_INITDB_ROOT_USERNAME": "root",
            "MONGO_INITDB_ROOT_PASSWORD": password,
        }

    def ready_exec_cmd(self, password: str) -> list[str]:
        return [
            "mongosh",
            "--quiet",
            "-u",
            self.root_user,
            "-p",
            password,
            "--authenticationDatabase",
            "admin",
            "--eval",
            "db.adminCommand({ping:1})",
        ]

    # ---- pymongo domain plane ------------------------------------------

    def _client(self, admin: Admin):
        from pymongo import MongoClient

        return MongoClient(
            host=admin.host,
            port=int(admin.port),
            username=admin.user,
            password=admin.password,
            authSource="admin",
            serverSelectionTimeoutMS=10000,
        )

    def _domain_admin(self, domain_meta: dict[str, str]) -> Admin:
        return Admin(
            host=domain_meta.get("HOST", "localhost"),
            port=domain_meta.get("PORT", ""),
            user=domain_meta.get("SYS_ADMIN_USER", self.root_user),
            password=domain_meta.get("SYS_ADMIN_PASSWORD", ""),
        )

    def domain_meta(
        self, sys_meta: dict[str, str], entity_name: str, password: str
    ) -> dict[str, str]:
        admin = self._admin(sys_meta)
        return {
            "HOST": admin.host,
            "PORT": admin.port,
            "USER": entity_name,
            "PASSWORD": password,
            "DB": entity_name,
            "URI": f"mongodb://{admin.host}:{admin.port}/{entity_name}",
            "IMAGE": sys_meta.get("IMAGE", ""),
            "SYS_CONTAINER": sys_meta.get("CONTAINER", ""),
            "SYS_ADMIN_USER": admin.user,
            "SYS_ADMIN_PASSWORD": admin.password,
        }

    def provision_domain(
        self,
        sys_meta: dict[str, str],
        entity_name: str,
        domain_meta: dict[str, str],
        *,
        force: bool = False,
    ) -> None:
        client = self._client(self._admin(sys_meta))
        try:
            if entity_name in client.list_database_names():
                if not force:
                    raise TdxError(
                        f"database '{entity_name}' already exists (use --force)"
                    )
                self._drop(client, entity_name)
            db = client[entity_name]
            db.command(
                "createUser",
                entity_name,
                pwd=domain_meta["PASSWORD"],
                roles=["dbOwner"],
            )
            db.create_collection("_init")  # materialise the DB so it lists
        except TdxError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise TdxError(f"mongodb create '{entity_name}' failed: {exc}") from exc
        finally:
            client.close()

    def destroy_domain(self, domain_meta: dict[str, str]) -> None:
        client = self._client(self._domain_admin(domain_meta))
        try:
            self._drop(client, domain_meta["DB"])
        finally:
            client.close()

    def list_domain_entities(self, sys_meta: dict[str, str]) -> list[str]:
        client = self._client(self._admin(sys_meta))
        try:
            return [n for n in client.list_database_names() if n not in _SYSTEM_DBS]
        finally:
            client.close()

    def purge_domain_entity(self, sys_meta: dict[str, str], name: str) -> None:
        if name in _SYSTEM_DBS:
            raise TdxError(f"refusing to purge MongoDB system database '{name}'")
        client = self._client(self._admin(sys_meta))
        try:
            self._drop(client, name)
        finally:
            client.close()

    @staticmethod
    def _drop(client, name: str) -> None:
        try:
            client[name].command("dropUser", name)
        except Exception:  # noqa: BLE001 - user may not exist; best-effort
            pass
        client.drop_database(name)
