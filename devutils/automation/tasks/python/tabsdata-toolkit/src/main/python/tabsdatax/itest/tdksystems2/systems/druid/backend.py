#
# Copyright 2026 Tabsdata Inc.
#

"""Apache Druid backend — a docker-compose stack; a domain is a datasource.

The stack (zookeeper + coordinator/broker/historical/middleManager/router) runs
with HTTP basic auth: an ``admin`` user is minted at create and every API call
authenticates as it. A datasource materialises only on ingestion (the
connector-under-test's job, via a separately-created cloud staging domain), so
``--model`` is merely recorded and there is no ``create-data`` (supports_data =
False); ``destroy`` disables the datasource through the coordinator API.
"""

from __future__ import annotations

import base64
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from tabsdatax.itest.tdksystems2 import datagen
from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.shell import gen_password, http_ok
from tabsdatax.itest.tdksystems2.systems._common.containers import ContainersBackend

_ADMIN_USER = "admin"
_SYS_DATASOURCES = frozenset({"wikipedia"})  # tutorial datasource, never a domain


class Druid(ContainersBackend):
    slug = "druid"
    label = "Apache Druid"
    kind = "druid"
    weight = 3  # multi-container compose stack — schedule first
    domain_word = "datasource"
    max_entity_name_length = 255
    supports_models = True  # records the model; no schema to create
    supports_data = False  # a datasource only materialises on ingestion

    compose_file = str(Path(__file__).parent / "druid-compose.yaml")
    compose_images = (
        ("zookeeper:3.5.10", ""),
        ("apache/druid:31.0.0", "linux/amd64"),
    )

    # ---- compose stack hooks -------------------------------------------

    def plan_endpoints(self) -> dict[str, str]:
        coordinator = self.next_free_port()
        router = self.next_free_port()
        while router == coordinator:
            router = self.next_free_port()
        return {
            "IMAGE": next(img for img, _ in self.compose_images if "druid" in img),
            "OVERLORD_URL": f"http://localhost:{coordinator}",
            "ROUTER_URL": f"http://localhost:{router}",
            "USER": _ADMIN_USER,
            "PASSWORD": gen_password(),
            "INTERNAL_PASSWORD": gen_password(),
        }

    @staticmethod
    def _port(url: str) -> str:
        return str(urllib.parse.urlsplit(url).port or "")

    def compose_env(self, meta: dict[str, str]) -> dict[str, str]:
        return {
            "TDX_DRUID_COORDINATOR_PORT": self._port(meta["OVERLORD_URL"]),
            "TDX_DRUID_ROUTER_PORT": self._port(meta["ROUTER_URL"]),
            "TDX_DRUID_ADMIN_PASSWORD": meta.get("PASSWORD", ""),
            "TDX_DRUID_INTERNAL_PASSWORD": meta.get("INTERNAL_PASSWORD", ""),
        }

    def wait_ready(self, meta: dict[str, str], timeout: int) -> None:
        user, password = meta.get("USER", ""), meta.get("PASSWORD", "")
        pending = [f"{meta['OVERLORD_URL']}/status", f"{meta['ROUTER_URL']}/status"]
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            pending = [
                u for u in pending if not http_ok(u, user=user, password=password)
            ]
            if not pending:
                return
            time.sleep(3)
        raise TdxError(
            f"Apache Druid did not become ready within {timeout}s "
            f"(still waiting on: {', '.join(pending)})"
        )

    def system_health(self, meta: dict[str, str]) -> tuple[bool, str]:
        router, overlord = meta.get("ROUTER_URL", ""), meta.get("OVERLORD_URL", "")
        user, password = meta.get("USER", ""), meta.get("PASSWORD", "")
        ok = (
            bool(router)
            and http_ok(f"{router}/status", user=user, password=password)
            and http_ok(f"{overlord}/status", user=user, password=password)
        )
        return ok, f"{self.label} router {router} {'ready' if ok else 'not ready'}"

    def plan_system(
        self, name: str, overrides: dict[str, str] | None = None
    ) -> dict[str, str]:
        meta = super().plan_system(name, overrides)
        meta["LOCATION"] = meta.get("ROUTER_URL", name)
        return meta

    # ---- data plane: model recorded only (no ingestion by tdx) ---------

    def has_model(self, model: str) -> bool:
        return model in datagen.list_models()

    def available_models(self) -> list[str]:
        return datagen.list_models()

    def install_schema(
        self, meta: dict[str, str], model: str, *, force: bool = False
    ) -> None:
        print(">>> druid: datasource materialises on ingestion; model recorded only")

    # ---- domain lifecycle: a datasource --------------------------------

    def new_domain_password(self) -> str:
        return ""

    def domain_meta(
        self, sys_meta: dict[str, str], entity_name: str, password: str
    ) -> dict[str, str]:
        return {
            "DATASOURCE": entity_name,
            "OVERLORD_URL": sys_meta.get("OVERLORD_URL", ""),
            "ROUTER_URL": sys_meta.get("ROUTER_URL", ""),
            "USER": sys_meta.get("USER", ""),
            "PASSWORD": sys_meta.get("PASSWORD", ""),
            "DB": entity_name,
            "URI": f"druid://{sys_meta.get('ROUTER_URL', '')}/{entity_name}",
        }

    def provision_domain(
        self,
        sys_meta: dict[str, str],
        entity_name: str,
        domain_meta: dict[str, str],
        *,
        force: bool = False,
    ) -> None:
        return None  # a datasource appears only when data is ingested

    def destroy_domain(self, domain_meta: dict[str, str]) -> None:
        self._delete_datasource(
            domain_meta.get("OVERLORD_URL", ""),
            domain_meta["DATASOURCE"],
            domain_meta.get("USER", ""),
            domain_meta.get("PASSWORD", ""),
        )

    def domain_status(self, domain_meta: dict[str, str]) -> tuple[bool, str]:
        router, ds = (
            domain_meta.get("ROUTER_URL", ""),
            domain_meta.get("DATASOURCE", ""),
        )
        user, password = domain_meta.get("USER", ""), domain_meta.get("PASSWORD", "")
        ok = bool(router) and http_ok(f"{router}/status", user=user, password=password)
        return (
            ok,
            f"{self.label} datasource '{ds}' {'reachable' if ok else 'unreachable'}",
        )

    def list_domain_entities(self, sys_meta: dict[str, str]) -> list[str]:
        names = self._list_datasources(
            sys_meta.get("OVERLORD_URL", ""),
            sys_meta.get("USER", ""),
            sys_meta.get("PASSWORD", ""),
        )
        return [n for n in names if n not in _SYS_DATASOURCES]

    def purge_domain_entity(self, sys_meta: dict[str, str], name: str) -> None:
        self._delete_datasource(
            sys_meta.get("OVERLORD_URL", ""),
            name,
            sys_meta.get("USER", ""),
            sys_meta.get("PASSWORD", ""),
        )

    # ---- coordinator API ------------------------------------------------

    @staticmethod
    def _request(url: str, method: str, user: str, password: str):
        req = urllib.request.Request(url, method=method)  # noqa: S310
        if user:
            token = base64.b64encode(f"{user}:{password}".encode()).decode()
            req.add_header("Authorization", f"Basic {token}")
        return req

    def _list_datasources(self, overlord: str, user: str, password: str) -> list[str]:
        url = f"{overlord}/druid/coordinator/v1/datasources"
        try:
            with urllib.request.urlopen(  # noqa: S310
                self._request(url, "GET", user, password), timeout=30
            ) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            raise TdxError(
                f"Druid list datasources failed: HTTP {exc.code} {exc.reason}"
            ) from exc
        except urllib.error.URLError as exc:
            print(f">>> Druid unreachable ({exc.reason}); no datasources")
            return []
        return [str(d) for d in data] if isinstance(data, list) else []

    def _delete_datasource(
        self, overlord: str, datasource: str, user: str, password: str
    ) -> None:
        url = f"{overlord}/druid/coordinator/v1/datasources/{datasource}"
        try:
            with urllib.request.urlopen(  # noqa: S310
                self._request(url, "DELETE", user, password), timeout=30
            ):
                return
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return
            raise TdxError(
                f"Druid delete datasource '{datasource}' failed: "
                f"HTTP {exc.code} {exc.reason}"
            ) from exc
        except urllib.error.URLError as exc:
            print(f">>> Druid unreachable ({exc.reason}); '{datasource}' assumed gone")
