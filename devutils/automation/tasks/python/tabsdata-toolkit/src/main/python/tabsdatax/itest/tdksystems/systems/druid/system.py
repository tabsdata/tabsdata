#
# Copyright 2026 Tabsdata Inc.
#

"""Apache Druid — a compose stack (zookeeper + coordinator / broker / historical
/ middleManager / router). Domain == a datasource.

Druid has no SQL login and no `CREATE`-able schema: a datasource only comes into
existence when data is *ingested* into it, and ingestion reads its input from a
cloud object store. tdx therefore does **not** stage or ingest data itself — the
Druid system/domain carry no cloud configuration. A tdx Druid domain simply
*names* a datasource and records the cluster's HTTP endpoints — the overlord
(ingestion / coordinator API) and the router (SQL queries + console) — plus the
admin credentials; `destroy` drops the datasource through the coordinator API.

`--model` is accepted but only *records* the model on the domain (there is no
schema to create); `--data` is rejected (`supports_data = False`). Staging + the
ingestion that materialises a datasource are the connector-under-test's job,
which reaches an object store through a *separately-created* `s3` / `gcs` /
`azureblob` tdx domain (the same pattern ClickHouse / StarRocks use) — so Druid
is cloud-agnostic and works with whichever staging domain a test pairs in.

The cluster runs with HTTP basic auth (the `druid-basic-security` extension): an
`admin` user is minted at create and exported as USER / PASSWORD, so every API
call — readiness, status, the datasource DELETE, and a connector's SQL / ingest
— authenticates as it. A separate internal-client password lets the Druid
services authenticate to each other; it is passed to the stack but not exported.
"""

from __future__ import annotations

import base64
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from tabsdatax.itest.tdksystems.errors import DatagenError
from tabsdatax.itest.tdksystems.shell import (
    find_free_port,
    gen_password,
    http_ok,
    validate_name,
)
from tabsdatax.itest.tdksystems.systems._containers import ContainersBase
from tabsdatax.itest.tdksystems.systems.base import (
    CreateDomainSpec,
    CreateSystemSpec,
    DomainContext,
    Health,
    Setting,
    SystemContext,
)

_HERE = Path(__file__).resolve().parent
COMPOSE_FILE = str(_HERE / "druid-compose.yaml")
DRUID_IMAGE = f"apache/druid:{os.environ.get('DRUID_IMAGE_VERSION', '31.0.0')}"
ZOOKEEPER_IMAGE = f"zookeeper:{os.environ.get('ZOOKEEPER_IMAGE_VERSION', '3.5.10')}"

# The compose file binds these host ports (chosen at plan time) to the
# coordinator's (8081) and router's (8888) container ports.
COORDINATOR_HOST_PORT_ENV = "TDX_DRUID_COORDINATOR_PORT"
ROUTER_HOST_PORT_ENV = "TDX_DRUID_ROUTER_PORT"

# The bootstrap admin user (druid-basic-security) and the env vars the compose
# file reads its password + the internal-client password from.
ADMIN_USER = "admin"
ADMIN_PASSWORD_ENV = "TDX_DRUID_ADMIN_PASSWORD"
INTERNAL_PASSWORD_ENV = "TDX_DRUID_INTERNAL_PASSWORD"


def _url_port(url: str) -> int:
    port = urllib.parse.urlsplit(url).port
    if not port:
        raise DatagenError(f"no host port in endpoint URL {url!r}")
    return port


class DruidSystem(ContainersBase):
    slug = "druid"
    label = "Apache Druid"
    kind = "druid"
    # `--model` records the model the domain will hold (no schema to create);
    # `--data` is rejected — tdx never stages or ingests, so a datasource
    # materialises only when the connector under test ingests through its own
    # (separately-created) cloud staging domain.
    supports_models = True
    supports_data = False

    image = DRUID_IMAGE
    # apache/druid is amd64-only, so pin it (emulated on arm64); zookeeper is
    # multi-arch and left to resolve natively (empty platform).
    image_platform = "linux/amd64"
    compose_images = ((ZOOKEEPER_IMAGE, ""), (DRUID_IMAGE, image_platform))
    compose_file = COMPOSE_FILE

    system_endpoints = ("OVERLORD_URL", "ROUTER_URL", "USER", "PASSWORD")
    domain_endpoints = ("DATASOURCE", "OVERLORD_URL", "ROUTER_URL", "USER", "PASSWORD")

    # ==================================================================
    # System readiness (compose lifecycle lives in ContainersBase)
    # ==================================================================

    def plan_system(self, spec: CreateSystemSpec) -> dict[str, Setting]:
        settings = super().plan_system(spec)  # PROJECT / IMAGE / OVERLORD / ROUTER
        # Mint the basic-auth credentials: the admin user (exported as
        # USER / PASSWORD) and the internal-client password used only for
        # inter-service auth (baked as a back-ref, not exported).
        settings["USER"] = Setting("USER", value=ADMIN_USER)
        settings["PASSWORD"] = Setting("PASSWORD", value=gen_password())
        settings["INTERNAL_PASSWORD"] = Setting(
            "INTERNAL_PASSWORD", value=gen_password()
        )
        return settings

    def _plan_endpoints(self) -> dict[str, str]:
        coordinator_port = find_free_port()
        router_port = find_free_port()
        while router_port == coordinator_port:
            router_port = find_free_port()
        return {
            "OVERLORD_URL": f"http://localhost:{coordinator_port}",
            "ROUTER_URL": f"http://localhost:{router_port}",
        }

    def _compose_env(self, settings: dict[str, Setting]) -> dict[str, str]:
        overlord = settings["OVERLORD_URL"].value or ""
        router = settings["ROUTER_URL"].value or ""
        return {
            COORDINATOR_HOST_PORT_ENV: str(_url_port(overlord)),
            ROUTER_HOST_PORT_ENV: str(_url_port(router)),
            ADMIN_PASSWORD_ENV: settings["PASSWORD"].value or "",
            INTERNAL_PASSWORD_ENV: settings["INTERNAL_PASSWORD"].value or "",
        }

    def _wait_ready(self, settings: dict[str, Setting], timeout: int) -> None:
        overlord = settings["OVERLORD_URL"].value or ""
        router = settings["ROUTER_URL"].value or ""
        user = settings["USER"].value or ""
        password = settings["PASSWORD"].value or ""
        # Probe with the admin credentials: /status is authenticated, so a 200
        # confirms both that the service is up and that the admin user has been
        # bootstrapped into the metadata store (i.e. auth actually works).
        urls = [f"{overlord}/status", f"{router}/status"]
        deadline = time.monotonic() + timeout
        pending = list(urls)
        while time.monotonic() < deadline:
            pending = [
                u for u in pending if not http_ok(u, user=user, password=password)
            ]
            if not pending:
                return
            time.sleep(2)
        raise DatagenError(
            f"Apache Druid did not become ready within {timeout}s "
            f"(still waiting on: {', '.join(pending)})"
        )

    def _system_health(self, meta: dict[str, str]) -> Health:
        overlord, router = meta.get("OVERLORD_URL", ""), meta.get("ROUTER_URL", "")
        user, password = meta.get("USER", ""), meta.get("PASSWORD", "")
        ok = (
            bool(router)
            and http_ok(f"{router}/status", user=user, password=password)
            and http_ok(f"{overlord}/status", user=user, password=password)
        )
        return Health(
            ok, f"{self.label} router {router} {'ready' if ok else 'not ready'}"
        )

    # ==================================================================
    # Domain lifecycle — a datasource
    # ==================================================================

    def plan_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec
    ) -> dict[str, Setting]:
        datasource = spec.entity_name
        validate_name(datasource, label="datasource")
        # A datasource materialises on ingestion, so there is nothing to create
        # server-side; record its name, the cluster endpoints, and the admin
        # credentials (inherited from the system) for consumers.
        return {
            "DATASOURCE": Setting("DATASOURCE", value=datasource),
            "OVERLORD_URL": Setting(
                "OVERLORD_URL", value=ctx.meta.get("OVERLORD_URL", "")
            ),
            "ROUTER_URL": Setting("ROUTER_URL", value=ctx.meta.get("ROUTER_URL", "")),
            "USER": Setting("USER", value=ctx.meta.get("USER", "")),
            "PASSWORD": Setting("PASSWORD", value=ctx.meta.get("PASSWORD", "")),
        }

    def provision_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec, settings: dict[str, Setting]
    ) -> None:
        # Nothing to provision: a datasource appears only when data is ingested.
        return None

    def destroy_domain(self, ctx: DomainContext) -> None:
        overlord = ctx.meta.get("OVERLORD_URL", "")
        user, password = ctx.meta.get("USER", ""), ctx.meta.get("PASSWORD", "")
        self._delete_datasource(overlord, ctx.meta["DATASOURCE"], user, password)

    def domain_status(self, ctx: DomainContext) -> Health:
        router, ds = ctx.meta.get("ROUTER_URL", ""), ctx.meta.get("DATASOURCE", "")
        user, password = ctx.meta.get("USER", ""), ctx.meta.get("PASSWORD", "")
        ok = bool(router) and http_ok(f"{router}/status", user=user, password=password)
        return Health(
            ok,
            f"{self.label} datasource '{ds}' via {router} "
            f"{'reachable' if ok else 'unreachable'}",
        )

    @staticmethod
    def _delete_datasource(
        overlord_url: str, datasource: str, user: str = "", password: str = ""
    ) -> None:
        """Disable a datasource (mark all its segments unused) via the
        coordinator API. Idempotent: a missing datasource (404) or an already
        gone cluster is treated as success."""
        url = f"{overlord_url}/druid/coordinator/v1/datasources/{datasource}"
        req = urllib.request.Request(url, method="DELETE")  # noqa: S310
        if user:
            token = base64.b64encode(f"{user}:{password}".encode()).decode()
            req.add_header("Authorization", f"Basic {token}")
        try:
            with urllib.request.urlopen(req, timeout=30):  # noqa: S310
                return
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return
            raise DatagenError(
                f"Druid delete datasource '{datasource}' failed: "
                f"HTTP {exc.code} {exc.reason}"
            ) from exc
        except urllib.error.URLError as exc:
            print(
                f">>> Druid cluster unreachable ({exc.reason}); "
                f"datasource '{datasource}' assumed removed"
            )
            return
