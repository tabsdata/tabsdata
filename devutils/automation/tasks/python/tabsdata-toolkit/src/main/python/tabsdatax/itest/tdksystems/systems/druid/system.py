#
# Copyright 2026 Tabsdata Inc.
#

"""Apache Druid — a compose stack (zookeeper + coordinator / broker / historical
/ middleManager / router). Domain == a datasource.

Druid has no SQL login and no `CREATE`-able schema: a datasource only comes into
existence when data is ingested into it (and ingestion stages files through a
separate object store). A tdx Druid domain therefore *names* a datasource and
records the cluster's HTTP endpoints — the overlord (ingestion / coordinator
API) and the router (SQL queries + console) — and `destroy` drops the datasource
through the coordinator API. Models / data are out of scope (no `--model` /
`--data`); ingestion belongs to the connector under test.

The cluster runs with HTTP basic auth (the `druid-basic-security` extension): an
`admin` user is minted at create and exported as USER / PASSWORD, so every API
call — readiness, status, the datasource DELETE, and a connector's SQL / ingest
— authenticates as it. A separate internal-client password lets the Druid
services authenticate to each other; it is passed to the stack but not exported.
"""

from __future__ import annotations

import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from tabsdatax.itest.tdksystems import druid_ingest
from tabsdatax.itest.tdksystems.errors import DatagenError
from tabsdatax.itest.tdksystems.shell import (
    find_free_port,
    gen_password,
    http_ok,
    validate_name,
)
from tabsdatax.itest.tdksystems.systems._backend import normalise_prefix
from tabsdatax.itest.tdksystems.systems._containers import ContainersBase
from tabsdatax.itest.tdksystems.systems.base import (
    CreateDomainSpec,
    CreateSystemSpec,
    DomainContext,
    Health,
    LoadRequest,
    Option,
    Setting,
    SystemContext,
)
from tabsdatax.itest.tdksystems.systems.s3.backend import S3Backend

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


# A seeded domain ingests one datasource per model table, named
# `<DATASOURCE><SEP><table>` so a single domain can carry several tables and
# `destroy_domain` can find exactly the ones it created.
_DATASOURCE_SEP = "__"


def _table_datasource(base: str, table: str) -> str:
    return f"{base}{_DATASOURCE_SEP}{table}"


def _staging_prefix(sys_path: str, datasource: str) -> str:
    root = (sys_path or "").strip("/")
    return f"{root}/{datasource}" if root else datasource


class DruidSystem(ContainersBase):
    slug = "druid"
    label = "Apache Druid"
    kind = "druid"
    supports_models = True

    # Druid datasources materialise only on ingestion, and native batch
    # ingestion reads its input from an object store the cluster can reach. The
    # tdx machinery stages the generated model parquet to S3 and ingests from
    # there, so the system carries the S3 staging bucket + credentials (pointed
    # at the same $S3__* env vars the s3 system uses).
    staging_root = True  # -> base adds the SYS_PATH (--sys-path) option
    options = (
        Option("BUCKET", "bucket"),
        Option("REGION", "region"),
        Option("ACCESS_KEY", "access-key"),
        Option("SECRET_KEY", "secret-key", secret=True),
    )

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
        # Carry the S3 staging options (resolved from the CLI / $S3__* env) so
        # `load_data` can reach the bucket the datasources are ingested from.
        settings.update(spec.settings)
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
        settings = {
            "DATASOURCE": Setting("DATASOURCE", value=datasource),
            "OVERLORD_URL": Setting(
                "OVERLORD_URL", value=ctx.meta.get("OVERLORD_URL", "")
            ),
            "ROUTER_URL": Setting("ROUTER_URL", value=ctx.meta.get("ROUTER_URL", "")),
            "USER": Setting("USER", value=ctx.meta.get("USER", "")),
            "PASSWORD": Setting("PASSWORD", value=ctx.meta.get("PASSWORD", "")),
        }
        # Inherit the S3 staging options from the system so `load_data` can stage
        # + ingest this datasource's data without re-resolving any env var.
        settings.update(self.settings_from_meta(ctx.meta))
        return settings

    def provision_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec, settings: dict[str, Setting]
    ) -> None:
        # Nothing to provision: a datasource appears only when data is ingested.
        return None

    # ==================================================================
    # Data plane — stage model parquet to S3, ingest one datasource per table
    # ==================================================================

    def load_data(self, ctx: DomainContext, req: LoadRequest) -> int:
        values = self.resolve_values(self.settings_from_meta(ctx.meta))
        bucket = values.get("BUCKET", "")
        if not bucket:
            raise DatagenError("druid load_data needs an S3 staging bucket")

        overlord = ctx.meta.get("OVERLORD_URL", "")
        user, password = ctx.meta.get("USER", ""), ctx.meta.get("PASSWORD", "")
        staging = druid_ingest.S3Staging(
            access_key=values.get("ACCESS_KEY", ""),
            secret_key=values.get("SECRET_KEY", ""),
            region=values.get("REGION", ""),
        )
        prefix = _staging_prefix(values.get("SYS_PATH", ""), ctx.meta["DATASOURCE"])

        # run-order.txt is a SQL-load manifest the ingest path doesn't use; drop
        # it so only parquet is staged (matching the object-store data plane).
        (req.data_dir / "run-order.txt").unlink(missing_ok=True)

        backend = S3Backend(values)
        if req.force:
            try:
                backend.clear(prefix)
            except Exception:  # noqa: BLE001 - best-effort (prefix may be empty)
                pass
        print(f">>> staging parquet -> s3://{bucket}/{normalise_prefix(prefix)} ...")
        backend.upload(prefix, [str(req.data_dir)])

        counts: dict[str, int] = {}
        for parquet in sorted(req.data_dir.glob("*.parquet")):
            table = parquet.stem
            datasource = _table_datasource(ctx.meta["DATASOURCE"], table)
            key = normalise_prefix(prefix) + parquet.name
            uri = f"s3://{bucket}/{key}"
            print(f">>> ingesting datasource '{datasource}' from {uri} ...")
            druid_ingest.ingest_parquet(
                overlord_url=overlord,
                datasource=datasource,
                s3_uris=[uri],
                staging=staging,
                user=user,
                password=password,
            )
            counts[datasource] = 1
        print(f">>> ingested {len(counts)} datasource(s)")
        return len(counts)

    def destroy_domain(self, ctx: DomainContext) -> None:
        overlord = ctx.meta.get("OVERLORD_URL", "")
        user, password = ctx.meta.get("USER", ""), ctx.meta.get("PASSWORD", "")
        datasource = ctx.meta["DATASOURCE"]
        # A seeded domain fans a datasource out per model table
        # (<DATASOURCE>__<table>); drop the bare datasource and every seeded one.
        seeded = self._datasources_with_prefix(
            overlord, f"{datasource}{_DATASOURCE_SEP}", user, password
        )
        for ds in (datasource, *seeded):
            self._delete_datasource(overlord, ds, user, password)

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

    @staticmethod
    def _datasources_with_prefix(
        overlord_url: str, prefix: str, user: str = "", password: str = ""
    ) -> list[str]:
        """The coordinator's datasources whose name starts with `prefix`
        (best-effort: an unreachable cluster yields none)."""
        url = f"{overlord_url}/druid/coordinator/v1/datasources"
        req = urllib.request.Request(url)  # noqa: S310
        if user:
            token = base64.b64encode(f"{user}:{password}".encode()).decode()
            req.add_header("Authorization", f"Basic {token}")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
                names = json.loads(resp.read().decode() or "[]")
        except (urllib.error.URLError, ValueError):
            return []
        return [n for n in names if isinstance(n, str) and n.startswith(prefix)]
