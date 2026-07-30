#
# Copyright 2026 Tabsdata Inc.
#

"""AWS RDS PostgreSQL backend — a managed remote instance, provisioning-only.

A variant of the plain ``postgres`` system: same dialect, and a domain is still a
DATABASE owned by a dedicated role. What differs is that the instance is
externally owned — there is nothing to run, so ``create`` only reads the
connection from the ``RDSPOSTGRES__`` environment (HOSTNAME / PORT / USERNAME /
PASSWORD / REGION, as loaded by ``envx set-secrets``) and probes it.

The RDS master user is not a superuser, which shapes the domain DDL: it can only
create a database owned by a role it is a member of, so the owner role is created
and granted to the master *before* the database. The owner also gets
``CREATEROLE`` so a test can add roles of its own and grant them privileges on
its database.

Every URL is TLS-verified (``sslmode=verify-full``). RDS server certificates
chain to Amazon's own root CAs, which are not in the public trust store, so tdx's
admin connections point ``sslrootcert`` at the AWS global bundle, downloaded to
``/tmp`` the first time a run needs it — nothing is kept between runs, so the
bundle is always a current one (AWS rotates these). That throwaway path stays out
of the descriptor's URIs: a connector-under-test brings its own root cert
(``$PGSSLROOTCERT``, else ``~/.postgresql/root.crt``).

There is no tdx data plane (``supports_models=False``): a domain is an empty
database for a connector-under-test to fill.
"""

from __future__ import annotations

import os
import urllib.request
from pathlib import Path
from urllib.parse import quote, urlencode

from tabsdatax.itest.tdksystems2 import _venv, options
from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.systems._common.sql import Admin
from tabsdatax.itest.tdksystems2.systems.postgres.backend import Postgres

SSL_MODE = "verify-full"
CA_BUNDLE_URL = "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"
CA_BUNDLE_PATH = "/tmp/tdx-rds-global-bundle.pem"
_PEM_HEADER = b"-----BEGIN CERTIFICATE-----"


class RdsPostgres(Postgres):
    slug = "rdspostgres"
    label = "AWS RDS PostgreSQL"
    is_container = False
    # A container's outside plane is docker-only, but this one has nothing to
    # start and probes over SQL instead, so it needs the driver from the outside
    # too (see _venv).
    sys_env = _venv.ENV_A
    supports_models = False
    supports_data = False
    supports_changes = False
    env_prefix = "RDSPOSTGRES"
    # rdsadmin is RDS's own maintenance database — never a domain.
    system_schemas = Postgres.system_schemas | {"rdsadmin"}

    _ca_bundle_path: str = ""

    def options(self) -> tuple[options.Option, ...]:
        return (
            options.Option(
                "HOSTNAME", "hostname", options.ENV, "env $RDSPOSTGRES__HOSTNAME"
            ),
            options.Option("PORT", "port", options.ENV, "env $RDSPOSTGRES__PORT"),
            options.Option(
                "USERNAME", "username", options.ENV, "env $RDSPOSTGRES__USERNAME"
            ),
            options.Option(
                "PASSWORD",
                "password",
                options.ENV,
                "env $RDSPOSTGRES__PASSWORD",
                secret=True,
            ),
            options.Option("REGION", "region", options.ENV, "env $RDSPOSTGRES__REGION"),
        )

    def docker_images(self) -> list[tuple[str, str]]:
        return []

    # ---- connection -----------------------------------------------------

    def ca_bundle(self) -> str:
        """The RDS root CA bundle ``sslmode=verify-full`` validates against, in
        ``/tmp``: downloaded the first time a run needs it, never reused across
        runs. Written through a pid-suffixed temporary so parallel creates
        sharing the path never read a half-written bundle."""
        if self._ca_bundle_path:
            return self._ca_bundle_path
        print(f">>> fetching the RDS CA bundle from {CA_BUNDLE_URL} …", flush=True)
        try:
            with urllib.request.urlopen(CA_BUNDLE_URL, timeout=30) as resp:  # noqa: S310
                pem = resp.read()
        except OSError as exc:
            raise TdxError(
                f"could not fetch the RDS CA bundle from {CA_BUNDLE_URL} "
                f"(needed for sslmode={SSL_MODE}): {exc}"
            ) from exc
        if not pem.startswith(_PEM_HEADER):
            raise TdxError(f"{CA_BUNDLE_URL} did not return a PEM bundle")
        staged = Path(f"{CA_BUNDLE_PATH}.{os.getpid()}")
        staged.write_bytes(pem)
        os.replace(staged, CA_BUNDLE_PATH)
        self._ca_bundle_path = CA_BUNDLE_PATH
        return self._ca_bundle_path

    def sqlalchemy_url(
        self, host: str, port: str, db: str = "", user: str = "", password: str = ""
    ) -> str:
        # The credentials are percent-encoded because SQLAlchemy decodes what it
        # parses out of a URL: an RDS master password may contain a literal
        # '%NN', which would otherwise arrive at the server as the single byte
        # it escapes. quote() (not quote_plus) — a space must be '%20', since
        # SQLAlchemy unquotes with unquote().
        creds = f"{quote(user, safe='')}:{quote(password, safe='')}@" if user else ""
        tail = f"/{db}" if db else ""
        return f"{self.driver}://{creds}{host}:{port}{tail}?sslmode={SSL_MODE}"

    def _admin_url(self, admin: Admin) -> str:
        # The CA bundle rides only on tdx's own admin connections — it is a
        # throwaway /tmp path, so it has no business in the URLs the descriptor
        # hands to a connector-under-test (those carry the sslmode; the CA is
        # the consumer's own $PGSSLROOTCERT / ~/.postgresql/root.crt).
        cert = urlencode({"sslrootcert": self.ca_bundle()})
        return f"{super()._admin_url(admin)}&{cert}"

    def _location(self, meta: dict[str, str]) -> str:
        return f"postgresql://{meta.get('HOSTNAME', '')}:{meta.get('PORT', '')}"

    def _admin(self, sys_meta: dict[str, str]) -> Admin:
        return Admin(
            host=sys_meta.get("HOSTNAME", ""),
            port=sys_meta.get("PORT", ""),
            user=sys_meta.get("USERNAME", ""),
            password=sys_meta.get("PASSWORD", ""),
        )

    def _probe(self, meta: dict[str, str]) -> None:
        self._query_admin(self._admin(meta), "SELECT 1")

    # ---- system lifecycle (the instance is externally owned) ------------

    def plan_system(
        self, name: str, overrides: dict[str, str] | None = None
    ) -> dict[str, str]:
        o = overrides or {}
        meta = {opt.name: o.get(opt.name, "") for opt in self.options()}
        meta["LOCATION"] = self._location(meta)
        return meta

    def provision_system(
        self, meta: dict[str, str], *, force: bool = False
    ) -> dict[str, str]:
        self._probe(meta)
        return {}

    def destroy_system(self, meta: dict[str, str]) -> None:
        return None

    def system_status(self, meta: dict[str, str]) -> tuple[bool, str]:
        try:
            self._probe(meta)
        except TdxError as exc:
            return False, f"cannot reach {self.label}: {exc}"
        return True, f"{self.label} at {self._location(meta)}"

    # ---- domain lifecycle (a database + its owner) ----------------------

    def domain_meta(
        self, sys_meta: dict[str, str], entity_name: str, password: str
    ) -> dict[str, str]:
        meta = super().domain_meta(sys_meta, entity_name, password)
        for key in ("IMAGE", "SYS_CONTAINER", "SYS_CONTAINER_ID"):
            meta.pop(key, None)
        meta["REGION"] = sys_meta.get("REGION", "")
        return meta

    def create_domain_sql(self, db: str, user: str, password: str) -> list[str]:
        return [
            f"CREATE USER \"{user}\" WITH PASSWORD '{password}' CREATEROLE",
            f'GRANT "{user}" TO CURRENT_USER',
            f'CREATE DATABASE "{db}" OWNER "{user}"',
            f'GRANT ALL PRIVILEGES ON DATABASE "{db}" TO "{user}"',
        ]
