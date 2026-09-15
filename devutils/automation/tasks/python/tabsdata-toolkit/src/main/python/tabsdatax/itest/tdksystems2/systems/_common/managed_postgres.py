#
# Copyright 2026 Tabsdata Inc.
#

"""``ManagedPostgres`` — the shape a provider-managed PostgreSQL instance shares.

AWS RDS and GCP Cloud SQL differ in almost nothing tdx cares about: both are
externally owned (there is nothing to run, so ``create`` reads the connection
from the environment and probes it), and in both the master user is *not* a
superuser, which is what shapes the domain DDL.

A domain is still a DATABASE owned by a dedicated role, but a non-superuser
master can only create a database owned by a role it is a member of, so the owner
role is created and granted to the master *before* the database. The owner also
gets ``CREATEROLE`` so a test can add roles of its own and grant them privileges
on its database. Those roles are the owner's to drop: it is the recorded grantor
of the memberships it handed out, so (PostgreSQL 16+) it cannot be dropped while
they exist -- deleting a domain drops the roles the owner administers between the
database and the owner. A purge that died between those drops used to leave the
owner behind with no database, invisible to a listing of ``pg_database``; owner
roles the master is a member of are listed as domain entities too, so such a
remnant is purged (and never collided with by a create) like any other domain.

That last query is why :attr:`role_prefixes` exists. A managed instance grants
its master service roles of its own -- ``rds_*`` on RDS, ``cloudsql*`` on Cloud
SQL -- which are memberships, not domains. A subclass that forgets its provider's
prefix does not merely list them: ``purge`` drops what it lists.

TLS is per provider. Both verify by default and neither chains to the public
trust store, so what a subclass supplies is the root certificate
(:meth:`_ca_pem`, PEM text) and, where the instance rather than the class decides
it, an ``SSLMODE`` in its meta. Where that certificate cannot be re-fetched on
demand it is the descriptor that holds it, as the certificate itself rather than
a path into somebody's home directory -- the system stays self-contained and the
domain descriptor inherits it. libpq wants a path, so :meth:`_ca_file` writes one
into a temporary directory for exactly as long as the connection needs it. The
cert rides only on tdx's own admin connections: the URIs in a descriptor carry
the mode alone, since a connector-under-test brings its own root
(``$PGSSLROOTCERT``, else ``~/.postgresql/root.crt``).

There is no tdx data plane (``supports_models=False``): a domain is an empty
database for a connector-under-test to fill.
"""

from __future__ import annotations

import tempfile
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from urllib.parse import quote, urlencode

from tabsdatax.itest.tdksystems2 import _venv, options
from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.systems._common.sql import Admin
from tabsdatax.itest.tdksystems2.systems.postgres.backend import Postgres

# libpq's modes, weakest first. The two that check the server's certificate are
# the ones that need a root cert to check it against.
SSL_MODES = ("disable", "allow", "prefer", "require", "verify-ca", "verify-full")
VERIFYING_SSL_MODES = ("verify-ca", "verify-full")

PEM_HEADER = "-----BEGIN CERTIFICATE-----"


class ManagedPostgres(Postgres):
    slug = ""
    is_container = False
    # A container's outside plane is docker-only, but this one has nothing to
    # start and probes over SQL instead, so it needs the driver from the outside
    # too (see _venv).
    sys_env = _venv.ENV_A
    supports_models = False
    supports_data = False
    supports_changes = False

    # The prefix of the `$<PREFIX>__*` variables the connection is read from.
    # Named by each provider, since it reaches its instance through its own.
    env_prefix: str = ""

    # The default posture, used when the system's meta names no SSLMODE.
    ssl_mode = "verify-full"
    # Role-name prefixes the provider owns: memberships the master is given by
    # the service, never domains. 'pg_' is filtered for every provider.
    role_prefixes: tuple[str, ...] = ()
    # Whether the region is part of reaching the instance (RDS names one) or is
    # only recorded (Cloud SQL). Informational means a create is never held up
    # for it.
    region_required: bool = True

    # ---- options --------------------------------------------------------

    def connection_options(self) -> tuple[options.Option, ...]:
        """Host, credentials and region, all from ``$<PREFIX>__*`` (as loaded by
        ``envx set-secrets``). A subclass appends whatever else it needs."""
        p = self.env_prefix
        region_doc = f"env ${p}__REGION" + (
            "" if self.region_required else " (informational)"
        )
        return (
            options.Option("HOSTNAME", "hostname", options.ENV, f"env ${p}__HOSTNAME"),
            options.Option("PORT", "port", options.ENV, f"env ${p}__PORT"),
            options.Option("USERNAME", "username", options.ENV, f"env ${p}__USERNAME"),
            options.Option(
                "PASSWORD",
                "password",
                options.ENV,
                f"env ${p}__PASSWORD",
                secret=True,
            ),
            options.Option(
                "REGION",
                "region",
                options.ENV,
                region_doc,
                required=self.region_required,
            ),
        )

    def options(self) -> tuple[options.Option, ...]:
        return self.connection_options()

    def docker_images(self) -> list[tuple[str, str]]:
        return []

    # ---- connection -----------------------------------------------------

    def _sslmode(self, meta: dict[str, str]) -> str:
        return meta.get("SSLMODE") or self.ssl_mode

    def _ca_pem(self, meta: dict[str, str]) -> str:
        """The root certificate a verifying mode validates against, as PEM text,
        or ``""`` when there is none. Provider-specific: a bundle to download, a
        certificate the descriptor carries."""
        return ""

    @contextmanager
    def _ca_file(self, admin: Admin):
        """`admin` with ``sslrootcert`` pointing at its certificate on disk, for
        the length of the block and no longer.

        ``sslrootcert`` takes a path, but a path is not what tdx has -- it has the
        certificate. Nothing is left behind between commands: the file is written
        into a temporary directory that goes when the connection that needed it
        does, so there is no cache to go stale after a CA rotation and nothing for
        a destroy to clean up."""
        if not admin.ca_pem:
            yield admin
            return
        with tempfile.TemporaryDirectory(prefix="tdx-ca-") as tmp:
            path = Path(tmp) / "server-ca.pem"
            path.write_text(admin.ca_pem, encoding="utf-8")
            yield replace(admin, sslrootcert=str(path))

    def _run_admin(self, admin: Admin, statements: list[str]) -> None:
        with self._ca_file(admin) as connectable:
            super()._run_admin(connectable, statements)

    def _query_admin(self, admin: Admin, query: str) -> list[str]:
        with self._ca_file(admin) as connectable:
            return super()._query_admin(connectable, query)

    def _with_ssl(self, url: str, sslmode: str, sslrootcert: str = "") -> str:
        params = {"sslmode": sslmode}
        if sslrootcert:
            params["sslrootcert"] = sslrootcert
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}{urlencode(params)}"

    def sqlalchemy_url(
        self, host: str, port: str, db: str = "", user: str = "", password: str = ""
    ) -> str:
        # The credentials are percent-encoded because SQLAlchemy decodes what it
        # parses out of a URL: a master password may contain a literal '%NN',
        # which would otherwise arrive at the server as the single byte it
        # escapes. quote() (not quote_plus) — a space must be '%20', since
        # SQLAlchemy unquotes with unquote(). TLS is added by the caller: it
        # differs between an admin connection and a recorded URI.
        creds = f"{quote(user, safe='')}:{quote(password, safe='')}@" if user else ""
        tail = f"/{db}" if db else ""
        return f"{self.driver}://{creds}{host}:{port}{tail}"

    def _admin_url(self, admin: Admin) -> str:
        return self._with_ssl(
            super()._admin_url(admin),
            admin.sslmode or self.ssl_mode,
            admin.sslrootcert,
        )

    def _location(self, meta: dict[str, str]) -> str:
        return f"postgresql://{meta.get('HOSTNAME', '')}:{meta.get('PORT', '')}"

    def _admin(self, sys_meta: dict[str, str]) -> Admin:
        return Admin(
            host=sys_meta.get("HOSTNAME", ""),
            port=sys_meta.get("PORT", ""),
            user=sys_meta.get("USERNAME", ""),
            password=sys_meta.get("PASSWORD", ""),
            sslmode=self._sslmode(sys_meta),
            ca_pem=self._ca_pem(sys_meta),
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
        mode = self._sslmode(sys_meta)
        meta["REGION"] = sys_meta.get("REGION", "")
        # Recorded so a domain descriptor is enough to reach the instance on its
        # own -- destroy runs from it, without the system's.
        meta["SSLMODE"] = mode
        if sys_meta.get("SERVER_CA_CERT"):
            meta["SERVER_CA_CERT"] = sys_meta["SERVER_CA_CERT"]
        for key in ("SQLALCHEMY_URI", "URI"):
            meta[key] = self._with_ssl(meta[key], mode)
        return meta

    def destroy_domain(self, domain_meta: dict[str, str]) -> None:
        self._do_delete(
            Admin(
                host=domain_meta.get("HOST", ""),
                port=domain_meta.get("PORT", ""),
                user=domain_meta.get("SYS_ADMIN_USER", ""),
                password=domain_meta.get("SYS_ADMIN_PASSWORD", ""),
                sslmode=self._sslmode(domain_meta),
                ca_pem=self._ca_pem(domain_meta),
            ),
            domain_meta["DB"],
        )

    def create_domain_sql(self, db: str, user: str, password: str) -> list[str]:
        return [
            f"CREATE USER \"{user}\" WITH PASSWORD '{password}' CREATEROLE",
            f'GRANT "{user}" TO CURRENT_USER',
            f'CREATE DATABASE "{db}" OWNER "{user}"',
            f'GRANT ALL PRIVILEGES ON DATABASE "{db}" TO "{user}"',
        ]

    def delete_domain_sql(self, name: str) -> list[str]:
        # No '%' in any statement: the driver would read it as a placeholder.
        user = self._login(name)
        drop_administered_roles = (
            "DO $$ DECLARE r text; BEGIN "
            "FOR r IN "
            "SELECT g.rolname FROM pg_auth_members m "
            "JOIN pg_roles g ON g.oid = m.roleid "
            "JOIN pg_roles o ON o.oid = m.member "
            f"WHERE o.rolname = '{user}' AND m.admin_option "
            "LOOP EXECUTE 'DROP ROLE ' || quote_ident(r); END LOOP; "
            "END $$"
        )
        return [
            f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)',
            drop_administered_roles,
            f'DROP USER IF EXISTS "{user}"',
        ]

    def list_schemas_sql(self) -> str:
        skips = "".join(
            f"AND NOT starts_with(r.rolname, '{p}') "
            for p in (*self.role_prefixes, "pg_")
        )
        return (
            "SELECT datname FROM pg_database "
            "WHERE datistemplate = false AND datname <> 'postgres' "
            "UNION "
            "SELECT r.rolname FROM pg_auth_members m "
            "JOIN pg_roles r ON r.oid = m.roleid "
            "WHERE m.member = (SELECT oid FROM pg_roles WHERE rolname = CURRENT_USER) "
            f"{skips}"
            "ORDER BY 1"
        )
