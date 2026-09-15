#
# Copyright 2026 Tabsdata Inc.
#

"""GCP Cloud SQL PostgreSQL backend — a managed remote instance, provisioning-only.

The provider half of :class:`ManagedPostgres`, which holds everything this shares
with RDS: nothing to start, a domain is a DATABASE owned by a dedicated role, and
the master (``postgres``, a member of ``cloudsqlsuperuser``) is not a superuser.
``create`` only reads the connection from the ``GCPPOSTGRES__`` environment
(HOSTNAME / PORT / USERNAME / PASSWORD, as loaded by ``envx set-secrets``) and
probes it. REGION and INSTANCE_ID are read the same way but are informational --
the instance is reached at its host, so a create is never held up for either.
INSTANCE_ID is what names the instance to ``gcloud``, so it is what the missing-CA
refusal quotes.

What is Google's own: ``cloudsqladmin`` is Cloud SQL's maintenance database and
``cloudsql*`` are its service roles -- ``cloudsqlsuperuser`` is one the master
holds, so without the prefix filter a listing would offer it as a domain and a
purge would drop it -- and the TLS root.

**TLS.** Nothing about a Cloud SQL certificate can be assumed from the RDS case,
which is why the mode is the system's rather than the class's:

- The leaf is issued by a **per-instance** ``Cloud SQL Server CA``, which lives
  in the instance resource itself (``serverCaCert.cert``, self-signed) -- there
  is no bundle to download the way RDS publishes one. It is fetched once::

      gcloud sql instances describe INSTANCE \
          --format='value(serverCaCert.cert)' > server-ca.pem

  and handed to create either way round: ``--server-ca-cert-path server-ca.pem``,
  or the certificate itself in ``$GCPPOSTGRES__SERVER_CA_CERT`` -- named for the
  field it comes from, so the vault entry and `gcloud` agree. Either
  way what the descriptor stores is the certificate, not the path -- every later
  connection (status, domain create, destroy, purge) needs it, and a path into
  somebody's home directory is not something a domain descriptor can inherit.
  A connection that needs a path gets one written to a temporary directory for
  the length of that connection. The cost is that the stored copy is frozen at
  create: rotating the instance's CA means ``system create --force``, which is
  only a probe.

- The leaf names the instance (CN ``project:instance``, SAN
  ``<uid>.<region>.sql.goog``) and carries **no IP SAN**, so ``verify-full``
  against a public IP fails the name check however good the root is. Hence the
  default of ``verify-ca``, which verifies the chain and skips the name; keep
  ``verify-full`` only with the ``.sql.goog`` name as HOSTNAME.

``--sslmode require`` encrypts without verifying and needs no cert -- available,
never the default.
"""

from __future__ import annotations

from pathlib import Path

from tabsdatax.itest.tdksystems2 import options
from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.systems._common.managed_postgres import (
    PEM_HEADER,
    SSL_MODES,
    VERIFYING_SSL_MODES,
    ManagedPostgres,
)
from tabsdatax.itest.tdksystems2.systems.postgres.backend import Postgres


class GcpPostgres(ManagedPostgres):
    slug = "gcppostgres"
    label = "GCP Cloud SQL PostgreSQL"
    env_prefix = "GCPPOSTGRES"
    # The certificate names the instance, not the host it is reached at.
    ssl_mode = "verify-ca"
    # cloudsqladmin is Cloud SQL's own maintenance database — never a domain.
    system_schemas = Postgres.system_schemas | {"cloudsqladmin"}
    role_prefixes = ("cloudsql",)
    # The instance is reached at its host: the region is recorded, never used.
    region_required = False

    def options(self) -> tuple[options.Option, ...]:
        return (
            *self.connection_options(),
            # Which instance this is, in Google's terms -- `k8sdb` of
            # `project:region:instance`. Not part of reaching it (that is the
            # host), but it is what names the instance to `gcloud`, which is
            # where its server CA comes from, so the refusal below can say the
            # command to run rather than a placeholder.
            options.Option(
                "INSTANCE_ID",
                "instance-id",
                options.ENV,
                f"env ${self.env_prefix}__INSTANCE_ID (the Cloud SQL instance)",
                required=False,
            ),
            options.Option("SSLMODE", "sslmode", options.SYNTH, self.ssl_mode),
            # Two ways in, one meta key out: the certificate itself (an env var
            # holding the PEM, as `envx set-secrets` loads it) or a file to read
            # it from. What is stored is always the PEM.
            options.Option(
                "SERVER_CA_CERT",
                "server-ca-cert",
                options.ENV,
                f"env ${self.env_prefix}__SERVER_CA_CERT (the certificate itself)",
                required=False,
            ),
            options.Option(
                "SERVER_CA_CERT_PATH",
                "server-ca-cert-path",
                options.ENV,
                f"env ${self.env_prefix}__SERVER_CA_CERT_PATH (a file holding it)",
                required=False,
            ),
        )

    def synth_default(self, name: str, sys_name: str) -> str:
        if name == "SSLMODE":
            return self.ssl_mode
        raise TdxError(f"{self.slug}: no synth default for {name}")

    def _ca_pem(self, meta: dict[str, str]) -> str:
        return meta.get("SERVER_CA_CERT", "")

    def domain_meta(
        self, sys_meta: dict[str, str], entity_name: str, password: str
    ) -> dict[str, str]:
        meta = super().domain_meta(sys_meta, entity_name, password)
        # Recorded beside the region: which instance a domain is a database in
        # is not derivable from a host that may be an IP.
        meta["INSTANCE_ID"] = sys_meta.get("INSTANCE_ID", "")
        return meta

    def plan_system(
        self, name: str, overrides: dict[str, str] | None = None
    ) -> dict[str, str]:
        meta = super().plan_system(name, overrides)
        self._read_ca(meta)
        self._check_ssl(meta)
        return meta

    def _read_ca(self, meta: dict[str, str]) -> None:
        """Resolve the two inputs into ``SERVER_CA_CERT`` -- the certificate itself,
        which is what the descriptor keeps. A path is read here, at create, so
        nothing later depends on that file still being there (or on this
        machine)."""
        pem, path = meta.get("SERVER_CA_CERT", ""), meta.get("SERVER_CA_CERT_PATH", "")
        if pem and path:
            raise TdxError(
                f"{self.slug}: specify the server CA once — --server-ca-cert (the "
                f"PEM) or --server-ca-cert-path (a file holding it), not both"
            )
        if path:
            src = Path(path).expanduser()
            if not src.is_file():
                raise TdxError(
                    f"{self.slug}: --server-ca-cert-path '{path}' is not a file"
                )
            pem = src.read_text(encoding="utf-8")
        if pem and not pem.lstrip().startswith(PEM_HEADER):
            where = f"--server-ca-cert-path '{path}'" if path else "--server-ca-cert"
            raise TdxError(f"{self.slug}: {where} is not a PEM certificate")
        # Canonical form: a file keeps its trailing newline and `$(cat ...)` eats
        # it, and the same certificate arriving two ways must not become two
        # stored values (and two content-addressed files).
        meta["SERVER_CA_CERT"] = f"{pem.strip()}\n" if pem else ""

    def _check_ssl(self, meta: dict[str, str]) -> None:
        """Refuse a TLS setting the instance cannot honour, at create time --
        libpq reports both of these as a bare ``certificate verify failed`` much
        later, and neither is guessable from that."""
        mode = self._sslmode(meta)
        if mode not in SSL_MODES:
            raise TdxError(
                f"{self.slug}: unknown sslmode '{mode}' — one of: "
                f"{', '.join(SSL_MODES)}"
            )
        if mode not in VERIFYING_SSL_MODES:
            return
        if not meta.get("SERVER_CA_CERT"):
            raise TdxError(
                f"{self.slug}: sslmode '{mode}' verifies the server, so it needs "
                f"the instance's own CA — Cloud SQL signs each instance with a CA "
                f"of its own, and there is no bundle to download. Fetch it once:\n"
                f"  gcloud sql instances describe "
                f"{meta.get('INSTANCE_ID') or 'INSTANCE'} "
                f"--format='value(serverCaCert.cert)' > server-ca.pem\n"
                f"then pass --server-ca-cert-path server-ca.pem (or hand the PEM "
                f"itself in ${self.env_prefix}__SERVER_CA_CERT).\n"
                f"(or --sslmode require to encrypt without verifying the server)"
            )
