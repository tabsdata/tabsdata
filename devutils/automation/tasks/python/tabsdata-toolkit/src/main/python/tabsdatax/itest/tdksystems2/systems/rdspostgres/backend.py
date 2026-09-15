#
# Copyright 2026 Tabsdata Inc.
#

"""AWS RDS PostgreSQL backend — a managed remote instance, provisioning-only.

The provider half of :class:`ManagedPostgres`, which holds everything this shares
with Cloud SQL: nothing to start, a domain is a DATABASE owned by a dedicated
role, and the master is not a superuser. ``create`` only reads the connection
from the ``RDSPOSTGRES__`` environment (HOSTNAME / PORT / USERNAME / PASSWORD /
REGION, as loaded by ``envx set-secrets``) and probes it.

What is AWS's own: ``rdsadmin`` is RDS's maintenance database and ``rds_*`` are
its service roles -- neither is ever a domain -- and the TLS root. RDS server
certificates chain to Amazon's own root CAs, which are not in the public trust
store, so ``sslmode=verify-full`` validates against the AWS global bundle,
downloaded the first time a run needs it. Nothing is kept between runs, so the
bundle is always a current one (AWS rotates these); that throwaway path stays out
of the descriptor's URIs.
"""

from __future__ import annotations

import urllib.request

from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.systems._common.managed_postgres import (
    PEM_HEADER,
    ManagedPostgres,
)
from tabsdatax.itest.tdksystems2.systems.postgres.backend import Postgres

CA_BUNDLE_URL = "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"


class RdsPostgres(ManagedPostgres):
    slug = "rdspostgres"
    label = "AWS RDS PostgreSQL"
    env_prefix = "RDSPOSTGRES"
    ssl_mode = "verify-full"
    # rdsadmin is RDS's own maintenance database — never a domain.
    system_schemas = Postgres.system_schemas | {"rdsadmin"}
    role_prefixes = ("rds_",)

    _ca_bundle: str = ""

    def _ca_pem(self, meta: dict[str, str]) -> str:
        """The RDS root CA bundle ``sslmode=verify-full`` validates against:
        downloaded the first time a run needs it, held for the rest of that run
        and never kept beyond it, so the bundle is always a current one."""
        if self._ca_bundle:
            return self._ca_bundle
        print(f">>> fetching the RDS CA bundle from {CA_BUNDLE_URL} …", flush=True)
        try:
            with urllib.request.urlopen(CA_BUNDLE_URL, timeout=30) as resp:  # noqa: S310
                pem = resp.read().decode("utf-8")
        except OSError as exc:
            raise TdxError(
                f"could not fetch the RDS CA bundle from {CA_BUNDLE_URL} "
                f"(needed for sslmode={self.ssl_mode}): {exc}"
            ) from exc
        if not pem.lstrip().startswith(PEM_HEADER):
            raise TdxError(f"{CA_BUNDLE_URL} did not return a PEM bundle")
        self._ca_bundle = pem
        return pem
