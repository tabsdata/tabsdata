#
# Copyright 2026 Tabsdata Inc.
#

"""Salesforce backend — a credential-only org (no container, no schema).

A domain is a clone of the org credentials (Salesforce has no per-domain entity
container). ``status`` is a lenient HTTPS reachability probe on the org URL (any
response means the host answered). Config comes from the ``SALESFORCE__``
environment (URL / USERNAME / PASSWORD / TOKEN).

Note: v1 also *loads* sample data (snowfakery -> Salesforce Bulk API, in a
dedicated venv, driven by per-model CumulusCI recipe/mapping files); that data
plane is a separate large port and is not included here — this provisions the
org credentials for a connector-under-test.
"""

from __future__ import annotations

import urllib.error
import urllib.request

from tabsdatax.itest.tdksystems2.systems._common.service import ServiceBackend


class Salesforce(ServiceBackend):
    slug = "salesforce"
    label = "Salesforce"
    kind = "salesforce"
    domain_word = "org"
    env_prefix = "SALESFORCE"
    cred_keys = ("URL", "USERNAME", "PASSWORD", "TOKEN")
    public_creds = frozenset({"URL", "USERNAME"})

    @staticmethod
    def _reachable(url: str) -> bool:
        if not url:
            return False
        try:
            urllib.request.urlopen(url, timeout=10)  # noqa: S310
            return True
        except urllib.error.HTTPError:
            return True  # the host answered (even a 4xx) — it's reachable
        except Exception:  # noqa: BLE001 - connection/DNS failure = unreachable
            return False

    def _status(self, meta: dict[str, str], what: str) -> tuple[bool, str]:
        ok, detail = self._creds_present(meta, what)
        if not ok:
            return ok, detail
        url = meta.get("URL", "")
        reachable = self._reachable(url)
        return reachable, f"{what}: {url} {'reachable' if reachable else 'unreachable'}"

    def system_status(self, meta: dict[str, str]) -> tuple[bool, str]:
        return self._status(meta, f"{self.label} org")

    def domain_status(self, domain_meta: dict[str, str]) -> tuple[bool, str]:
        return self._status(domain_meta, f"{self.label} domain")
