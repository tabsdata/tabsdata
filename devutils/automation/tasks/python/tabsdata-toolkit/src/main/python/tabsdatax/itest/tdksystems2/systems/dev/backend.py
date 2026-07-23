#
# Copyright 2026 Tabsdata Inc.
#

"""Development — a fully no-op system (no external resource, no credentials).

Exists so a dev system + domain can be created / listed / destroyed through the
same tooling for CI consistency; it backs the tabsdata dev connectors.
"""

from __future__ import annotations

from tabsdatax.itest.tdksystems2.systems._common.service import ServiceBackend


class Dev(ServiceBackend):
    slug = "dev"
    label = "Development"
    kind = "dev"
    domain_word = "domain"

    def system_status(self, meta: dict[str, str]) -> tuple[bool, str]:
        return True, "dev system (no external resource)"

    def domain_status(self, domain_meta: dict[str, str]) -> tuple[bool, str]:
        return True, "dev domain (no external resource)"
