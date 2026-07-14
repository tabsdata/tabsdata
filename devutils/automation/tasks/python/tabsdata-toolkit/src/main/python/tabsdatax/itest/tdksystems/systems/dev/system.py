#
# Copyright 2026 Tabsdata Inc.
#

"""Development (no-op) system — backs the `tabsdata-conn-dev` connectors
(`DataGenSrc` / `TrashDest`), which need no external resource.

Every lifecycle verb is a no-op: create provisions nothing and bakes no
settings, destroy tears nothing down, status is always healthy. It exists so a
dev system + domain can be created, listed and destroyed through the same
`tdx-system` / `tdx-domain` tooling as the real backends — for consistency and
uniform management in CI and IntTest. It has no connection options, no data
plane (`supports_models = False`), and its domain creates no entity container
(`domain_is_named = False`, like salesforce)."""

from __future__ import annotations

from ..base import (
    CreateDomainSpec,
    CreateSystemSpec,
    DomainContext,
    Health,
    Setting,
    System,
    SystemContext,
)


class DevSystem(System):
    slug = "dev"
    label = "Development"
    category = "dev"
    kind = "dev"
    supports_pause = False
    staging_root = False
    domain_is_named = False  # a dev domain creates no external entity
    supports_sql = False
    supports_models = False  # no --model / --data
    options = ()
    domain_options = ()
    domain_flag = ""  # no entity container, so no system-terminology term

    def plan_system(self, spec: CreateSystemSpec) -> dict[str, Setting]:
        return {}

    def provision_system(
        self, spec: CreateSystemSpec, settings: dict[str, Setting]
    ) -> None:
        return None

    def destroy_system(self, ctx: SystemContext) -> None:
        return None

    def system_status(self, ctx: SystemContext) -> Health:
        return Health(True, "dev system (no external resource)")

    def plan_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec
    ) -> dict[str, Setting]:
        return {}

    def provision_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec, settings: dict[str, Setting]
    ) -> None:
        return None

    def destroy_domain(self, ctx: DomainContext) -> None:
        return None

    def domain_status(self, ctx: DomainContext) -> Health:
        return Health(True, f"dev domain '{ctx.name}' (no external resource)")
