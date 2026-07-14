#
# Copyright 2026 Tabsdata Inc.
#

"""Shared credential-only service lifecycle on the unified `System` contract.

Some external systems are keyed API accounts (LLM providers: Anthropic,
OpenAI) — externally owned, 1Password-seeded credentials with nothing to
provision. So `create` / `destroy` are no-ops (the framework just bakes the
resolved credentials into the descriptor), a domain is a credentials clone with
no entity container (`domain_is_named = False`, like salesforce), and there is
no model/data plane (`supports_models = False`). `status` confirms offline that
the descriptor carries every credential it declares — there is no cheap
reachability probe that wouldn't spend a real API call.

A concrete service is just a `slug` / `label` and its `options` (the credential
settings, e.g. `API_KEY`, resolved from `$<SLUG>__<OPT>`).
"""

from __future__ import annotations

from .base import (
    CreateDomainSpec,
    CreateSystemSpec,
    DomainContext,
    Health,
    Setting,
    System,
    SystemContext,
)


class ServiceBase(System):
    category = "service"
    kind = "service"
    domain_is_named = False  # a domain clones creds — no entity container
    supports_sql = False
    supports_models = False  # no schema / data plane
    domain_flag = ""

    # ---- system lifecycle (account is external: nothing to provision) ----

    def plan_system(self, spec: CreateSystemSpec) -> dict[str, Setting]:
        return dict(spec.settings)

    def provision_system(
        self, spec: CreateSystemSpec, settings: dict[str, Setting]
    ) -> None:
        return None

    def destroy_system(self, ctx: SystemContext) -> None:
        return None

    def system_status(self, ctx: SystemContext) -> Health:
        return self._credentials_present(ctx.meta, f"{self.slug} account")

    # ---- domain lifecycle (credentials clone, no entity container) ----

    def plan_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec
    ) -> dict[str, Setting]:
        return self.settings_from_meta(ctx.meta)

    def provision_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec, settings: dict[str, Setting]
    ) -> None:
        return None

    def destroy_domain(self, ctx: DomainContext) -> None:
        return None

    def domain_status(self, ctx: DomainContext) -> Health:
        return self._credentials_present(ctx.meta, f"{self.slug} domain '{ctx.name}'")

    # ---- helpers ----

    def _credentials_present(self, meta: dict[str, str], what: str) -> Health:
        """Offline health: the descriptor carries a non-empty value for every
        declared option (the credentials were baked at create time)."""
        missing = [opt.name for opt in self._all_options() if not meta.get(opt.name)]
        if missing:
            return Health(False, f"{what} missing credentials: {', '.join(missing)}")
        return Health(True, f"{what} credentials present")
