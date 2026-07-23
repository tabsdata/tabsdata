#
# Copyright 2026 Tabsdata Inc.
#

"""``ServiceBackend`` — credential-only systems (an API account), plus dev noop.

A service system has no container, no schema and no data plane: create just reads
its API credential(s) from the ``<ENV_PREFIX>__<KEY>`` environment and bakes them
into the encrypted descriptor; a domain is a credential clone. ``status`` is
OFFLINE (there is no cheap probe that wouldn't spend a real API call) — it only
checks the credentials are present. Verbs are gated off by the capability flags
(no models/data/purge/restart), so only create / destroy / status remain.
"""

from __future__ import annotations

from tabsdatax.itest.tdksystems2 import options
from tabsdatax.itest.tdksystems2.errors import TdxError


class ServiceBackend:
    is_container: bool = False
    supports_purge: bool = False
    supports_models: bool = False
    supports_data: bool = False
    supports_changes: bool = False

    slug: str = ""
    label: str = ""
    kind: str = "service"
    weight: int = 1
    domain_word: str = "account"
    max_entity_name_length: int = 255
    env_prefix: str = ""
    cred_keys: tuple[str, ...] = ()  # e.g. ("API_KEY",)
    public_creds: frozenset[str] = frozenset()  # cred_keys that aren't secret

    def docker_images(self) -> list[tuple[str, str]]:
        return []

    def options(self) -> tuple[options.Option, ...]:
        return tuple(
            options.Option(
                key,
                key.lower().replace("_", "-"),
                options.ENV,
                f"env ${self.env_prefix}__{key}",
                secret=key not in self.public_creds,
            )
            for key in self.cred_keys
        )

    # ---- system lifecycle ----

    def plan_system(
        self, name: str, overrides: dict[str, str] | None = None
    ) -> dict[str, str]:
        o = overrides or {}
        return {key: o.get(key, "") for key in self.cred_keys}

    def provision_system(
        self, meta: dict[str, str], *, force: bool = False
    ) -> dict[str, str]:
        return {}

    def destroy_system(self, meta: dict[str, str]) -> None:
        return None

    def system_status(self, meta: dict[str, str]) -> tuple[bool, str]:
        return self._creds_present(meta, f"{self.label} account")

    def restart_system(self, meta: dict[str, str]) -> str:
        raise TdxError(f"{self.slug} is not a container (nothing to restart)")

    def _creds_present(self, meta: dict[str, str], what: str) -> tuple[bool, str]:
        missing = [k for k in self.cred_keys if not meta.get(k)]
        if missing:
            return False, f"{what}: missing credentials {', '.join(missing)}"
        return True, f"{what}: credentials present"

    # ---- domain lifecycle (a credential clone; no external container) ----

    def new_domain_password(self) -> str:
        return ""

    def domain_meta(
        self, sys_meta: dict[str, str], entity_name: str, password: str
    ) -> dict[str, str]:
        return dict(sys_meta)

    def provision_domain(
        self,
        sys_meta: dict[str, str],
        entity_name: str,
        domain_meta: dict[str, str],
        *,
        force: bool = False,
    ) -> None:
        return None

    def destroy_domain(self, domain_meta: dict[str, str]) -> None:
        return None

    def domain_status(self, domain_meta: dict[str, str]) -> tuple[bool, str]:
        return self._creds_present(domain_meta, f"{self.label} domain")

    def list_domain_entities(self, sys_meta: dict[str, str]) -> list[str]:
        return []

    def purge_domain_entity(self, sys_meta: dict[str, str], name: str) -> None:
        return None
