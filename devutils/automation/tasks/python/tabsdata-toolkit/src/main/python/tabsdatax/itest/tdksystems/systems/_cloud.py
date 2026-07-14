#
# Copyright 2026 Tabsdata Inc.
#

"""Shared object-store lifecycle on the unified `System` contract.

A cloud system's pluggable logic is one hook: `_backend(values)` returning an
object with `status` / `exists` / `clear` / `upload`. The local-filesystem
backend (`fs`) overrides `create_system` (mkdir instead of a reachability
check).
"""

from __future__ import annotations

from ..errors import DatagenError
from .base import (
    CreateDomainSpec,
    CreateSystemSpec,
    DomainContext,
    Health,
    LoadRequest,
    Setting,
    System,
    SystemContext,
    _normalize_path,
    _validate_path,
)


class CloudBase(System):
    category = "cloud"
    kind = "file"
    staging_root = True  # -> base adds the SYS_PATH (--sys-path) option
    domain_flag = "prefix"  # object stores name the domain a prefix (fs: folder)

    def _backend(self, values: dict[str, str]):
        raise NotImplementedError

    # ---- system lifecycle ----

    def plan_system(self, spec: CreateSystemSpec) -> dict[str, Setting]:
        return dict(spec.settings)

    def provision_system(
        self, spec: CreateSystemSpec, settings: dict[str, Setting]
    ) -> None:
        backend = self._backend(self.resolve_values(settings))
        try:
            backend.status()
        except Exception as exc:  # noqa: BLE001
            raise DatagenError(f"could not reach {self.slug}: {exc}")

    def destroy_system(self, ctx: SystemContext) -> None:
        return None  # the root prefix is externally owned — never touch it

    def system_status(self, ctx: SystemContext) -> Health:
        return self._reachable(
            ctx.meta, f"{self.slug} storage (root='{ctx.meta.get('SYS_PATH', '')}')"
        )

    # ---- domain lifecycle ----

    def plan_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec
    ) -> dict[str, Setting]:
        settings = self.settings_from_meta(ctx.meta)
        base = _normalize_path(spec.entity_name)
        if not base:
            raise DatagenError("domain name is empty")
        _validate_path(base, "name")
        root = self.resolve_values(settings).get("SYS_PATH", "")
        full = f"{root}/{base}" if root else base
        settings["BASE_PATH"] = Setting("BASE_PATH", value=full, is_path=True)
        return settings

    def provision_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec, settings: dict[str, Setting]
    ) -> None:
        values = self.resolve_values(settings)
        full = settings["BASE_PATH"].value
        backend = self._backend(values)
        if backend.exists(full):
            if spec.force:
                backend.clear(full)
            else:
                raise DatagenError(
                    f"domain prefix has objects: {self.slug}://{full} "
                    f"(use --force to clear)"
                )

    def destroy_domain(self, ctx: DomainContext) -> None:
        values = self.resolve_values(self.settings_from_meta(ctx.meta))
        self._backend(values).clear(ctx.meta["BASE_PATH"])

    def domain_status(self, ctx: DomainContext) -> Health:
        return self._reachable(ctx.meta, f"{self.slug} domain '{ctx.name}'")

    # ---- data plane ----

    def load_data(self, ctx: DomainContext, req: LoadRequest) -> int:
        values = self.resolve_values(self.settings_from_meta(ctx.meta))
        backend = self._backend(values)
        full = ctx.meta["BASE_PATH"]
        if req.force:
            try:
                backend.clear(full)
            except Exception:  # noqa: BLE001 - best-effort (prefix may be empty)
                pass
        print(f">>> uploading -> {self.slug}://{full} ...")
        n = backend.upload(full, [str(req.data_dir)])
        print(f">>> uploaded {n} file(s)")
        return n

    def _reachable(self, meta: dict[str, str], what: str) -> Health:
        values = self.resolve_values(self.settings_from_meta(meta))
        try:
            self._backend(values).status()
            return Health(True, f"{what} reachable")
        except Exception as exc:  # noqa: BLE001
            return Health(False, f"cannot reach {self.slug}: {exc}")
