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
    max_entity_name_length = 1024  # object-store keys/prefixes are effectively
    # unbounded for our names

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

    def list_domain_entities(self, ctx: SystemContext) -> list[str]:
        # Domains are the immediate child prefixes / folders under the staging
        # root (SYS_PATH); each was created as `<root>/<entity>`.
        values = self.resolve_values(self.settings_from_meta(ctx.meta))
        return self._backend(values).children(values.get("SYS_PATH", ""))

    def purge_domain_entity(self, ctx: SystemContext, name: str) -> None:
        values = self.resolve_values(self.settings_from_meta(ctx.meta))
        root = values.get("SYS_PATH", "")
        self._backend(values).clear(f"{root}/{name}" if root else name)

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

    def apply_changes(self, ctx: DomainContext, transactions: list) -> int:
        """File domains take inserts only — each run stages the inserted rows as
        NEW data files (per staged format), alongside the seed. The file name
        carries a short content hash so an identical plan is idempotent
        (overwrites) while a different plan adds fresh files. Returns the number
        of transactions applied."""
        import hashlib
        import json
        import tempfile
        from pathlib import Path

        from .. import changes as changes_mod, data_gen
        from ..emit import ENTITIES_MANIFEST, Table, write_tables

        if not changes_mod.has_insert(transactions):
            raise DatagenError(
                f"{self.slug} domains support only inserts for `changes` "
                f"(a change is a new file); use --ops insert"
            )
        tables = changes_mod.insert_tables(transactions)
        tag = hashlib.sha1(
            json.dumps(
                [[t.name, t.rows] for t in tables], sort_keys=True, default=str
            ).encode()
        ).hexdigest()[:8]
        values = self.resolve_values(self.settings_from_meta(ctx.meta))
        backend = self._backend(values)
        full = ctx.meta["BASE_PATH"]
        rows = sum(len(t.rows) for t in tables)
        with tempfile.TemporaryDirectory(prefix="tdx-changes-") as tmp:
            root = Path(tmp)
            for fmt in data_gen.FILE_FORMATS:
                fmt_dir = root / fmt
                staged = [
                    Table(name=f"{t.name}__chg_{tag}", columns=t.columns, rows=t.rows)
                    for t in tables
                ]
                write_tables(staged, fmt_dir, fmt)
                (fmt_dir / "run-order.txt").unlink(missing_ok=True)
                (fmt_dir / ENTITIES_MANIFEST).unlink(missing_ok=True)
            print(f">>> staging {rows} inserted row(s) -> {self.slug}://{full} ...")
            backend.upload(full, [str(root)])
        print(f">>> wrote {rows} row(s) as new change file(s)")
        return len(transactions)

    def _reachable(self, meta: dict[str, str], what: str) -> Health:
        values = self.resolve_values(self.settings_from_meta(meta))
        try:
            self._backend(values).status()
            return Health(True, f"{what} reachable")
        except Exception as exc:  # noqa: BLE001
            return Health(False, f"cannot reach {self.slug}: {exc}")
