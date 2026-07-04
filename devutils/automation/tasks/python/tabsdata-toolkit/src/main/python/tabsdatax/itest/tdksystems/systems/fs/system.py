"""Local-filesystem system — domains are directories under a root path.

Diverges from the object stores: the root and each domain are real dirs
(mkdir / rmtree), with stricter root validation so a system-level destroy can
never rmtree a sensitive path. Absolute roots are valid, so `relative_sys_path`
is False (no leading-slash strip).
"""
from __future__ import annotations

import shutil
from pathlib import Path

from .._cloud import CloudBase
from ..base import (
    CreateDomainSpec, CreateSystemSpec, DomainContext, Setting, SystemContext,
    _normalize_path, _validate_path,
)
from ...errors import DatagenError
from .backend import FsBackend


def _validate_fs_root(root: str) -> None:
    """Allow only safe locations so a system-level destroy can rmtree without
    wiping a real system directory."""
    if not root:
        raise DatagenError("root path is empty")
    parts = [p for p in root.split("/") if p]
    if root.startswith("/"):
        first = parts[0] if parts else ""
        if first not in ("tmp", "home", "Users"):
            raise DatagenError(
                f"absolute root path must start with /tmp/, /home/, or "
                f"/Users/ (got: '{root}')")
    else:
        if parts and parts[0] in ("usr", "opt"):
            raise DatagenError(
                f"relative root path's first element cannot be 'usr' or "
                f"'opt' (got: '{root}')")
    if len(parts) < 2:
        raise DatagenError(f"root path must have at least 2 elements (got: '{root}')")


class FsSystem(CloudBase):
    slug = "fs"
    label = "fs"
    category = "cloud"           # descriptor.write routes fs via write_cloud_*
    relative_sys_path = False    # absolute roots are valid — don't strip slash
    options = ()                 # only --sys-path (the root)

    def _backend(self, values: dict[str, str]) -> FsBackend:
        _validate_fs_root(values.get("SYS_PATH", ""))
        return FsBackend(values)

    # fs creates its root dir rather than checking a remote bucket.
    def create_system(self, spec: CreateSystemSpec) -> dict[str, Setting]:
        root = self.resolve_values(spec.settings).get("SYS_PATH", "")
        _validate_fs_root(root)
        root_p = Path(root)
        if root_p.exists():
            if spec.force:
                shutil.rmtree(root_p)
                print(f">>> --force: removed existing root path '{root}'")
            else:
                raise DatagenError(
                    f"root path already exists: '{root}' (use --force to clear)")
        root_p.mkdir(parents=True)
        return dict(spec.settings)

    def destroy_system(self, ctx: SystemContext) -> None:
        p = Path(ctx.meta.get("SYS_PATH", ""))
        if p.exists():
            shutil.rmtree(p)

    # fs materialises the domain as a real directory (mkdir).
    def create_domain(self, ctx: SystemContext, spec: CreateDomainSpec) -> dict[str, Setting]:
        settings = self.settings_from_meta(ctx.meta)
        root = self.resolve_values(settings).get("SYS_PATH", "")
        base = _normalize_path(spec.name)
        if not base:
            raise DatagenError("domain --name is empty")
        _validate_path(base, "name")
        full = f"{root}/{base}"
        full_p = Path(full)
        if full_p.exists():
            if spec.force:
                shutil.rmtree(full_p)
            else:
                raise DatagenError(
                    f"domain already exists: {full} (use --force to recreate)")
        full_p.mkdir(parents=True)
        settings["BASE_PATH"] = Setting("BASE_PATH", value=full, is_path=True)
        return settings
