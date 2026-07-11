#
# Copyright 2026 Tabsdata Inc.
#

"""Local-filesystem system — domains are directories under a root path.

Diverges from the object stores: the root and each domain are real dirs
(mkdir), with stricter root validation so a force-recreate rmtree can never
wipe a sensitive path. The root itself is left in place on system destroy, and
is not checked for pre-existing content on create. The root is stored as an
absolute path (a full path from the filesystem root), so `relative_sys_path`
is False (the descriptor keeps the leading slash) — the mirror image of the
object stores, whose bucket-relative prefixes must not start with a slash.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from ...errors import DatagenError
from .._cloud import CloudBase
from ..base import (
    CreateDomainSpec,
    CreateSystemSpec,
    Setting,
    SystemContext,
    _normalize_path,
    _validate_path,
)
from .backend import FsBackend

# First path element of a root we refuse to sit under, so a force-recreate
# rmtree of a domain directory can never wipe a real system location.
_UNSAFE_ROOT_HEADS = frozenset({
    "usr",
    "opt",
    "etc",
    "var",
    "bin",
    "sbin",
    "lib",
    "boot",
    "dev",
    "proc",
    "sys",
    "root",
})


def _validate_fs_root(root: str) -> None:
    """fs roots are absolute (a full path from the filesystem root); a root
    directly under a system directory is refused so a force-recreate rmtree of a
    domain directory under this root can never wipe a real system location."""
    if not root:
        raise DatagenError("root path is empty")
    if not root.startswith("/"):
        raise DatagenError(
            f"fs root path must be absolute (start with '/') (got: '{root}')"
        )
    parts = [p for p in root.split("/") if p]
    if parts and parts[0] in _UNSAFE_ROOT_HEADS:
        raise DatagenError(
            f"fs root path must not sit under a system directory (got: '{root}')"
        )
    if len(parts) < 2:
        raise DatagenError(f"root path must have at least 2 elements (got: '{root}')")


class FsSystem(CloudBase):
    slug = "fs"
    label = "fs"
    category = "native"  # local filesystem, not a cloud object store; still
    # persisted via write_cloud_* (descriptor kind stays "cloud-*")
    relative_sys_path = False  # roots are absolute — keep the leading slash
    domain_flag = "folder"  # a domain is a directory on disk
    options = ()  # only --sys-path (the root)

    def _backend(self, values: dict[str, str]) -> FsBackend:
        _validate_fs_root(values.get("SYS_PATH", ""))
        return FsBackend(values)

    # fs is local, so — unlike the object stores — its root is a plain
    # `--sys-path` option (no `$FS__SYS_PATH` env fallback), defaulting to
    # `target/backends/<system name>`; whatever is given is resolved to an
    # absolute path against the current directory.
    def add_create_options(self, parser) -> None:
        parser.add_argument(
            "--sys-path",
            dest="lit_SYS_PATH",
            default=None,
            metavar="DIR",
            help="root dir (default: target/backends/<system name>); stored as "
            "an absolute path resolved against the current directory",
        )

    def resolve_create_settings(self, ns) -> dict[str, Setting]:
        return {
            "SYS_PATH": Setting(
                "SYS_PATH", value=getattr(ns, "lit_SYS_PATH", None), is_path=True
            )
        }

    # fs ensures its root dir exists rather than checking a remote bucket; it
    # neither errors on a pre-existing root nor deletes it (destroy is a no-op,
    # inherited from CloudBase). The root is made absolute so the localfile
    # connection's base_path is a full path from the filesystem root.
    def create_system(self, spec: CreateSystemSpec) -> dict[str, Setting]:
        root = _normalize_path(
            str(
                Path(
                    spec.settings["SYS_PATH"].value or f"target/backends/{spec.name}"
                ).absolute()
            )
        )
        _validate_fs_root(root)
        Path(root).mkdir(parents=True, exist_ok=True)
        return {"SYS_PATH": Setting("SYS_PATH", value=root, is_path=True)}

    # fs materialises the domain as a real directory (mkdir).
    def create_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec
    ) -> dict[str, Setting]:
        settings = self.settings_from_meta(ctx.meta)
        root = self.resolve_values(settings).get("SYS_PATH", "")
        base = _normalize_path(spec.entity_name)
        if not base:
            raise DatagenError("domain name is empty")
        _validate_path(base, "name")
        full = f"{root}/{base}"
        full_p = Path(full)
        if full_p.exists():
            if spec.force:
                shutil.rmtree(full_p)
            else:
                raise DatagenError(
                    f"domain already exists: {full} (use --force to recreate)"
                )
        full_p.mkdir(parents=True)
        settings["BASE_PATH"] = Setting("BASE_PATH", value=full, is_path=True)
        return settings
