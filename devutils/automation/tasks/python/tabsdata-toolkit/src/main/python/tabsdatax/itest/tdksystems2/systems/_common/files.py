#
# Copyright 2026 Tabsdata Inc.
#

"""``FileBackend`` — the shared base for file / object-store systems.

A file system has no container and no schema: the "system" is a staging root, a
"domain" is a prefix/folder under it, and a model's data is staged as one file
per entity per format (:data:`emit.FILE_FORMATS` — csv / ndjson / parquet), each
named ``<entity>_<YYYYMMDD>T<hhmmss_mmm>.<fmt>`` after the command's invocation
time. ``modify-data`` is **insert-only** — each run writes the inserted rows as
NEW (freshly timestamped) files alongside the seed, never mutating existing ones.
Concrete backends (fs, s3, gcs, azureblob) implement only the five storage
primitives; everything else lives here once.

The generic data-plane commands take the domain's staging destination from
:meth:`data_plane_uri` (the opaque ``BASE_PATH`` — a path for fs, a bucket key for
object stores). The descriptor's ``URI`` is the connector-facing location URI
(``file://`` / ``s3://`` / ``gs://`` / ``az://``); a file domain has no
``SQLALCHEMY_URI``, and its ``entities`` are the staged file paths (relative to
the domain root) mapped to their row counts.
"""

from __future__ import annotations

import tempfile
from collections.abc import Iterator
from pathlib import Path

from tabsdatax.itest.tdksystems2 import _venv, datagen, emit, options
from tabsdatax.itest.tdksystems2.datagen import OpKind
from tabsdatax.itest.tdksystems2.errors import TdxError


def squash_slashes(path: str) -> str:
    """Collapse every run of ``/`` down to a single ``/`` so a stray ``//`` in a
    configured ``SYS_PATH`` never reaches a composed key/path (the connectors
    reject empty path segments). A single leading ``/`` is preserved."""
    while "//" in path:
        path = path.replace("//", "/")
    return path


def normalise_prefix(prefix: str) -> str:
    """An object-store key prefix: no leading ``/``, a single trailing ``/`` when
    non-empty (so ``prefix + rel`` is a valid key)."""
    p = squash_slashes((prefix or "").strip()).lstrip("/")
    if p and not p.endswith("/"):
        p += "/"
    return p


def walk_files(local_dir: str) -> Iterator[tuple[Path, str]]:
    """Yield ``(absolute_file, posix_relpath)`` for every file under `local_dir`."""
    base = Path(local_dir)
    for src in sorted(base.rglob("*")):
        if src.is_file():
            yield src, src.relative_to(base).as_posix()


class FileBackend:
    # ---- capability flags ----
    is_container: bool = False
    supports_purge: bool = True
    supports_models: bool = True
    supports_data: bool = True
    supports_changes: bool = True  # insert-only (enforced in apply_changes)

    # Staging data files needs the frame writer; a local root does not.
    sys_env: str = _venv.ENV_C
    dom_env: str = _venv.ENV_A

    slug: str = ""
    label: str = ""
    kind: str = "file"
    weight: int = 1
    domain_word: str = "prefix"
    max_entity_name_length: int = 255

    def docker_images(self) -> list[tuple[str, str]]:
        return []

    def options(self) -> tuple:
        return ()  # declarative create options; overridden by backends that have any

    # ---- storage primitives (subclass implements) ----

    def store_status(self, root: str) -> None:
        """Raise if the staging root isn't reachable."""
        raise NotImplementedError

    def store_exists(self, dest: str) -> bool:
        """True if `dest` already holds objects."""
        raise NotImplementedError

    def store_children(self, root: str) -> list[str]:
        """Immediate child prefix/folder names under `root` (the domains)."""
        raise NotImplementedError

    def store_clear(self, dest: str) -> None:
        """Remove every object under `dest`."""
        raise NotImplementedError

    def store_upload(self, dest: str, local_dir: str) -> int:
        """Upload the `local_dir` tree under `dest`; return the file count."""
        raise NotImplementedError

    def join(self, root: str, child: str) -> str:
        root = root.replace("\\", "/")
        return squash_slashes(f"{root.rstrip('/')}/{child}")

    # ---- system lifecycle ----

    def plan_system(
        self, name: str, overrides: dict[str, str] | None = None
    ) -> dict[str, str]:
        raise NotImplementedError  # file systems create via their own command

    def provision_system(
        self, meta: dict[str, str], *, force: bool = False
    ) -> dict[str, str]:
        self.store_status(meta["SYS_PATH"])
        return {}

    def destroy_system(self, meta: dict[str, str]) -> None:
        return None  # the staging root is externally owned — never removed

    def system_status(self, meta: dict[str, str]) -> tuple[bool, str]:
        root = meta.get("SYS_PATH", "")
        try:
            self.store_status(root)
        except Exception as exc:  # noqa: BLE001
            return False, f"cannot reach {self.slug}: {exc}"
        return True, f"{self.label} root '{root}' reachable"

    def restart_system(self, meta: dict[str, str]) -> str:
        raise TdxError(f"{self.slug} is not a container (nothing to restart)")

    # ---- domain lifecycle ----

    def new_domain_password(self) -> str:
        return ""  # file domains have no credentials

    def data_plane_uri(self, meta: dict[str, str]) -> str:
        return meta["BASE_PATH"]  # the opaque staging destination

    def _uri(self, dest: str) -> str:
        """The connector-facing location URI for storage destination `dest`."""
        return Path(dest).as_uri()  # fs: file:///abs/path

    def domain_meta(
        self, sys_meta: dict[str, str], entity_name: str, password: str
    ) -> dict[str, str]:
        root = sys_meta["SYS_PATH"]
        dest = self.join(root, entity_name)
        return {
            "SYS_PATH": root,
            "BASE_PATH": dest,  # the opaque staging destination (data_plane_uri)
            "DB": entity_name,
            "URI": self._uri(dest),
        }

    def provision_domain(
        self,
        sys_meta: dict[str, str],
        entity_name: str,
        domain_meta: dict[str, str],
        *,
        force: bool = False,
    ) -> None:
        dest = domain_meta["BASE_PATH"]
        if self.store_exists(dest):
            if force:
                self.store_clear(dest)
            else:
                raise TdxError(
                    f"{self.domain_word} has objects: {dest} (use --force to clear)"
                )

    def destroy_domain(self, domain_meta: dict[str, str]) -> None:
        self.store_clear(domain_meta["BASE_PATH"])

    def domain_status(self, domain_meta: dict[str, str]) -> tuple[bool, str]:
        try:
            self.store_status(domain_meta.get("SYS_PATH", ""))
        except Exception as exc:  # noqa: BLE001
            return False, f"cannot reach {self.slug}: {exc}"
        return True, f"{self.label} domain at {domain_meta.get('BASE_PATH', '')}"

    def list_domain_entities(self, sys_meta: dict[str, str]) -> list[str]:
        return self.store_children(sys_meta["SYS_PATH"])

    def purge_domain_entity(self, sys_meta: dict[str, str], name: str) -> None:
        self.store_clear(self.join(sys_meta["SYS_PATH"], name))

    # ---- data plane ----

    def has_model(self, model: str) -> bool:
        return model in datagen.list_models()  # any model — no per-system DDL

    def available_models(self) -> list[str]:
        return datagen.list_models()

    def install_schema(
        self, meta: dict[str, str], model: str, *, force: bool = False
    ) -> None:
        if force:
            self.store_clear(self.data_plane_uri(meta))
        print(">>> file domain: no schema to install")

    def _stage_and_upload(
        self, dest: str, tables: list[emit.Table], stamp: str
    ) -> dict[str, int]:
        """Stage each table as ``<fmt>/<entity>_<stamp>.<fmt>`` for every format,
        upload the tree under `dest`, and return ``{relative_path: row_count}``."""
        entities: dict[str, int] = {}
        with tempfile.TemporaryDirectory(prefix="tdx-files-") as tmp:
            root = Path(tmp)
            for fmt in emit.FILE_FORMATS:
                for tbl in tables:
                    rel = f"{fmt}/{tbl.name}_{stamp}.{fmt}"
                    emit.write_frame(tbl, root / rel, fmt)
                    entities[rel] = len(tbl.rows)
            self.store_upload(dest, str(root))
        return entities

    def load_tables(self, uri: str, tables: list[emit.Table]) -> dict[str, int]:
        stamp = emit.file_stamp()
        print(
            f">>> staging {sum(len(t.rows) for t in tables)} rows as "
            f"{len(emit.FILE_FORMATS)} format(s) -> {uri} …"
        )
        return self._stage_and_upload(uri, tables, stamp)

    def apply_changes(
        self, uri: str, transactions: list, before: dict[str, int]
    ) -> dict[str, int]:
        changes = [c for trx in transactions for c in trx]
        if any(c.kind is not OpKind.INSERT for c in changes):
            raise TdxError(
                f"{self.slug} domains support only inserts for modify-data "
                f"(a change is a new file) — use --ops Insert"
            )
        by_table: dict[str, tuple[list, list]] = {}
        for c in changes:
            by_table.setdefault(c.table, (c.columns, []))[1].append(c.row)
        tables = [
            emit.Table(name=t, columns=cols, rows=rows)
            for t, (cols, rows) in by_table.items()
        ]
        stamp = emit.file_stamp()
        print(f">>> staging {len(changes)} inserted row(s) -> {uri} …")
        staged = self._stage_and_upload(uri, tables, stamp)
        return {**before, **staged}  # accumulate the new files onto the prior ones

    def drop_all_tables(self, uri: str) -> None:
        self.store_clear(uri)


class CloudFileBackend(FileBackend):
    """Object-store base (s3 / gcs / azureblob).

    Its connection config (bucket/container, secrets, ``SYS_PATH`` staging-root)
    is resolved from the declared ``options()`` (env-sourced by default) **at
    create time** and baked into the (encrypted) descriptor; every later op reads
    it back from that descriptor via :meth:`configure` — the only environment
    variable read at use time is the crypto key. Staged files live at
    ``<bucket>/<SYS_PATH>/<domain>/<fmt>/<entity>_<stamp>.<fmt>``. Subclasses
    implement the five ``store_*`` primitives with their SDK plus ``_bucket`` (the
    bucket / container name shown in URIs), declare their connection ``options()``
    and their ``uri_scheme`` (``s3`` / ``gs`` / ``az``).
    """

    # Creating the bucket already goes through the store's SDK.
    sys_env = _venv.ENV_A
    env_prefix: str = ""  # S3 / GCS / AZ
    uri_scheme: str = ""  # URI scheme for the location URI; defaults to the slug

    _bound: dict[str, str] | None = None

    def options(self) -> tuple[options.Option, ...]:
        return ()  # subclass declares its connection options

    def configure(self, meta: dict[str, str]) -> None:
        self._bound = dict(meta)

    def _cfg(self, key: str, *, required: bool = True) -> str:
        """A config value from the bound descriptor (create binds it before
        provisioning; every later op binds it from the loaded descriptor)."""
        return (self._bound or {}).get(key, "")

    def _bucket(self) -> str:
        raise NotImplementedError

    def _location(self, prefix: str) -> str:
        return f"{self.uri_scheme or self.slug}://{self._bucket()}/{prefix}"

    def _uri(self, dest: str) -> str:
        return self._location(dest)  # s3:// / gs:// / az:// bucket/prefix

    def join(self, root: str, child: str) -> str:
        root = squash_slashes(root).strip("/")
        return squash_slashes(f"{root}/{child}") if root else squash_slashes(child)

    def plan_system(
        self, name: str, overrides: dict[str, str] | None = None
    ) -> dict[str, str]:
        o = overrides or {}
        root = squash_slashes(o.get("SYS_PATH") or "").strip("/")
        meta = {"SYS_PATH": root, "ENV_PREFIX": self.env_prefix}
        for opt in self.options():
            if opt.name != "SYS_PATH":
                meta[opt.name] = o.get(opt.name, "")
        self.configure(meta)  # so _bucket()/_cfg read the planned config
        meta["LOCATION"] = self._location(root)
        return meta

    def domain_meta(
        self, sys_meta: dict[str, str], entity_name: str, password: str
    ) -> dict[str, str]:
        # super() sets URI via the polymorphic _uri (== _location for cloud)
        meta = super().domain_meta(sys_meta, entity_name, password)
        for opt in self.options():  # forward baked config so the domain
            if opt.name in sys_meta:  # descriptor is self-contained too
                meta[opt.name] = sys_meta[opt.name]
        return meta
