#
# Copyright 2026 Tabsdata Inc.
#

"""Encrypted, typed descriptors — the persisted state for systems and domains.

A descriptor is a pydantic model stored as an encrypted file (see ``crypto``).
``load`` returns a :class:`SystemInfo` or :class:`DomainInfo` (chosen by the
``role`` discriminator), so callers work with typed attributes rather than a raw
dict. ``meta`` stays a ``dict[str, str]`` — an open-ended, system-specific bag of
connection settings (env-var-style ``KEY -> value``). The domain descriptor
carries every back-ref it needs (``SQLALCHEMY_URI``, admin creds), so domain
commands never load the system descriptor.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Literal, Union
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from tabsdatax.itest.tdksystems2 import constants, crypto
from tabsdatax.itest.tdksystems2.errors import TdxError

GENERATOR = "tdx2"


def _new_uuid() -> str:
    return str(uuid4())


class SystemInfo(BaseModel):
    """A provisioned system: its slug, store name, and connection ``meta``."""

    model_config = ConfigDict(protected_namespaces=())

    role: Literal["system"] = constants.ROLE_SYSTEM
    generator: str = GENERATOR
    system: str  # slug, e.g. "mysql"
    name: str  # store file name / logical name
    uuid: str = Field(default_factory=_new_uuid)  # this instance's stable id
    started: str  # ISO-8601 UTC
    meta: dict[str, str] = Field(default_factory=dict)
    # {META_KEY: ENV_VAR} for meta keys whose value came from an env var — the
    # value is still baked into `meta`; this records the source so a manifest can
    # re-emit the reference (`<flag>$: ENV_VAR`) instead of the (secret) value.
    env_refs: dict[str, str] = Field(default_factory=dict)
    # option keys given a literal `--<flag> VALUE` at create — a manifest re-emits
    # these (`<flag>: value`) but omits the auto-generated defaults (free port,
    # generated password) a fresh create re-derives.
    opt_literals: list[str] = Field(default_factory=list)
    # the `--sys-hostname` this system was created with (empty = plain localhost),
    # kept so a manifest replay advertises the same host again.
    sys_hostname: str = ""


class DomainInfo(BaseModel):
    """A domain in a system: connection ``meta`` plus model/data/seed state.

    ``model`` / ``data`` are stamped by create-model / create-data; ``entities``
    maps each seeded entity to its row count — for a SQL domain that is
    ``{table: rows}``; for a file domain it is ``{staged_file_path: rows}`` (the
    files relative to the domain root). ``seed`` is the last ``modify-data --seed
    auto`` seed applied (insert ordinals derive from it, so the seed alone makes a
    run sequence repeatable)."""

    model_config = ConfigDict(protected_namespaces=())

    role: Literal["domain"] = constants.ROLE_DOMAIN
    generator: str = GENERATOR
    system: str
    name: str
    uuid: str = Field(default_factory=_new_uuid)  # this domain's stable id
    started: str
    sys_name: str
    sys_uuid: str = ""  # uuid of the system instance this domain was created in
    external_name: str  # entity-container name in the system
    meta: dict[str, str] = Field(default_factory=dict)
    env_refs: dict[str, str] = Field(default_factory=dict)  # see SystemInfo.env_refs
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] = Field(default_factory=dict)
    seed: int = 0


Info = Annotated[Union[SystemInfo, DomainInfo], Field(discriminator="role")]
_ADAPTER: TypeAdapter[SystemInfo | DomainInfo] = TypeAdapter(Info)


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def validate_name(name: str) -> None:
    if not name:
        raise TdxError("name is empty")
    if name.upper() == constants.TARGET_ALL:
        raise TdxError(f"'{name}' is reserved (the batch selector) — pick another name")
    for ch in name:
        if not (ch.isalnum() or ch in "_-"):
            raise TdxError(f"name must be alphanumeric/_/- (got: {name})")


def save(info: SystemInfo | DomainInfo, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pem = crypto.encrypt(info.model_dump_json(indent=2).encode("utf-8"))
    # Write-then-rename: a descriptor is read by parallel test workers and by
    # concurrent batch commands, and a half-written one is unreadable.
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(pem, encoding="utf-8")
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def load(path: str | Path) -> SystemInfo | DomainInfo:
    """Read a descriptor. A legacy ``TDX2`` one raises
    :class:`.errors.LegacyDescriptorError` naming the ``upgrade`` command —
    converting is an operator's explicit act, never a read's side effect."""
    return loads(Path(path).read_text(encoding="utf-8"))


def upgrade(path: str | Path) -> bool:
    """Rewrite `path` in the current format if it is a legacy ``TDX2``
    descriptor. Returns whether it converted; a current descriptor is left byte
    for byte alone rather than needlessly re-salted."""
    path = Path(path)
    text = path.read_text(encoding="utf-8")
    if not crypto.is_legacy(text):
        return False
    save(_ADAPTER.validate_json(crypto.decrypt(text, allow_legacy=True)), path)
    return True


def loads(text: str) -> SystemInfo | DomainInfo:
    return _ADAPTER.validate_json(crypto.decrypt(text))


def is_descriptor(path: str | Path) -> bool:
    p = Path(path)
    if not p.is_file():
        return False
    try:
        return crypto.is_pem(p.read_text(encoding="utf-8"))
    except OSError:
        return False


def write_system(
    path: str | Path,
    slug: str,
    name: str,
    meta: dict[str, str],
    env_refs: dict[str, str] | None = None,
    opt_literals: list[str] | None = None,
    sys_hostname: str = "",
) -> SystemInfo:
    info = SystemInfo(
        system=slug,
        name=name,
        started=now_stamp(),
        meta=dict(meta),
        env_refs=dict(env_refs or {}),
        opt_literals=list(opt_literals or []),
        sys_hostname=sys_hostname,
    )
    save(info, path)
    return info


def write_domain(
    path: str | Path,
    slug: str,
    name: str,
    *,
    sys_name: str,
    sys_uuid: str = "",
    external_name: str,
    meta: dict[str, str],
    env_refs: dict[str, str] | None = None,
) -> DomainInfo:
    info = DomainInfo(
        system=slug,
        name=name,
        started=now_stamp(),
        sys_name=sys_name,
        sys_uuid=sys_uuid,
        external_name=external_name,
        meta=dict(meta),
        env_refs=dict(env_refs or {}),
    )
    save(info, path)
    return info


def _load_domain(path: str | Path) -> DomainInfo:
    info = load(path)
    if not isinstance(info, DomainInfo):
        raise TdxError(f"{path} is not a domain descriptor")
    return info


def set_model(path: str | Path, model: str, entity_names: list[str]) -> None:
    """Record the installed `model` and reset data/seed state. ``create-model``
    installs an empty schema, so every entity starts at zero rows — the domain's
    ``entities`` map is populated with all entity names at count 0 (create-data
    fills in the real counts)."""
    info = _load_domain(path)
    info.model = model
    info.data = None
    info.entities = {n: 0 for n in entity_names}
    info.seed = 0
    save(info, path)


def record_data(path: str | Path, size: str, entities: dict[str, int]) -> None:
    """Record the loaded data `size` and per-entity row counts, resetting the
    ``modify-data`` seed to the fresh baseline."""
    info = _load_domain(path)
    info.data = size
    info.entities = dict(entities)
    info.seed = 0
    save(info, path)


def record_changes(
    path: str | Path, entities: dict[str, int], *, seed: int | None = None
) -> None:
    """Persist the post-``modify-data`` per-entity row counts. When the run used
    ``--seed auto``, `seed` also records the applied seed so the next auto run
    continues from ``seed + 1``; a fixed seed passes ``None`` and leaves the auto
    cursor untouched."""
    info = _load_domain(path)
    info.entities = dict(entities)
    if seed is not None:
        info.seed = seed
    save(info, path)


def info_render(
    info: SystemInfo | DomainInfo, fmt: str, prefix: str | None = None
) -> str:
    """Render a descriptor for ``info``. ``json`` dumps the whole model;
    ``export`` / ``unset`` emit shell lines for each ``meta`` key, optionally
    prefixed as ``<PREFIX>__<KEY>``."""
    if fmt == "json":
        return info.model_dump_json(indent=2)

    def key(k: str) -> str:
        return f"{prefix}__{k}" if prefix else k

    if fmt == "export":
        return "\n".join(f"export {key(k)}={v!r}" for k, v in info.meta.items())
    if fmt == "unset":
        return "\n".join(f"unset {key(k)}" for k in info.meta)
    raise TdxError(f"unknown info format: {fmt} ({'|'.join(constants.INFO_FORMATS)})")
