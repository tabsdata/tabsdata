"""Unified external-system contract.

ONE `System` ABC that every backend implements, so `tdx.py` and `cli.py`
dispatch without any `if category == …` branch::

    sys = registry.get(slug)                 # from systems/<slug>/
    sys.destroy_domain(DomainContext(...))    # category-free

Each system is a directory under `systems/` (`systems/mysql/`, `systems/s3/`,
`systems/snowflake/`, …) whose class subclasses one of the shared bases
(`_container.ContainerBase` / `_cloud.CloudBase` / `_saas.SaasBase`) and
supplies only its per-system hooks. `category` survives as a *descriptor field
/ --type grouping*, not as control flow; the former per-category distinctions
are capability flags (`supports_pause` / `staging_root` / `domain_is_named` /
`supports_sql` / `kind`).

The verb contract:
    create_system(spec) -> {name: Setting}      destroy_system(ctx)
    system_status(ctx) -> Health                pause / resume(ctx)
    create_domain(ctx, spec) -> {name: Setting} destroy_domain(ctx)
    domain_status(ctx) -> Health
    install_schema(ctx, model, *, force)        load_data(ctx, LoadRequest)

What the framework (not the System) owns
----------------------------------------
  * CLI parsing / the `--<flag>` / `--env:<flag>` / `--env-prefix` surface
    (shared `resolve_create_settings` / `add_create_options` below).
  * Persisting descriptors: `create_system` / `create_domain` RETURN the
    settings to bake; `descriptor.write(sys_obj, role, out, settings)` persists
    them (the one systems -> descriptor serialization step).
  * The `$TD_DEV_PERSONAL_SECRET` gate, the venv bootstrap, and model data
    *generation* (model-layer, keyed off `System.kind`).
"""
from __future__ import annotations

import argparse
import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path

from ..errors import DatagenError


def _normalize_path(p: str) -> str:
    """Strip trailing slashes; collapse inner runs; keep a single leading
    slash (absolute fs paths preserved, bucket-relative prefixes cleaned)."""
    if not p:
        return ""
    abs_ = p.startswith("/")
    inner = p.strip("/")
    while "//" in inner:
        inner = inner.replace("//", "/")
    return ("/" + inner) if abs_ else inner


def _validate_path(p: str, label: str) -> None:
    if ".." in p.split("/"):
        raise DatagenError(f"{label} must not contain '..' (got: {p})")
    if "\n" in p:
        raise DatagenError(f"{label} contains newlines")


# ======================================================================
# Value types
# ======================================================================

@dataclass(frozen=True)
class Option:
    """A connection option a system declares (drives `create -h` + resolution).

    e.g. Option("BUCKET", "bucket") -> `--bucket` / `--env:bucket` /
    `$<PREFIX>__BUCKET`. `secret=True` masks the value in printed create
    details (was the per-category `_concealed()`)."""
    name: str                 # env-var / setting name, e.g. "BUCKET"
    cli_flag: str             # flag stem, e.g. "bucket"
    is_path: bool = False     # normalise + validate as a path (e.g. SYS_PATH)
    secret: bool = False      # mask when echoing create details


@dataclass
class Setting:
    """A resolved option: either a literal `value`, or a reference to `env_var`
    (baked into the descriptor as a forwarded `${ENV}` so it re-resolves at use
    time). Exactly one of value / env_var is the source of truth."""
    name: str
    value: str | None = None
    env_var: str | None = None
    is_path: bool = False


@dataclass
class Health:
    """Result of a status/reachability check (replaces the three shapes:
    container's `(ok, msg)`, cloud's raising `reachable`, saas's raising
    `ping`)."""
    ok: bool
    detail: str


# --- create-time input (parsed from the CLI, system does not exist yet) ---

@dataclass
class CreateSystemSpec:
    name: str                          # logical --name (identity + info prefix)
    out: str                           # descriptor path to write
    force: bool = False
    settings: dict[str, Setting] = field(default_factory=dict)  # resolved options
    # container-only knobs; ignored by cloud/saas backends.
    host_port: int | None = None
    ready_timeout: int = 300


@dataclass
class CreateDomainSpec:
    name: str
    out: str
    force: bool = False


# --- run-time handles (resolved from an existing descriptor) ---

@dataclass
class SystemContext:
    """A resolved handle to an existing system, built from its descriptor.
    `meta` is `descriptor.resolve(doc)` — the connection settings (CONTAINER /
    PASSWORD for containers, BUCKET/REGION/… for cloud, etc.). Backends read
    the fields they need; there is no category branch at the call site."""
    slug: str
    name: str
    meta: dict[str, str]


@dataclass
class DomainContext:
    """A resolved handle to an existing domain (its descriptor's settings,
    including system back-references like SYS_CONTAINER / BASE_PATH)."""
    slug: str
    name: str
    meta: dict[str, str]


@dataclass
class LoadRequest:
    """Generated model artifacts handed to `load_data`. cli.py generates the
    shape that matches `System.kind` (SQL INSERTs in run-order for SQL
    containers, CSV/Parquet for object stores, JSON + per-entity schema
    sidecars for kafka / saas-SQL) into `data_dir`; each backend reads only the
    fields relevant to its data plane. `ser` / `schemas_dir` /
    `use_schema_registry` are kafka-only."""
    model: str
    data_dir: Path
    size: str | None
    force: bool = False
    ser: str | None = None
    schemas_dir: str | None = None
    use_schema_registry: bool = False


class Unsupported(DatagenError):
    """Raised for a verb a system doesn't offer (e.g. pause on a cloud store)."""


# ======================================================================
# The contract
# ======================================================================

class System(ABC):
    # ---- identity / declaration (class attributes) ----
    slug: str = ""             # --type value, dir name under systems/
    label: str = ""            # human name, e.g. "MySQL"
    category: str = ""         # "container" | "cloud" | "fs" | "saas"
                               # descriptor field + --type grouping ONLY
    kind: str = ""             # data-plane compat tag: sql|file|kafka|
                               # mongodb|salesforce (model-vs-system check)
    options: tuple[Option, ...] = ()   # connection options (excl. SYS_PATH)

    # ---- capability flags (replace the isinstance/category checks) ----
    supports_pause: bool = False       # container: stop/start the process
    staging_root: bool = False         # has a SYS_PATH root (cloud/fs +
                                       # file-staging containers) -> base adds
                                       # the --sys-path option automatically
    domain_is_named: bool = True       # False for salesforce (creds clone, no
                                       # schema/db/prefix to create)
    supports_sql: bool = False         # can install_schema + run INSERTs

    @property
    def default_env_prefix(self) -> str:
        """Default `--env-prefix`: the slug upper-cased (s3 -> S3)."""
        return self.slug.upper()

    # ==================================================================
    # System lifecycle
    # ==================================================================

    @abstractmethod
    def create_system(self, spec: CreateSystemSpec) -> dict[str, Setting]:
        """Provision the system (start the container / verify the bucket /
        validate creds) and return the settings to bake into the descriptor.

        Returns Settings (not raw strings) so the framework's `descriptor.write`
        can honour the literal-vs-env distinction: a Setting with `env_var` is
        stored as a forwarded reference (re-resolved at use time, so secrets are
        never written to disk); a Setting with `value` is baked literally (and
        the descriptor file is itself encrypted). Idempotent under `spec.force`;
        the framework adds identity (slug/category/kind/name/started)."""

    @abstractmethod
    def destroy_system(self, ctx: SystemContext) -> None:
        """Tear down the system (rm container / rmtree fs root / no-op for a
        managed cloud or saas account). The framework removes the descriptor."""

    @abstractmethod
    def system_status(self, ctx: SystemContext) -> Health:
        """Is the system reachable / running?"""

    def pause(self, ctx: SystemContext) -> str:
        """Stop the system, keeping data (container stop). Default: unsupported."""
        raise Unsupported(f"{self.slug} does not support pause")

    def resume(self, ctx: SystemContext) -> str:
        raise Unsupported(f"{self.slug} does not support resume")

    # ==================================================================
    # Domain lifecycle
    # ==================================================================

    @abstractmethod
    def create_domain(self, ctx: SystemContext, spec: CreateDomainSpec) -> dict[str, Setting]:
        """Create the domain resource inside the system — a database, schema,
        Kafka topic, object-store prefix, or directory — and return the domain
        settings to bake (e.g. SQLALCHEMY_URI + system back-refs, or BASE_PATH +
        the forwarded connection settings). `spec.name` is used verbatim (no run
        prefix). Same Setting-vs-string contract as `create_system`."""

    @abstractmethod
    def destroy_domain(self, ctx: DomainContext) -> None:
        """Drop the domain resource."""

    @abstractmethod
    def domain_status(self, ctx: DomainContext) -> Health:
        """Is the domain reachable?"""

    # ==================================================================
    # Data plane (optional — defaults suit no-schema / no-load systems)
    # ==================================================================

    def install_schema(self, ctx: DomainContext, model: str, *, force: bool = False) -> None:
        """Run the model's `<slug>.ddl` against the domain (dropping first when
        `force`). Only SQL systems (`supports_sql`) implement this; cli guards
        the call, so the default just refuses."""
        raise Unsupported(f"{self.slug} has no SQL schema to install")

    def load_data(self, ctx: DomainContext, req: LoadRequest) -> int:
        """Load generated model data into the domain; return the row/file/record
        count. Default: unsupported (a backend without a data plane)."""
        raise Unsupported(f"{self.slug} has no data-load path")

    def _model_ddl_text(self, model: str) -> str:
        """Read the model's `<slug>.ddl` (for SQL systems)."""
        from .. import data_gen
        ddl = data_gen.model_dir(model) / f"{self.slug}.ddl"
        if not ddl.is_file():
            raise DatagenError(
                f"model '{model}' has no DDL for {self.slug} (looked for {ddl})")
        return ddl.read_text()

    # ==================================================================
    # Shared CLI + option resolution (was duplicated in cloud/ and saas/)
    # ==================================================================

    def _all_options(self) -> tuple[Option, ...]:
        """Declared options, plus SYS_PATH when the system has a staging root."""
        if self.staging_root:
            return (*self.options,
                    Option("SYS_PATH", "sys-path", is_path=True))
        return self.options

    def add_create_options(self, parser: argparse.ArgumentParser) -> None:
        """Add the `create` config flags for this system. The framework
        (tdx.py) calls this instead of branching on category. Container
        subclasses extend it with --port / --ready-timeout."""
        prefix = self.default_env_prefix
        for opt in self._all_options():
            env = f"${prefix}__{opt.name}"
            parser.add_argument(f"--{opt.cli_flag}", dest=f"lit_{opt.name}",
                                default=None, metavar=opt.name,
                                help=f"optional; literal, else read from {env}")
            parser.add_argument(f"--env:{opt.cli_flag}", dest=f"env_{opt.name}",
                                default=None, metavar="ENV_VAR",
                                help=f"optional; read from $ENV_VAR (overrides {env})")
        if self._all_options():
            parser.add_argument("--env-prefix", dest="env_prefix", default=prefix,
                                metavar="PREFIX",
                                help=f"optional; $<PREFIX>__<OPT> (default: {prefix})")

    def resolve_create_settings(self, ns) -> dict[str, Setting]:
        """Resolve `lit_<X>` / `env_<X>` / `--env-prefix` from a parsed
        Namespace into `{name: Setting}`. This is the ONE copy of the logic
        that cloud/base.py and saas/base.py duplicate today."""
        prefix = getattr(ns, "env_prefix", None) or self.default_env_prefix
        if not re.fullmatch(r"[A-Za-z0-9_]+", prefix) or set(prefix) == {"_"}:
            raise DatagenError(
                f"--env-prefix must be alphanumeric/underscore, not all "
                f"underscores (got: {prefix!r})")
        out: dict[str, Setting] = {}
        for opt in self._all_options():
            lit = getattr(ns, f"lit_{opt.name}", None)
            env = getattr(ns, f"env_{opt.name}", None)
            if lit and env:
                raise DatagenError(
                    f"specify either --{opt.cli_flag} or --env:{opt.cli_flag}")
            if not lit and not env:
                env = f"{prefix}__{opt.name}"       # default-prefix fallback
            out[opt.name] = Setting(name=opt.name, value=lit, env_var=env,
                                    is_path=opt.is_path)
        return out

    def settings_from_meta(self, meta: dict[str, str]) -> dict[str, Setting]:
        """Rebuild settings from an existing descriptor's resolved `meta` (for
        destroy / status / domain create) — the inverse of resolution. Carries
        both the concrete `value` (for connecting now) AND any forwarded
        `FORWARDED_<name>_ENV` reference (so re-baking into a domain descriptor
        keeps the secret a reference, not a literal)."""
        return {
            opt.name: Setting(
                name=opt.name,
                value=meta.get(opt.name) or None,
                env_var=meta.get(f"FORWARDED_{opt.name}_ENV") or None,
                is_path=opt.is_path,
            )
            for opt in self._all_options()
        }

    def resolve_values(self, settings: dict[str, Setting]) -> dict[str, str]:
        """Turn Settings into concrete values for the backend to connect NOW:
        the literal `value` if present, else the current value of the forwarded
        env var (erroring if unset). Paths are normalised + validated. This
        feeds the live API call only — descriptors keep the Setting (reference),
        so this never causes a secret to be written to disk."""
        out: dict[str, str] = {}
        for s in settings.values():
            val = s.value
            if val is None and s.env_var:
                val = os.environ.get(s.env_var, "")
                if not val:
                    raise DatagenError(f"env var '{s.env_var}' is unset / empty")
            val = val or ""
            if s.is_path:
                val = _normalize_path(val)
                _validate_path(val, s.name)
            out[s.name] = val
        return out
