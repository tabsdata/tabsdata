#
# Copyright 2026 Tabsdata Inc.
#

"""Unified external-system contract.

ONE `System` ABC that every backend implements, so `tdx.py` and `cli.py`
dispatch without any `if category == …` branch::

    sys = registry.get(slug)                 # from systems/<slug>/
    sys.destroy_domain(DomainContext(...))    # category-free

Each system is a directory under `systems/` (`systems/mysql/`, `systems/s3/`,
`systems/snowflake/`, …) whose class subclasses one of the shared bases
(`_container.ContainerBase` / `_cloud.CloudBase` / `_saas.SaasBase` /
`_service.ServiceBase`) and supplies only its per-system hooks. `category`
survives as a *descriptor field
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
  * The `$TD_BACKENDS_CRYPTO_KEY` gate, the venv bootstrap, and model data
    *generation* (model-layer, keyed off `System.kind`).
"""

from __future__ import annotations

import argparse
import getpass
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
    """Guard a path without ever rewriting it: reject multi-line values and
    leading/trailing whitespace. Slashes (leading, trailing, or inner) are left
    exactly as given — the value is used verbatim."""
    if p != p.strip():
        raise DatagenError(
            f"{label} must not have leading/trailing whitespace (got: {p!r})"
        )
    if "\n" in p or "\r" in p:
        raise DatagenError(f"{label} must be a single line (got: {p!r})")
    if ".." in p.split("/"):
        raise DatagenError(f"{label} must not contain '..' (got: {p})")


_NON_IDENT = re.compile(r"[^A-Za-z0-9_]+")


def _ident_token(value: str) -> str:
    """Squeeze an arbitrary string (a username / build number) into an
    identifier-safe token — non-`[A-Za-z0-9_]` runs collapse to a single `_`,
    trimmed at the ends — so it can head a system/schema/database name."""
    tok = _NON_IDENT.sub("_", value).strip("_")
    return tok or "x"


def _os_username() -> str:
    """The OS login name, with env fallbacks for environments where
    `getpass.getuser()` can't read one."""
    try:
        return getpass.getuser()
    except Exception:  # noqa: BLE001 - no controlling user (odd CI); fall back
        return os.environ.get("USER") or os.environ.get("USERNAME") or "user"


def _build_identity() -> str:
    """The CI/dev build identity `<user>_<build>` used to namespace shared
    resources across concurrent runs: `$TD_BUILD_USER` (else the OS login) and
    `$TD_BUILD_NUMBER` (else `0`), each squeezed to an identifier-safe token.
    Heads the domain `DOMAIN_PREFIX` and, for kafka systems, the consumer
    `GROUP_ID`."""
    user = os.environ.get("TD_BUILD_USER") or _os_username()
    build = os.environ.get("TD_BUILD_NUMBER") or "0"
    return f"{_ident_token(user)}_{_ident_token(build)}"


# ======================================================================
# Value types
# ======================================================================


@dataclass(frozen=True)
class Option:
    """A connection option a system declares (drives `create -h` + resolution).

    e.g. Option("BUCKET", "bucket") -> `--bucket` / `--env:bucket` /
    `$<PREFIX>__BUCKET`. `secret=True` masks the value in printed create
    details (was the per-category `_concealed()`)."""

    name: str  # env-var / setting name, e.g. "BUCKET"
    cli_flag: str  # flag stem, e.g. "bucket"
    is_path: bool = False  # validate as a path, used verbatim (e.g. SYS_PATH)
    secret: bool = False  # mask when echoing create details


@dataclass
class Setting:
    """A create-time option: either a literal `value`, or a reference to
    `env_var` whose value is read from the environment and baked into the
    descriptor at create. Exactly one of value / env_var is the source of truth."""

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
    name: str  # logical name (= --sys file name); identity
    out: str  # descriptor path to write
    force: bool = False
    settings: dict[str, Setting] = field(default_factory=dict)  # resolved options
    # container-only knobs; ignored by cloud/saas backends.
    host_port: int | None = None
    ready_timeout: int = 300


@dataclass
class CreateDomainSpec:
    name: str  # logical name (= --domain file name); identity
    out: str  # descriptor path to write
    force: bool = False
    # Extra per-system domain inputs beyond the name (e.g. bigquery's DATASET /
    # DOMAIN_PATH), resolved from the system's `domain_options`.
    options: dict[str, str] = field(default_factory=dict)
    # The name of the entity container to create in the external system (schema
    # / dataset / folder / database). Carries a run prefix so concurrent runs
    # don't collide (see tdx.py's DOMAIN_PREFIX); empty means "no prefix" and
    # `entity_name` falls back to the logical `name`.
    external_name: str = ""

    @property
    def entity_name(self) -> str:
        """The name to give the entity container in the external system — the
        prefixed `external_name` if one was resolved, else the logical `name`."""
        return self.external_name or self.name


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
    containers, every file format under a per-format subdir for object stores /
    fs, JSON + per-entity schema sidecars for kafka / saas-SQL) into `data_dir`;
    each backend reads only the
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
    slug: str = ""  # --type value, dir name under systems/
    label: str = ""  # human name, e.g. "MySQL"
    category: str = ""  # container|cloud|native|saas|service|dev
    # descriptor field + --type grouping ONLY
    kind: str = ""  # data-plane compat tag: sql|file|kafka|
    # mongodb|salesforce (model-vs-system check)
    options: tuple[Option, ...] = ()  # connection options (excl. SYS_PATH)

    # ---- capability flags (replace the isinstance/category checks) ----
    supports_pause: bool = False  # container: stop/start the process
    staging_root: bool = False  # has a SYS_PATH root (cloud/fs +
    # file-staging containers) -> base adds
    # the --sys-path option automatically
    domain_is_named: bool = True  # False for salesforce (creds clone, no
    # schema/db/prefix to create)
    supports_sql: bool = False  # can install_schema + run INSERTs
    supports_models: bool = True  # accepts --model/--data on domain create
    # (False -> tdx-domain rejects them)
    supports_data: bool = True  # loads row data via --data. False -> --model is
    # accepted (records the model; create-only) but --data is rejected — for a
    # system tdx can't seed with rows (e.g. druid: a datasource materialises only
    # when the connector ingests, so tdx stages / loads nothing)
    domain_options: tuple[Option, ...] = ()  # extra create-domain inputs a
    # system needs beyond the name (bigquery:
    # --dataset / --domain-path)
    env_prefix: str = ""  # override the $<PREFIX>__ stem (else the slug
    # upper-cased); azureblob -> AZ
    domain_flag: str = ""  # the system's word for its entity container
    # (postgres: schema, fs: folder, bigquery: dataset); labels create output
    # and is stamped as a redundant exported key on the domain descriptor

    @property
    def default_env_prefix(self) -> str:
        """Default `--env-prefix`: the explicit `env_prefix` if a system sets
        one (azureblob -> AZ), else the slug upper-cased (s3 -> S3)."""
        return self.env_prefix or self.slug.upper()

    def docker_images(self) -> list[tuple[str, str]]:
        """The `(image, platform)` docker images this system runs, for
        `tdx-system types` / `fetch-images`. Empty for systems that start no
        containers (cloud / native / saas / service); a single-container system
        returns one, a compose stack returns each service image."""
        return []

    # ==================================================================
    # System lifecycle
    # ==================================================================

    @abstractmethod
    def plan_system(self, spec: CreateSystemSpec) -> dict[str, Setting]:
        """Compute the COMPLETE descriptor settings — generating any values that
        would otherwise be minted during provisioning (a container's password
        and published port, …) — with NO external side effects.

        The create verbs persist these BEFORE calling `provision_system`, so a
        provision that fails partway can be torn down from the descriptor
        already on disk. Returns Settings (not raw strings) so the framework's
        `descriptor.write` can honour the literal-vs-env distinction: a Setting
        with `env_var` is a forwarded reference (re-resolved at use time, so
        secrets are never written to disk); a Setting with `value` is baked
        literally (and the descriptor file is itself encrypted)."""

    @abstractmethod
    def provision_system(
        self, spec: CreateSystemSpec, settings: dict[str, Setting]
    ) -> None:
        """Provision the system (start the container / verify the bucket /
        validate creds) from the already-planned `settings`. Idempotent under
        `spec.force`; may be retried — the framework destroys any partial
        resource between attempts, so each call starts from a clean slate."""

    def create_system(self, spec: CreateSystemSpec) -> dict[str, Setting]:
        """Plan then provision in one call, returning the planned settings. The
        create verbs call `plan_system` / `provision_system` separately (so the
        full descriptor is on disk before provisioning); this convenience form
        is for callers that don't need the descriptor-first split (e.g.
        `recreate`)."""
        settings = self.plan_system(spec)
        self.provision_system(spec, settings)
        return settings

    def provisioned_meta(self, settings: dict[str, Setting]) -> dict[str, str]:
        """Descriptor keys knowable only AFTER a successful provision (e.g. a
        container's real docker id). The create verbs record these into the
        already-written descriptor once provisioning succeeds. Default: none."""
        return {}

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

    def restart(self, ctx: SystemContext) -> str:
        """Restart the system's container(s) (docker restart) — bringing them
        back after the host machine or Docker was restarted. Default:
        unsupported (a system with no container has nothing to restart)."""
        raise Unsupported(f"{self.slug} does not support restart")

    # ==================================================================
    # Domain lifecycle
    # ==================================================================

    @abstractmethod
    def plan_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec
    ) -> dict[str, Setting]:
        """Compute the COMPLETE domain settings to bake (e.g. SQLALCHEMY_URI +
        system back-refs, or BASE_PATH + the forwarded connection settings),
        generating any per-domain credentials up front, with NO external side
        effects. The entity container is named `spec.entity_name` (the
        run-prefixed external name, defaulting to the logical `spec.name`); the
        descriptor's logical name is stamped separately. Persisted before
        `provision_domain`, same Setting-vs-string contract as `plan_system`."""

    @abstractmethod
    def provision_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec, settings: dict[str, Setting]
    ) -> None:
        """Create the domain resource inside the system — a database, schema,
        Kafka topic, object-store prefix, or directory — from the already-planned
        `settings`. Idempotent under `spec.force`; may be retried."""

    def create_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec
    ) -> dict[str, Setting]:
        """Plan then provision the domain in one call (see `create_system`)."""
        settings = self.plan_domain(ctx, spec)
        self.provision_domain(ctx, spec, settings)
        return settings

    @abstractmethod
    def destroy_domain(self, ctx: DomainContext) -> None:
        """Drop the domain resource."""

    @abstractmethod
    def domain_status(self, ctx: DomainContext) -> Health:
        """Is the domain reachable?"""

    def reset_domain(self, ctx: DomainContext) -> None:
        """Reset the domain's entity container to empty, REUSING its name and
        credentials (drop the tables / collections), so `tdx-domain recreate`
        can replay its DDL/data on a clean slate without minting new
        credentials. Default: unsupported — `recreate` then falls back to
        dropping and re-provisioning the resource (fine for systems with no
        per-domain credentials, e.g. object stores / fs / kafka)."""
        raise Unsupported(f"{self.slug} does not reset domains in place")

    # ==================================================================
    # Data plane (optional — defaults suit no-schema / no-load systems)
    # ==================================================================

    def install_schema(
        self, ctx: DomainContext, model: str, *, force: bool = False
    ) -> None:
        """Run the model's `<slug>.ddl` against the domain (dropping first when
        `force`). Only SQL systems (`supports_sql`) implement this; cli guards
        the call, so the default just refuses."""
        raise Unsupported(f"{self.slug} has no SQL schema to install")

    def load_data(self, ctx: DomainContext, req: LoadRequest) -> int:
        """Load generated model data into the domain; return the row/file/record
        count. Default: unsupported (a backend without a data plane)."""
        raise Unsupported(f"{self.slug} has no data-load path")

    def configure_cdc(self, ctx: DomainContext, model: str) -> None:
        """Configure change-data-capture for the model's tables, once they
        exist (called at the tail of seed / create --model). Default: no-op —
        most systems (and every non-CDC system) capture nothing, so seeding a
        model needs no extra step. A CDC system overrides this to do whatever
        its capture mechanism needs per table (e.g. Postgres `REPLICA IDENTITY
        FULL`, Db2 `asnclp` registration); it may still be a no-op where capture
        is server-wide (MySQL binlog)."""
        return None

    def apply_changes(self, ctx: DomainContext, transactions: list) -> int:
        """Apply planned change transactions (see `changes.Transaction`) to the
        domain, returning the number of transactions applied. Default:
        unsupported (a backend with no change plane). SQL systems apply each
        transaction atomically; file systems take inserts only (new files)."""
        raise Unsupported(f"{self.slug} does not support `changes`")

    @property
    def ddl_slug(self) -> str:
        """The model DDL / recipe flavour to use — normally the system's own
        slug, but a variant system (e.g. `postgrescdc`) overrides this to reuse
        the base system's DDL (`postgres.ddl`) rather than needing its own."""
        return self.slug

    def _model_ddl_text(self, model: str) -> str:
        """Read the model's `<ddl_slug>.ddl` (for SQL systems)."""
        from .. import data_gen

        ddl = data_gen.model_dir(model) / f"{self.ddl_slug}.ddl"
        if not ddl.is_file():
            raise DatagenError(
                f"model '{model}' has no DDL for {self.ddl_slug} (looked for {ddl})"
            )
        return ddl.read_text()

    # ==================================================================
    # Shared CLI + option resolution (was duplicated in cloud/ and saas/)
    # ==================================================================

    def _all_options(self) -> tuple[Option, ...]:
        """Declared options, plus SYS_PATH when the system has a staging root."""
        if self.staging_root:
            return (*self.options, Option("SYS_PATH", "sys-path", is_path=True))
        return self.options

    def add_create_options(self, parser: argparse.ArgumentParser) -> None:
        """Add the `create` config flags for this system. The framework
        (tdx.py) calls this instead of branching on category. Container
        subclasses extend it with --port / --ready-timeout."""
        prefix = self.default_env_prefix
        for opt in self._all_options():
            env = f"${prefix}__{opt.name}"
            parser.add_argument(
                f"--{opt.cli_flag}",
                dest=f"lit_{opt.name}",
                default=None,
                metavar=opt.name,
                help=f"optional; literal, else read from {env}",
            )
            parser.add_argument(
                f"--env:{opt.cli_flag}",
                dest=f"env_{opt.name}",
                default=None,
                metavar="ENV_VAR",
                help=f"optional; read from $ENV_VAR (overrides {env})",
            )
        if self._all_options():
            parser.add_argument(
                "--env-prefix",
                dest="env_prefix",
                default=prefix,
                metavar="PREFIX",
                help=f"optional; $<PREFIX>__<OPT> (default: {prefix})",
            )

    def resolve_create_settings(self, ns) -> dict[str, Setting]:
        """Resolve `lit_<X>` / `env_<X>` / `--env-prefix` from a parsed
        Namespace into `{name: Setting}`. This is the ONE copy of the logic
        that cloud/base.py and saas/base.py duplicate today."""
        prefix = getattr(ns, "env_prefix", None) or self.default_env_prefix
        if not re.fullmatch(r"[A-Za-z0-9_]+", prefix) or set(prefix) == {"_"}:
            raise DatagenError(
                f"--env-prefix must be alphanumeric/underscore, not all "
                f"underscores (got: {prefix!r})"
            )
        out: dict[str, Setting] = {}
        for opt in self._all_options():
            lit = getattr(ns, f"lit_{opt.name}", None)
            env = getattr(ns, f"env_{opt.name}", None)
            if lit and env:
                raise DatagenError(
                    f"specify either --{opt.cli_flag} or --env:{opt.cli_flag}"
                )
            if not lit and not env:
                env = f"{prefix}__{opt.name}"  # default-prefix fallback
            out[opt.name] = Setting(
                name=opt.name, value=lit, env_var=env, is_path=opt.is_path
            )
        return out

    def add_domain_options(self, parser: argparse.ArgumentParser) -> None:
        """Add any per-system `create-domain` args beyond the name (e.g.
        bigquery's --dataset / --domain-path). Default: none. tdx.py calls this
        once it has loaded the target system, so `tdx-domain create --sys NAME
        -h` lists them."""
        for opt in self.domain_options:
            parser.add_argument(
                f"--{opt.cli_flag}",
                dest=f"dom_{opt.name}",
                required=True,
                metavar=opt.name,
                help=f"{self.slug} domain: --{opt.cli_flag}",
            )

    def resolve_domain_options(self, ns) -> dict[str, str]:
        """Collect the declared `domain_options` from a parsed Namespace into a
        `{name: value}` dict (paths validated, never rewritten), for
        CreateDomainSpec.options."""
        out: dict[str, str] = {}
        for opt in self.domain_options:
            val = getattr(ns, f"dom_{opt.name}", None) or ""
            if opt.is_path:
                _validate_path(val, opt.name)
            out[opt.name] = val
        return out

    def settings_from_meta(self, meta: dict[str, str]) -> dict[str, Setting]:
        """Rebuild settings from an existing descriptor's resolved `meta` (for
        destroy / status / domain create) — the inverse of resolution. Values are
        baked into the descriptor, so each Setting carries the concrete `value`;
        there is no env var to re-resolve."""
        return {
            opt.name: Setting(
                name=opt.name,
                value=meta.get(opt.name) or None,
                is_path=opt.is_path,
            )
            for opt in self._all_options()
        }

    def resolve_values(self, settings: dict[str, Setting]) -> dict[str, str]:
        """Turn Settings into concrete values for the backend to connect NOW:
        the literal `value` if present, else the current value of the referenced
        env var (erroring if unset). Paths are validated, never rewritten. Used at
        create time, both to reach the system and (via `descriptor.write`) to
        bake the resolved values into the descriptor."""
        out: dict[str, str] = {}
        for s in settings.values():
            val = s.value
            if val is None and s.env_var:
                val = os.environ.get(s.env_var, "")
                if not val:
                    raise DatagenError(f"env var '{s.env_var}' is unset / empty")
            val = val or ""
            if s.is_path:
                _validate_path(val, s.name)
            out[s.name] = val
        return out
