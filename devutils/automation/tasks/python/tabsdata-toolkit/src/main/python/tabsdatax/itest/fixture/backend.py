#
# Copyright 2026 Tabsdata Inc.
#

"""Inject tdx-provisioned systems / domains into tests as typed dataclasses.

A test declares the backends it needs by name with the per-type decorators
(`fs_system`, `mysql_domain`, …) and receives each one through its own
``system_<name>`` / ``domain_<name>`` fixture::

    @mysql_domain("orders")
    @s3_system("landing")
    def test_pipeline(domain_orders: MysqlDomain, system_landing: S3System):
        ...

Each decorator emits a ``tabsdata_backend(name, dataclass)`` marker; the
``system_<name>`` / ``domain_<name>`` fixtures are generated at plugin import
from the store (``$TD_BACKENDS_DIR``, populated by ``tdx-system2`` /
``tdx-domain2`` before pytest starts). Resolution decrypts the
descriptor with ``$TD_BACKENDS_CRYPTO_KEY``, checks its role + ``system_type``
match the dataclass, and builds it — each field pulled from the descriptor key
of the same (upper-cased) name, pydantic coercing the type. Loads are lazy and
cached for the session.

Resolution order
----------------

A backend is looked for in three places, in this order:

1. **The session cache**, per ``(descriptor path, dataclass)``.
2. **``<UPCASE_NAME>__<KEY>`` environment variables**, the shape
   ``tdx-domain info --format export --prefix <NAME>`` emits. A *complete* set is
   the whole backend: neither the descriptor nor ``$TD_BACKENDS_CRYPTO_KEY`` is
   touched. It exists because decrypting a descriptor costs a 200k-round key
   derivation, which a per-test process pays every time. ``__ENTITIES`` is read as
   a JSON object (``{"city": 4079}``) so a seeded domain's row counts survive;
   ``NAME`` is filled from the requested name.
3. **The descriptor**, which stays the source of truth.

An *incomplete* prefix is not an error — it warns on stderr and falls through to
the descriptor, keeping **none** of the environment's values, so the two sources
are never mixed. Falling through rather than failing is what makes a collision
harmless: ``<NAME>__<KEY>`` is also the shape tdx's own create-time options use,
so a system named ``s3`` shares a prefix with the ``S3__BUCKET`` / ``S3__REGION``
variables ``tdx-system create`` reads.

**A stale export outranks a fresh descriptor.** ``tdx-domain create`` mints a new
password each time, so an environment exported before a re-provision still builds
a complete — and wrong — backend. Re-export after provisioning.

The declaration and the fixtures a test uses are held in exact agreement at
collection time, keyed by ``(family, name)`` where family is ``system`` or
``domain`` (so ``system_orders`` and ``domain_orders`` are distinct). The check
runs in both directions and aborts the session before any test body when they
disagree:

* every backend a test **declares** (via a decorator) must appear as its
  ``system_<name>`` / ``domain_<name>`` fixture in the test's **own signature**,
  even when only a helper fixture reads it; and
* every backend **reached** anywhere in the fixture graph must be declared.

Env, split per family so systems and domains are governed independently:
``TD_TEST_ON_MISSING_SYSTEM`` / ``TD_TEST_ON_MISSING_DOMAIN`` = ``skip`` |
``fail`` (default) when a descriptor is absent; ``TD_TEST_SKIP_SYSTEMS`` /
``TD_TEST_SKIP_DOMAINS`` = comma-separated backend ``name``s to skip even when
present.
"""

from __future__ import annotations

import dataclasses
import json
import os
import sys
from typing import ClassVar

import pytest
from pydantic import ValidationError

from ..tdksystems2 import constants, descriptor, store
from ..tdksystems2.descriptor import DomainInfo, SystemInfo
from ..tdksystems2.errors import LegacyDescriptorError
from . import operations

SYSTEM = "system"
DOMAIN = "domain"
_DESCRIPTOR_ROLE = {SYSTEM: constants.ROLE_SYSTEM, DOMAIN: constants.ROLE_DOMAIN}

# skip | fail (default: fail) when a descriptor is absent, per family.
ON_MISSING_ENV = {
    SYSTEM: "TD_TEST_ON_MISSING_SYSTEM",
    DOMAIN: "TD_TEST_ON_MISSING_DOMAIN",
}
# comma-separated backend `name`s to skip even when present, per family.
SKIP_ENV = {
    SYSTEM: "TD_TEST_SKIP_SYSTEMS",
    DOMAIN: "TD_TEST_SKIP_DOMAINS",
}

MARKER = "tabsdata_backend"

SYSTEM_FIXTURE_PREFIX = "system_"
DOMAIN_FIXTURE_PREFIX = "domain_"

# The identity keys the flat attribute view adds on top of a descriptor's `meta`,
# and the separator between a backend's name and a key in the environment. The
# tests assert the literals rather than these, so renaming a value is caught.
KEY_NAME = "NAME"
KEY_MODEL = "MODEL"
KEY_DATA = "DATA"
KEY_ENTITIES = "ENTITIES"
KEY_EXTERNAL_NAME = "EXTERNAL_NAME"
KEY_SYS_NAME = "SYS_NAME"
ENV_KEY_SEPARATOR = "__"


class System:
    """Base for a system fixture dataclass; set ``system_type`` (the vendor) and
    declare fields named after the descriptor's (lower-cased) info keys."""

    system_type: ClassVar[str]


class Domain:
    """Base for a domain fixture dataclass (see :class:`System`).

    Every domain fixture inherits :meth:`create_model` / :meth:`create_data` /
    :meth:`modify_data`, which drive the matching ``tdx-domain2`` subcommands
    against this domain (by ``name``) through :mod:`.operations` — the shared
    util all domains use, so seeding/mutating a domain is the same for every
    backend. Each returns a :class:`.operations.ChangeReport` (the verb's
    ``{entity: {insert, update, delete}}`` output). They run against the same
    store the fixture loaded from, decrypting with the ambient
    ``$TD_BACKENDS_CRYPTO_KEY`` — nothing else to wire up.
    """

    system_type: ClassVar[str]
    name: str

    def create_model(
        self, *, model: str, force: bool = False, check: bool = True
    ) -> operations.ChangeReport:
        """Install `model`'s schema via ``tdx-domain2 create-model``."""
        return operations.create_model(self.name, model=model, force=force, check=check)

    def create_data(
        self, *, size: int = 1, force: bool = False, check: bool = True
    ) -> operations.ChangeReport:
        """Load deterministic initial data via ``tdx-domain2 create-data``."""
        return operations.create_data(self.name, size=size, force=force, check=check)

    def modify_data(
        self,
        *,
        entity: str,
        sets: int,
        ops: str = "Mix",
        seed: str | int | None = None,
        check: bool = True,
    ) -> operations.ChangeReport:
        """Apply deterministic INSERT/UPDATE/DELETE changes via ``tdx-domain2
        modify-data`` (``entity`` a name or ``ALL``; ``seed`` ``AUTO`` or int)."""
        return operations.modify_data(
            self.name, entity=entity, sets=sets, ops=ops, seed=seed, check=check
        )


def _on_missing(family: str) -> str:
    env = ON_MISSING_ENV[family]
    mode = (os.environ.get(env) or "fail").strip().lower()
    if mode not in ("skip", "fail"):
        pytest.fail(f"${env} must be 'skip' or 'fail' (got {mode!r})")
    return mode


def _skip_names(family: str) -> set[str]:
    env = SKIP_ENV[family]
    return {t.strip() for t in os.environ.get(env, "").split(",") if t.strip()}


_CACHE: dict[tuple[str, type], object] = {}  # (descriptor path, cls) -> built


def load(name: str, cls: type):
    """Load the backend named `name` as `cls`, or skip / fail per the env policy."""
    system_type = getattr(cls, "system_type", None)
    if not system_type:
        pytest.fail(f"{cls.__name__} must define a `system_type`")
    if issubclass(cls, System):
        backend_type, path = SYSTEM, store.system_path(name)
    elif issubclass(cls, Domain):
        backend_type, path = DOMAIN, store.domain_path(name)
    else:
        pytest.fail(f"{cls.__name__} must subclass System or Domain")

    # A backend the developer asked to skip is skipped whether or not it is
    # reachable at all.
    if name in _skip_names(backend_type):
        pytest.skip(
            f"Requested {backend_type} {name!r} is listed in "
            f"${SKIP_ENV[backend_type]}, so this test is skipped."
        )

    key = (str(path), cls)
    if key in _CACHE:
        return _CACHE[key]

    # Then the environment, ahead of the descriptor and of needing it at all: a
    # complete `<NAME>__<KEY>` set is the whole backend, where opening the
    # descriptor costs a 200k-round key derivation.
    from_env = _from_env(cls, name, backend_type)
    if from_env is not None:
        _CACHE[key] = from_env
        return _CACHE[key]

    # The descriptor secret is always $TD_BACKENDS_CRYPTO_KEY (the v2 crypto
    # reads it directly); missing / skip is re-checked per call.
    if not path.exists():
        if _on_missing(backend_type) == "skip":
            pytest.skip(
                f"Provisioned {backend_type} {name!r} was not found at {path}, "
                f"so this test is skipped (${ON_MISSING_ENV[backend_type]}=skip)."
            )
        pytest.fail(
            f"Provisioned {backend_type} {name!r} was not found at {path}. "
            f"Set ${ON_MISSING_ENV[backend_type]}=skip to skip instead of fail."
        )

    if key not in _CACHE:
        try:
            info = descriptor.load(path)
        except LegacyDescriptorError as exc:
            # Never skipped: the backend is there and usable, the store just has
            # to be migrated, and that is one command away.
            pytest.fail(f"{backend_type} {name!r}: {exc}", pytrace=False)
        if info.role != _DESCRIPTOR_ROLE[backend_type]:
            pytest.fail(f"{name!r} is a {info.role!r} descriptor, not a {backend_type}")
        if info.system != system_type:
            pytest.fail(
                f"{name!r} is a {info.system!r} {backend_type}, but was requested "
                f"as {cls.__name__} (system_type {system_type!r})"
            )
        _CACHE[key] = _construct(cls, _attributes(info), name=name)
    return _CACHE[key]


def _env_attributes(name: str) -> dict[str, object] | None:
    """The flat ``{KEY: value}`` view read from ``<UPCASE_NAME>__<KEY>`` environment
    variables -- the shape ``tdx-domain info --format export --prefix <NAME>`` emits.

    ``NAME`` is filled from the requested name (the env set carries the descriptor's
    ``meta``, not its identity) and ``ENTITIES`` is read as a JSON object so a seeded
    domain's row counts survive. ``None`` when the prefix names nothing, which is the
    signal to read the descriptor."""
    prefix = f"{name.upper()}{ENV_KEY_SEPARATOR}"
    attrs: dict[str, object] = {
        key[len(prefix) :]: value
        for key, value in os.environ.items()
        if key.startswith(prefix)
    }
    if not attrs:
        return None
    if KEY_ENTITIES in attrs:
        try:
            attrs[KEY_ENTITIES] = json.loads(attrs[KEY_ENTITIES])
        except ValueError:
            # Absent, exactly as for a domain create-data has not run against.
            del attrs[KEY_ENTITIES]
    attrs.setdefault(KEY_NAME, name)
    return attrs


def _missing_fields(cls, attrs: dict[str, object]) -> list[str]:
    """The dataclass fields `attrs` cannot fill and that have no default."""
    return [
        f.name.upper()
        for f in dataclasses.fields(cls)
        if f.name.upper() not in attrs
        and f.default is dataclasses.MISSING
        and f.default_factory is dataclasses.MISSING
    ]


def _kwargs(cls, attrs: dict[str, object]) -> dict[str, object]:
    return {
        f.name: attrs[f.name.upper()]
        for f in dataclasses.fields(cls)
        if f.name.upper() in attrs
    }


def _from_env(cls, name: str, backend_type: str):
    """Build `cls` from ``<UPCASE_NAME>__<KEY>``, or ``None`` to fall through to the
    descriptor.

    An incomplete prefix is not an error -- but it is never silent, a half-set prefix
    being far more likely a typo or a stale export than a deliberate act. Nothing read
    from the environment is kept in that case, so the two sources are never mixed."""
    attrs = _env_attributes(name)
    if attrs is None:
        return None
    prefix = f"{name.upper()}{ENV_KEY_SEPARATOR}"
    missing = _missing_fields(cls, attrs)
    if missing:
        print(
            f"[td-test] WARNING: ${prefix}* is set but does not describe a whole "
            f"{backend_type} {name!r} (missing key(s): {', '.join(missing)}); "
            f"ignoring it and reading the descriptor instead",
            file=sys.stderr,
        )
        return None
    try:
        return cls(**_kwargs(cls, attrs))
    except (ValidationError, TypeError, ValueError) as exc:
        print(
            f"[td-test] WARNING: ${prefix}* is set but does not build "
            f"{cls.__name__} ({exc}); ignoring it and reading the descriptor instead",
            file=sys.stderr,
        )
        return None


def _attributes(info: SystemInfo | DomainInfo) -> dict[str, object]:
    """The flat ``{KEY: value}`` view the fixture dataclasses map their fields
    onto: the descriptor's connection ``meta`` (already upper-cased keys), plus
    the identity fields (``NAME`` and, for a domain, ``MODEL`` / ``DATA`` /
    ``ENTITIES`` / ``EXTERNAL_NAME`` / ``SYS_NAME``). Values are strings except
    ``ENTITIES`` (the ``{table: rows}`` mapping)."""
    attrs: dict[str, object] = dict(info.meta)
    attrs[KEY_NAME] = info.name
    if isinstance(info, DomainInfo):
        if info.model is not None:
            attrs[KEY_MODEL] = info.model
        if info.data is not None:
            attrs[KEY_DATA] = info.data
        if info.entities:
            attrs[KEY_ENTITIES] = info.entities
        attrs[KEY_EXTERNAL_NAME] = info.external_name
        attrs[KEY_SYS_NAME] = info.sys_name
    return attrs


def _construct(cls, attrs: dict[str, object], *, name: str):
    """Build `cls` from the flat attribute dict (field ← key ``field_name.upper()``).
    Values are strings except ``ENTITIES`` (the ``{table: rows}`` mapping)."""
    try:
        return cls(**_kwargs(cls, attrs))
    except (ValidationError, TypeError, ValueError) as exc:
        missing = _missing_fields(cls, attrs)
        hint = f" (descriptor missing key(s): {', '.join(missing)})" if missing else ""
        pytest.fail(
            f"{name!r}: cannot build {cls.__name__} from its descriptor{hint}: {exc}"
        )


def require(name: str, cls: type):
    """Decorator: declare the test needs backend `name`, loaded as `cls`. Stack to
    request several; each one is injected through its ``system_<name>`` /
    ``domain_<name>`` fixture."""
    return getattr(pytest.mark, MARKER)(name, cls)


def declared_backends(request) -> dict[tuple[str, str], type]:
    declared: dict[tuple[str, str], type] = {}
    for marker in request.node.iter_markers(MARKER):
        name, cls = marker.args
        declared.setdefault((_kind_of(cls), name), cls)
    return declared


def _kind_of(cls: type) -> str:
    if issubclass(cls, System):
        return SYSTEM
    if issubclass(cls, Domain):
        return DOMAIN
    pytest.fail(f"{cls.__name__} must subclass System or Domain")


def marker_backends(node) -> set[tuple[str, str]]:
    """The ``(kind, name)`` pairs a test declares via the backend markers."""
    declared: set[tuple[str, str]] = set()
    for marker in node.iter_markers(MARKER):
        name, cls = marker.args
        declared.add((_kind_of(cls), name))
    return declared


def requested_backends(fixturenames) -> set[tuple[str, str]]:
    """The ``(kind, name)`` pairs reachable through the given fixture names."""
    requested: set[tuple[str, str]] = set()
    for name in fixturenames:
        if name.startswith(SYSTEM_FIXTURE_PREFIX):
            requested.add((SYSTEM, name[len(SYSTEM_FIXTURE_PREFIX) :]))
        elif name.startswith(DOMAIN_FIXTURE_PREFIX):
            requested.add((DOMAIN, name[len(DOMAIN_FIXTURE_PREFIX) :]))
    return requested


def resolve_declared(request, name: str, expected: type):
    declared = declared_backends(request)
    kind = _kind_of(expected)
    cls = declared.get((kind, name))
    if cls is None:
        pytest.fail(
            f"No {kind} {name!r} declared — add a @<type>_system / "
            f"@<type>_domain decorator for it",
            pytrace=False,
        )
    return load(name, cls)


def stored_names(folder) -> list[str]:
    if not folder.is_dir():
        return []
    return sorted(entry.name for entry in folder.iterdir() if entry.is_file())


def system_fixture(name: str):
    @pytest.fixture(name=f"system_{name}")
    def fixture(request):
        return resolve_declared(request, name, System)

    return fixture


def domain_fixture(name: str):
    @pytest.fixture(name=f"domain_{name}")
    def fixture(request):
        return resolve_declared(request, name, Domain)

    return fixture


for stored_system_name in stored_names(store.system_dir()):
    globals()[f"system_{stored_system_name}"] = system_fixture(stored_system_name)
for stored_domain_name in stored_names(store.domain_dir()):
    globals()[f"domain_{stored_domain_name}"] = domain_fixture(stored_domain_name)


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        f"{MARKER}(name, cls): the test uses the tdx-provisioned backend `name`"
        " loaded as `cls`; read it through the `system_<name>` /"
        " `domain_<name>` fixtures. Every declared backend must appear in the"
        " test's own signature (even if only forwarded to a helper), and every"
        " backend reached anywhere in the fixture graph must be declared.",
    )


def _kind_prefix(kind: str) -> str:
    return SYSTEM_FIXTURE_PREFIX if kind == SYSTEM else DOMAIN_FIXTURE_PREFIX


def direct_argnames(item) -> tuple[str, ...]:
    info = getattr(item, "_fixtureinfo", None)
    return tuple(getattr(info, "argnames", ()))


def pytest_collection_modifyitems(config, items):
    violations: list[str] = []
    for item in items:
        if not hasattr(item, "iter_markers"):
            continue
        declared = marker_backends(item)
        reached = requested_backends(getattr(item, "fixturenames", ()))
        signature = requested_backends(direct_argnames(item))
        undeclared = reached - declared
        unlisted = declared - signature
        problems: list[str] = []
        if undeclared:
            requested = sorted(
                f"{_kind_prefix(kind)}{name}" for kind, name in undeclared
            )
            problems.append(
                f"requests backend fixture(s) {requested} without declaring them "
                f"via @<type>_system / @<type>_domain"
            )
        if unlisted:
            missing = sorted(f"{_kind_prefix(kind)}{name}" for kind, name in unlisted)
            problems.append(
                f"declares backend(s) that the test signature does not request; "
                f"add the {missing} fixture argument(s) to the test even if only "
                f"to forward them"
            )
        if problems:
            violations.append(f"{item.nodeid}: {'; '.join(problems)}")
    if violations:
        listing = "\n  ".join(violations)
        raise pytest.UsageError(
            "tabsdata_backend contract violated (declared backends must match "
            f"requested backend fixtures exactly):\n  {listing}"
        )
