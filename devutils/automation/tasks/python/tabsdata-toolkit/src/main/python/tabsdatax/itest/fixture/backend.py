#
# Copyright 2026 Tabsdata Inc.
#

"""Inject tdd-provisioned systems / domains into tests as typed dataclasses.

A test declares the backends it needs by name with the per-type decorators
(`fs_system`, `mysql_domain`, …) and receives each one through its own
``system_<name>`` / ``domain_<name>`` fixture::

    @mysql_domain("orders")
    @s3_system("landing")
    def test_pipeline(domain_orders: MysqlDomain, system_landing: S3System):
        ...

Each decorator emits a ``tabsdata_backend(name, dataclass)`` marker; the
``system_<name>`` / ``domain_<name>`` fixtures are generated at plugin import
from ``tdd system list`` / ``tdd domain list`` (the store ``tdd system create`` /
``tdd domain create`` populate before pytest starts). Resolution reads the
descriptor through ``tdd ... info`` (tdd decrypts it with ``$_TDD_CRYPTO_KEY``),
checks its role + ``system_type`` match the dataclass, and builds it — each field
pulled from the descriptor key of the same (upper-cased) name, pydantic coercing
the type. Loads are lazy and cached for the session.

The plugin loads in every pytest run, most of which use no backend and have no
key, so a store tdd cannot list generates no fixtures and fails only the tests
that declare a backend, with tdd's own message.

Resolution order
----------------

A backend is looked for in three places, in this order:

1. **The session cache**, per ``(family, store, name, dataclass)``.
2. **``<UPCASE_NAME>__<KEY>`` environment variables**, the shape
   ``tdd domain info --format export --prefix <NAME>`` emits. A *complete* set is
   the whole backend: neither tdd nor ``$_TDD_CRYPTO_KEY`` is touched, which is
   how a run hands its backends to processes that have neither. ``__ENTITIES`` is
   read as a JSON object (``{"city": 4079}``) so a seeded domain's row counts
   survive; ``NAME`` is filled from the requested name.
3. **The descriptor**, through tdd, which stays the source of truth.

An *incomplete* prefix is not an error — it warns on stderr and falls through to
the descriptor, keeping **none** of the environment's values, so the two sources
are never mixed. Falling through rather than failing is what makes a collision
harmless: ``<NAME>__<KEY>`` is also the shape tdd's own create-time options use,
so a system named ``s3`` shares a prefix with the ``S3__BUCKET`` / ``S3__REGION``
variables ``tdd system create`` reads.

**A stale export outranks a fresh descriptor.** ``tdd domain create`` mints a new
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
``fail`` (default) when the store has no such backend; ``TD_TEST_SKIP_SYSTEMS`` /
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

from . import _tdd, operations
from ._tdd import DOMAIN, SYSTEM, TddError

# skip | fail (default: fail) when the store has no such backend, per family.
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
    :meth:`modify_data`, which drive the matching ``tdd domain`` verbs against
    this domain (by ``name``) through :mod:`.operations` — the shared util all
    domains use, so seeding/mutating a domain is the same for every backend.
    Each returns a :class:`.operations.ChangeReport` (the verb's
    ``{entity: {insert, update, delete}}`` output). They run against the same
    store the fixture loaded from, tdd decrypting with the ambient
    ``$_TDD_CRYPTO_KEY`` — nothing else to wire up; a failure raises
    :class:`._tdd.TddError` with tdd's message.
    """

    system_type: ClassVar[str]
    name: str

    def create_model(
        self, *, model: str, force: bool = False
    ) -> operations.ChangeReport:
        """Install `model`'s schema via ``tdd domain create-model``."""
        return operations.create_model(self.name, model=model, force=force)

    def create_data(
        self, *, size: int = 1, force: bool = False
    ) -> operations.ChangeReport:
        """Load deterministic initial data via ``tdd domain create-data``."""
        return operations.create_data(self.name, size=size, force=force)

    def modify_data(
        self,
        *,
        entity: str,
        sets: int,
        ops: str = "Mix",
        seed: str | int | None = None,
    ) -> operations.ChangeReport:
        """Apply deterministic INSERT/UPDATE/DELETE changes via ``tdd domain
        modify-data`` (``entity`` a name or ``ALL``; ``seed`` ``AUTO`` or int)."""
        return operations.modify_data(
            self.name, entity=entity, sets=sets, ops=ops, seed=seed
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


# (family, store, name, cls) -> built
_CACHE: dict[tuple[str, str, str, type], object] = {}


def load(name: str, cls: type):
    """Load the backend named `name` as `cls`, or skip / fail per the env policy."""
    system_type = getattr(cls, "system_type", None)
    if not system_type:
        pytest.fail(f"{cls.__name__} must define a `system_type`")
    backend_type = _kind_of(cls)

    # A backend the developer asked to skip is skipped whether or not it is
    # reachable at all.
    if name in _skip_names(backend_type):
        pytest.skip(
            f"Requested {backend_type} {name!r} is listed in "
            f"${SKIP_ENV[backend_type]}, so this test is skipped."
        )

    key = (backend_type, str(_tdd.store_dirs[backend_type]), name, cls)
    if key in _CACHE:
        return _CACHE[key]

    # Then the environment, ahead of the descriptor and of needing tdd at all: a
    # complete `<NAME>__<KEY>` set is the whole backend.
    from_env = _from_env(cls, name, backend_type)
    if from_env is not None:
        _CACHE[key] = from_env
        return _CACHE[key]

    try:
        info = _tdd.descriptor(backend_type, name)
    except TddError as exc:
        _unavailable(backend_type, name, exc)
    if info["role"] != backend_type:
        pytest.fail(f"{name!r} is a {info['role']!r} descriptor, not a {backend_type}")
    if info["system"] != system_type:
        pytest.fail(
            f"{name!r} is a {info['system']!r} {backend_type}, but was requested "
            f"as {cls.__name__} (system_type {system_type!r})"
        )
    _CACHE[key] = _construct(cls, _attributes(backend_type, info), name=name)
    return _CACHE[key]


def _unavailable(backend_type: str, name: str, exc: TddError):
    """Skip or fail a backend tdd could not read: per the missing policy when the
    store has no such name, else with tdd's own message (never skipped, since
    the backend is there and something else is wrong)."""
    try:
        missing = name not in _tdd.names(backend_type)
    except TddError:
        missing = False
    if not missing:
        pytest.fail(f"{backend_type} {name!r}: {exc}", pytrace=False)
    if _on_missing(backend_type) == "skip":
        pytest.skip(
            f"Provisioned {backend_type} {name!r} is not in the tdd store, so this "
            f"test is skipped (${ON_MISSING_ENV[backend_type]}=skip)."
        )
    pytest.fail(
        f"Provisioned {backend_type} {name!r} is not in the tdd store. "
        f"Set ${ON_MISSING_ENV[backend_type]}=skip to skip instead of fail."
    )


def _env_attributes(name: str) -> dict[str, object] | None:
    """The flat ``{KEY: value}`` view read from ``<UPCASE_NAME>__<KEY>`` environment
    variables -- the shape ``tdd domain info --format export --prefix <NAME>`` emits.

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


def _attributes(backend_type: str, info: dict) -> dict[str, object]:
    """The flat ``{KEY: value}`` view the fixture dataclasses map their fields
    onto: the descriptor's connection ``meta`` (already upper-cased keys), plus
    the identity fields (``NAME`` and, for a domain, ``MODEL`` / ``DATA`` /
    ``ENTITIES`` / ``EXTERNAL_NAME`` / ``SYS_NAME``). Values are strings except
    ``ENTITIES`` (the ``{table: rows}`` mapping)."""
    attrs: dict[str, object] = dict(info["meta"])
    attrs[KEY_NAME] = info["name"]
    if backend_type == DOMAIN:
        if info.get("model") is not None:
            attrs[KEY_MODEL] = info["model"]
        if info.get("data") is not None:
            attrs[KEY_DATA] = info["data"]
        if info.get("entities"):
            attrs[KEY_ENTITIES] = info["entities"]
        attrs[KEY_EXTERNAL_NAME] = info["external_name"]
        attrs[KEY_SYS_NAME] = info["sys_name"]
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


# Why each family's store could not be listed at import; reported to the tests
# that declare one of its backends, and to no other.
LISTING_ERRORS: dict[str, TddError] = {}


def _stored_names(backend_type: str) -> list[str]:
    try:
        return _tdd.names(backend_type)
    except TddError as exc:
        LISTING_ERRORS[backend_type] = exc
        return []


for stored_system_name in _stored_names(SYSTEM):
    globals()[f"system_{stored_system_name}"] = system_fixture(stored_system_name)
for stored_domain_name in _stored_names(DOMAIN):
    globals()[f"domain_{stored_domain_name}"] = domain_fixture(stored_domain_name)


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    # Ahead of fixture setup, which would only say the fixture does not exist.
    if not LISTING_ERRORS or not hasattr(item, "iter_markers"):
        return
    for kind, name in sorted(marker_backends(item)):
        if kind in LISTING_ERRORS:
            pytest.fail(
                f"Test needs {kind} {name!r}, but the tdd store could not be "
                f"listed: {LISTING_ERRORS[kind]}",
                pytrace=False,
            )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        f"{MARKER}(name, cls): the test uses the tdd-provisioned backend `name`"
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
