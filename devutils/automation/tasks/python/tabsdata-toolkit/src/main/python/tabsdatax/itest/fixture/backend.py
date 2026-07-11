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
from the store (``$TD_BACKENDS_DIR/{systems,domains}/<name>``, populated by
``tdx-system`` / ``tdx-domain`` before pytest starts). Resolution decrypts the
descriptor with ``$TD_BACKENDS_CRYPTO_KEY``, checks its role + ``system_type``
match the dataclass, and builds it — each field pulled from the descriptor key
of the same (upper-cased) name, pydantic coercing the type. Loads are lazy and
cached for the session.

The declaration and the fixtures a test uses are held in exact agreement at
collection time, keyed by ``(family, name)`` where family is ``system`` or
``domain`` (so ``system_orders`` and ``domain_orders`` are distinct). The check
runs in both directions and aborts the session before any test body when they
disagree:

* every backend a test **declares** (via a decorator) must appear as its
  ``system_<name>`` / ``domain_<name>`` fixture in the test's **own signature**,
  even when only a helper fixture reads it; and
* every backend **reached** anywhere in the fixture graph must be declared.

Env: ``TD_TEST_ON_MISSING_BACKEND`` = ``skip`` | ``fail`` (default) when a
descriptor is absent; ``TD_TEST_SKIP_BACKENDS`` = comma-separated vendor
``system_type``s to skip even when present.
"""

from __future__ import annotations

import dataclasses
import os
from typing import ClassVar

import pytest
from pydantic import ValidationError

from ..tdksystems import crypto, descriptor, store

ON_MISSING_ENV = "TD_TEST_ON_MISSING_BACKEND"  # skip | fail (default: fail)
SKIP_ENV = "TD_TEST_SKIP_BACKENDS"  # comma-separated types to skip

SYSTEM = "system"
DOMAIN = "domain"
_DESCRIPTOR_ROLE = {SYSTEM: "sys", DOMAIN: "env"}

MARKER = "tabsdata_backend"

SYSTEM_FIXTURE_PREFIX = "system_"
DOMAIN_FIXTURE_PREFIX = "domain_"


class System:
    """Base for a system fixture dataclass; set ``system_type`` (the vendor) and
    declare fields named after the descriptor's (lower-cased) info keys."""

    system_type: ClassVar[str]


class Domain:
    """Base for a domain fixture dataclass (see :class:`System`)."""

    system_type: ClassVar[str]


def _on_missing() -> str:
    mode = (os.environ.get(ON_MISSING_ENV) or "fail").strip().lower()
    if mode not in ("skip", "fail"):
        pytest.fail(f"${ON_MISSING_ENV} must be 'skip' or 'fail' (got {mode!r})")
    return mode


def _skip_types() -> set[str]:
    return {t.strip() for t in os.environ.get(SKIP_ENV, "").split(",") if t.strip()}


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

    # secret always from $TD_BACKENDS_CRYPTO_KEY; missing / skip re-checked per call.
    crypto.use_secret_env(None)
    if not path.exists():
        if _on_missing() == "skip":
            pytest.skip(
                f"{backend_type} {name!r} not found at {path} (${ON_MISSING_ENV}=skip)"
            )
        pytest.fail(
            f"{backend_type} {name!r} not found at {path} "
            f"(set ${ON_MISSING_ENV}=skip to skip instead)"
        )
    if system_type in _skip_types():
        pytest.skip(
            f"{backend_type} {name!r} (system_type {system_type!r}) "
            f"skipped via ${SKIP_ENV}"
        )

    key = (str(path), cls)
    if key not in _CACHE:
        doc = descriptor.load(path)
        if doc.get("role") != _DESCRIPTOR_ROLE[backend_type]:
            pytest.fail(
                f"{name!r} is a {doc.get('kind')} descriptor, not a {backend_type}"
            )
        found = doc.get("system", "")
        if found != system_type:
            pytest.fail(
                f"{name!r} is a {found!r} {backend_type}, but was requested "
                f"as {cls.__name__} (system_type {system_type!r})"
            )
        _CACHE[key] = _construct(cls, descriptor.attributes(doc), name=name)
    return _CACHE[key]


def _construct(cls, attrs: dict[str, object], *, name: str):
    """Build `cls` from the flat attribute dict (field ← key ``field_name.upper()``).
    Values are strings except ``ENTITIES`` (the ``{table: rows}`` mapping)."""
    kwargs = {
        f.name: attrs[f.name.upper()]
        for f in dataclasses.fields(cls)
        if f.name.upper() in attrs
    }
    try:
        return cls(**kwargs)
    except (ValidationError, TypeError, ValueError) as exc:
        missing = [
            f.name.upper()
            for f in dataclasses.fields(cls)
            if f.name.upper() not in attrs
            and f.default is dataclasses.MISSING
            and f.default_factory is dataclasses.MISSING
        ]
        hint = f" (descriptor missing key(s): {', '.join(missing)})" if missing else ""
        pytest.fail(
            f"{name!r}: cannot build {cls.__name__} from its descriptor{hint}: {exc}"
        )


def require(name: str, cls: type):
    """Decorator: declare the test needs backend `name`, loaded as `cls`. Stack to
    request several; each one is injected through its ``system_<name>`` /
    ``domain_<name>`` fixture."""
    return getattr(pytest.mark, MARKER)(name, cls)


def declared_backends(request) -> dict[str, type]:
    declared: dict[str, type] = {}
    for marker in request.node.iter_markers(MARKER):
        name, cls = marker.args
        declared.setdefault(name, cls)
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


def resolve_declared(request, name: str, expected: type | None = None):
    declared = declared_backends(request)
    if name not in declared:
        pytest.fail(
            f"No backend {name!r} declared — add a @<type>_system / "
            f"@<type>_domain decorator for it",
            pytrace=False,
        )
    cls = declared[name]
    if expected is not None and not issubclass(cls, expected):
        pytest.fail(
            f"Backend {name!r} is declared as {cls.__name__}, "
            f"not a {expected.__name__.lower()}",
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
            missing = sorted(
                f"{_kind_prefix(kind)}{name}" for kind, name in unlisted
            )
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
