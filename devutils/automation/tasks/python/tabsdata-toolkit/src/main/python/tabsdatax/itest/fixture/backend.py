"""Inject tdx-provisioned systems / domains into tests as typed dataclasses.

A test declares the backends it needs by name with the per-type decorators
(`fs_system`, `mysql_domain`, …) and reads them from the `backends` fixture::

    @mysql_domain("orders")
    @s3_system("landing")
    def test_pipeline(backends):
        orders: MysqlDomain = backends["orders"]
        landing: S3System = backends["landing"]

Each decorator records ``(name, dataclass)``; ``backends`` resolves the descriptor
via the store (``$TD_BACKENDS_DIR/{systems,domains}/<name>``), decrypts it with
``$TD_BACKENDS_CRYPTO_KEY``, checks its role + ``system_type`` match the dataclass,
and builds it — each field pulled from the descriptor key of the same (upper-cased)
name, pydantic coercing the type. Loads are lazy and cached for the session.

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

ON_MISSING_ENV = "TD_TEST_ON_MISSING_BACKEND"   # skip | fail (default: fail)
SKIP_ENV = "TD_TEST_SKIP_BACKENDS"              # comma-separated types to skip

SYSTEM = "system"
DOMAIN = "domain"
_DESCRIPTOR_ROLE = {SYSTEM: "sys", DOMAIN: "env"}

_REQUESTED = "_tdx_backends"   # marker on a test fn: [(name, cls), …]


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


_CACHE: dict[tuple[str, type], object] = {}   # (descriptor path, cls) -> built


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
            pytest.skip(f"{backend_type} {name!r} not found at {path} "
                        f"(${ON_MISSING_ENV}=skip)")
        pytest.fail(f"{backend_type} {name!r} not found at {path} "
                    f"(set ${ON_MISSING_ENV}=skip to skip instead)")
    if system_type in _skip_types():
        pytest.skip(f"{backend_type} {name!r} (system_type {system_type!r}) "
                    f"skipped via ${SKIP_ENV}")

    key = (str(path), cls)
    if key not in _CACHE:
        doc = descriptor.load(path)
        if doc.get("role") != _DESCRIPTOR_ROLE[backend_type]:
            pytest.fail(f"{name!r} is a {doc.get('kind')} descriptor, "
                        f"not a {backend_type}")
        found = doc.get("system", "")
        if found != system_type:
            pytest.fail(f"{name!r} is a {found!r} {backend_type}, but was requested "
                        f"as {cls.__name__} (system_type {system_type!r})")
        _CACHE[key] = _construct(cls, descriptor.attributes(doc), name=name)
    return _CACHE[key]


def _construct(cls, attrs: dict[str, str], *, name: str):
    """Build `cls` from the flat attribute dict (field ← key ``field_name.upper()``)."""
    kwargs = {f.name: attrs[f.name.upper()]
              for f in dataclasses.fields(cls) if f.name.upper() in attrs}
    try:
        return cls(**kwargs)
    except (ValidationError, TypeError, ValueError) as exc:
        missing = [f.name.upper() for f in dataclasses.fields(cls)
                   if f.name.upper() not in attrs
                   and f.default is dataclasses.MISSING
                   and f.default_factory is dataclasses.MISSING]
        hint = f" (descriptor missing key(s): {', '.join(missing)})" if missing else ""
        pytest.fail(f"{name!r}: cannot build {cls.__name__} from its "
                    f"descriptor{hint}: {exc}")


def require(name: str, cls: type):
    """Decorator: declare the test needs backend `name`, loaded as `cls`. Stack to
    request several; read them from the `backends` fixture by name."""
    def deco(fn):
        fn.__dict__.setdefault(_REQUESTED, []).append((name, cls))
        return fn
    return deco


class _Backends:
    """Lazy read-only ``name -> loaded backend`` mapping; loads on first access and
    caches, so repeated ``backends[name]`` returns the same object."""

    def __init__(self, requested: list[tuple[str, type]]):
        self._by_name = dict(requested)

    def __getitem__(self, name: str):
        try:
            cls = self._by_name[name]
        except KeyError:
            raise KeyError(f"no backend {name!r} declared — add a @<type>_system "
                           f"/ @<type>_domain decorator for it") from None
        return load(name, cls)

    def __contains__(self, name: str) -> bool:
        return name in self._by_name

    def __iter__(self):
        return iter(self._by_name)

    def __len__(self) -> int:
        return len(self._by_name)


@pytest.fixture
def backends(request) -> _Backends:
    """The backends declared on the test (by the per-type decorators), keyed by name."""
    return _Backends(getattr(request.function, _REQUESTED, []))
