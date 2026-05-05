#
# Copyright 2026 Tabs Data Inc.
#
"""External-system framework for live integration tests.

Three moving pieces:

* :class:`ExternalSystem` — ABC every live system (MySQL, OpenAI, …)
  subclasses. Declares which env var gates the test and which
  suffixes the system reads.
* :func:`external` — decorator a test wears to say "I talk to this
  system". Absorbs :data:`pytest.mark.live_extractor` so test bodies
  don't need to apply it separately. Attaches an
  ``external_system`` pytest marker that the :func:`envs` fixture
  reads.
* :fixture:`envs` — injects a ``dict`` of the system's env values,
  keyed by the bare suffix names. The gate env var has one single
  purpose: setting it to ``"skip"`` (case-insensitive) opts the
  test out. Any other state — unset, empty, ``"mysql"``, whatever
  — runs the test. The dict always carries every
  :meth:`ExternalSystem.env_names` key; missing ones come through
  as ``None`` so test code can fall back or assert explicitly.

A developer who doesn't want the MySQL suite locally does
``export MYSQL=skip`` and that suite goes green; a CI run that
expects the live env still runs and surfaces actual connection
failures when something's misconfigured.

Fixture discovery: add ``from tabsdata_testutils.external import
envs  # noqa: F401`` to the nearest ``conftest.py`` to make the
fixture visible in that subtree.
"""

from __future__ import annotations

import abc
import functools
import os
from collections.abc import Callable
from typing import Any

import pytest

# Marker name the rest of the module uses. Declared once so the
# string doesn't drift between the decorator and the fixture.
_EXTERNAL_SYSTEM_MARKER = "external_system"

# Sentinel value for the gate env var: when ``$<gate>`` is set to
# this string (case-insensitive) the test skips instead of running.
# Lets a developer ``export MYSQL=skip`` to tolerate an unconfigured
# system without touching test source.
_SKIP_SENTINEL = "skip"


class ExternalSystem(abc.ABC):
    """Describes one external system's env-var surface for live tests.

    Subclasses override two class methods:

    * :meth:`system_env_name` — the env var whose presence gates the test
      AND the default prefix applied to every suffix in
      :meth:`env_names`. Same value serves both roles because a
      live harness that exports the gate never forgets to export
      the dependent fields under the same prefix (they come from
      the same ``.sh`` script).
    * :meth:`env_names` — the env-var suffixes the system expects.
      When a test runs, each suffix *name* is read from
      ``<prefix>__<name>`` (or :meth:`env_var` if the subclass
      overrides the joining convention) and handed to the test
      keyed by the bare suffix, prefix stripped.
    """

    @classmethod
    @abc.abstractmethod
    def system_env_name(cls) -> str:
        """Return the env var that gates the test (presence ⇒ run).

        Doubles as the default prefix: e.g. ``"MYSQL"`` gates on
        ``$MYSQL`` and reads ``$MYSQL__HOST`` / ``$MYSQL__PORT`` /
        etc. Override the prefix at the call site via
        :func:`external`'s ``prefix`` kwarg to target a second
        instance.
        """

    @classmethod
    @abc.abstractmethod
    def env_names(cls) -> list[str]:
        """Return the env-var suffixes this system reads — without any prefix.

        The actual env var read is ``env_var(prefix, name)`` for each
        entry (``<prefix>__<name>`` by default). Keys in the dict
        handed to the test are the bare suffixes, so test code is
        prefix-agnostic.
        """

    @classmethod
    def env_var(cls, prefix: str, name: str) -> str:
        """Map ``(prefix, suffix)`` to the real env var name.

        Default is ``<prefix>__<name>`` — the ``__`` convention the
        setup scripts use. Subclasses override only when a system's
        native env-var naming deviates from that form; no concrete
        system ships with an override today.
        """
        return f"{prefix}__{name}"


def external(
    *,
    system: type[ExternalSystem],
    prefix: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Mark a test as talking to a live external *system*.

    Implies ``@pytest.mark.live_extractor``. Stores *system* /
    *prefix* on the test via a pytest marker so the :func:`envs`
    fixture can read them at run time.

    The test body receives an ``envs`` dict (via the fixture) keyed
    by the bare suffixes from :meth:`ExternalSystem.env_names`. When
    *prefix* is passed, it overrides :meth:`ExternalSystem.system_env_name`
    as both the gate env var and the prefix applied to each suffix
    — useful for targeting a second instance of the same system.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        # Order matters only for readability in ``pytest --collect-only``;
        # both markers attach to the same function regardless.
        func = pytest.mark.live_extractor(func)
        func = getattr(pytest.mark, _EXTERNAL_SYSTEM_MARKER)(
            system=system,
            prefix=prefix,
        )(func)
        return func

    return decorator


@pytest.fixture
def envs(request: pytest.FixtureRequest) -> dict[str, str | None]:
    """Inject the ``dict`` of env values for the test's ``@external`` system.

    The :func:`external` decorator stores the :class:`ExternalSystem`
    subclass + optional prefix on the test as a pytest marker; this
    fixture reads that marker and builds the dict.

    Gate handling is one-way only: when ``$<gate>`` is set to
    ``"skip"`` (case-insensitive, leading/trailing whitespace
    tolerated) the test skips. Any other state — unset, empty,
    ``"mysql"``, any live value — runs the test.

    The returned dict always carries every
    :meth:`ExternalSystem.env_names` key. Values are the
    corresponding env-var strings, or ``None`` when the env var
    isn't set — test code can fall back (``envs.get("HOST") or
    "localhost"``) or assert explicitly, as makes sense per system.
    """
    marker = request.node.get_closest_marker(_EXTERNAL_SYSTEM_MARKER)
    if marker is None:
        pytest.fail(
            "envs fixture requires @external(system=..., prefix=...) "
            "— the marker was not found on the test",
        )

    system: type[ExternalSystem] = marker.kwargs["system"]
    prefix_override = marker.kwargs.get("prefix")
    effective_prefix = (
        prefix_override if prefix_override is not None else system.system_env_name()
    )

    gate_value = os.environ.get(effective_prefix)
    if gate_value is not None and gate_value.strip().lower() == _SKIP_SENTINEL:
        pytest.skip(
            f"{effective_prefix} set to '{gate_value}' — live test skipped by request",
        )

    # ``os.environ.get`` with no default returns ``None`` when the
    # var is unset — exactly the contract we want for the dict.
    return {
        name: os.environ.get(system.env_var(effective_prefix, name))
        for name in system.env_names()
    }


# Exposed for callers that need the marker name for their own hooks.
EXTERNAL_SYSTEM_MARKER = _EXTERNAL_SYSTEM_MARKER


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):  # noqa: ARG001 — pytest hook signature
    """Tack a "how to skip" hint onto failed ``@external`` tests.

    On failure, the test's final report grows a section like::

        ---- external-system hint ----
        export MYSQL=skip   # to skip this test

    so a developer who sees a red live test knows the one-line
    escape hatch without having to go read docs. Only runs for
    tests that actually carry the ``external_system`` marker (i.e.
    the ones :func:`external` decorated), so plain unit-test
    failures aren't polluted with irrelevant advice.

    Consuming conftests re-export this function so pytest
    discovers it — hooks are collected by name from
    ``conftest.py`` modules, not by cross-package import.
    """
    outcome = yield
    report = outcome.get_result()
    if report.when != "call" or report.outcome != "failed":
        return

    marker = item.get_closest_marker(_EXTERNAL_SYSTEM_MARKER)
    if marker is None:
        return

    system: type[ExternalSystem] = marker.kwargs["system"]
    prefix_override = marker.kwargs.get("prefix")
    gate = prefix_override if prefix_override is not None else system.system_env_name()
    report.sections.append((
        "external-system hint",
        f"export {gate}=skip   # to skip this test",
    ))


@functools.lru_cache(maxsize=None)
def _marker_docs() -> dict[str, str]:
    """Return the marker → short-doc map callers can emit in their
    ``pyproject.toml`` markers section without hand-copying strings."""
    return {
        EXTERNAL_SYSTEM_MARKER: (
            f"{EXTERNAL_SYSTEM_MARKER}(system, prefix): external-system "
            f"metadata attached by @external(...)"
        ),
    }
