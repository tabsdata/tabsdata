#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import warnings
from contextlib import contextmanager

import pytest
from typing_extensions import deprecated

# noinspection PyProtectedMember
from tabsdatak.common import _annotations as annotations_module

# noinspection PyProtectedMember
from tabsdatak.common._annotations import (
    TabsdataDeprecatedWarning,
    TabsdataUnstableWarning,
    deprecated_category,
    unstable,
    unstable_category,
)


@contextmanager
def _ignore_typing_warnings():
    """PEP 702's ``deprecated`` also emits a DeprecationWarning on
    type-checker compatibility paths; silence it so tests assert only
    on the categories we care about."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        yield


# ---------------------------------------------------------------------------
# Category factories
# ---------------------------------------------------------------------------


def test_deprecated_category_subclasses_tabsdata_deprecated_warning():
    cat = deprecated_category("legacy api", replacement="new_api", since="2.0")
    assert issubclass(cat, TabsdataDeprecatedWarning)
    msg = str(cat())
    assert "legacy api" in msg
    assert "new_api" in msg
    assert "2.0" in msg


def test_deprecated_category_without_replacement_omits_replacement_line():
    cat = deprecated_category("legacy api", since="2.0")
    msg = str(cat())
    assert "legacy api" in msg
    assert "2.0" in msg
    assert "Replacement" not in msg


def test_unstable_category_subclasses_tabsdata_unstable_warning():
    cat = unstable_category("api still in flux", since="2.0")
    assert issubclass(cat, TabsdataUnstableWarning)
    msg = str(cat())
    assert "api still in flux" in msg


# ---------------------------------------------------------------------------
# @unstable -- stacks on top of PEP 702 @deprecated and refires the
# category at every call / construction
# ---------------------------------------------------------------------------


@pytest.fixture
def warn_calls(monkeypatch):
    """Spy on the internal ``_warn`` helper so tests can assert on
    the message and category that would be emitted, without dealing
    with the warnings module's filter machinery."""
    calls: list[dict] = []

    def _spy(log_message, msg, category):
        calls.append({"log_message": log_message, "message": msg, "category": category})

    monkeypatch.setattr(annotations_module, "_warn", _spy)
    return calls


def test_unstable_function_uses_deprecated_metadata(warn_calls):
    cat = unstable_category(reason="fn", since="1.0.0")

    @unstable()
    @deprecated("function is deprecated", category=cat)
    def sample() -> str:
        return "ok"

    with _ignore_typing_warnings():
        assert sample() == "ok"

    assert len(warn_calls) == 1
    assert warn_calls[0]["message"] == "function is deprecated"
    assert warn_calls[0]["category"] is cat


def test_unstable_class_uses_deprecated_metadata(warn_calls):
    cat = unstable_category(reason="cls", since="2.0.0")

    @unstable()
    @deprecated("class is deprecated", category=cat)
    class Sample:
        def __init__(self) -> None:
            self.value = "class"

    with _ignore_typing_warnings():
        instance = Sample()
    assert instance.value == "class"

    assert len(warn_calls) == 1
    assert warn_calls[0]["message"] == "class is deprecated"
    assert warn_calls[0]["category"] is cat


def test_unstable_requires_deprecated_metadata_otherwise_raises():
    def plain() -> int:
        return 1

    with pytest.raises(AttributeError):
        unstable()(plain)  # missing __deprecated__ attribute set by @deprecated


def test_unstable_rejects_non_callable_non_class():
    with pytest.raises(TypeError):
        unstable()(42)  # neither class nor callable


def test_unstable_decorated_function_fires_warning_when_not_stubbed():
    """End-to-end: without the ``_warn`` spy, calling an
    ``@unstable @deprecated`` function actually raises the configured
    warning category through the warnings machinery."""
    cat = unstable_category(reason="e2e")

    @unstable()
    @deprecated("e2e", category=cat)
    def f() -> int:
        return 7

    with warnings.catch_warnings(record=True) as records:
        warnings.simplefilter("always", cat)
        warnings.simplefilter("ignore", DeprecationWarning)
        f()
    assert any(issubclass(r.category, cat) for r in records)
