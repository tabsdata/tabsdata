#
# Copyright 2025 Tabsdata Inc.
#

import warnings
from contextlib import contextmanager

import pytest
from typing_extensions import deprecated

from tabsdata._utils import annotations as annotations_module
from tabsdata._utils.annotations import unstable, unstable_category


@pytest.fixture
def warn_calls(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, object]]:
    calls: list[dict[str, object]] = []

    def _spy(log_message: str, msg: str, category: type[Warning]) -> None:
        calls.append(
            {
                "log_message": log_message,
                "message": msg,
                "category": category,
            }
        )

    monkeypatch.setattr(annotations_module, "_warn", _spy)
    return calls


@contextmanager
def _ignore_typing_warnings():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        yield


# noinspection PyDeprecation
def test_unstable_function_uses_deprecated_metadata(
    warn_calls: list[dict[str, object]],
) -> None:
    category = unstable_category(
        reason="fn",
        replacement=None,
        since="1.0.0",
    )

    @unstable()
    @deprecated(
        "function is deprecated",
        category=category,
    )
    def sample() -> str:
        return "ok"

    with _ignore_typing_warnings():
        assert sample() == "ok"

    assert len(warn_calls) == 1
    call = warn_calls[0]
    assert call["message"] == "function is deprecated"
    assert call["category"] is category


# noinspection PyDeprecation
def test_unstable_class_uses_deprecated_metadata(
    warn_calls: list[dict[str, object]],
) -> None:
    category = unstable_category(
        reason="cls",
        replacement=None,
        since="2.0.0",
    )

    @unstable()
    @deprecated(
        "class is deprecated",
        category=category,
    )
    class Sample:
        def __init__(self) -> None:
            self.value = "class"

    with _ignore_typing_warnings():
        instance = Sample()
        assert instance.value == "class"

    assert len(warn_calls) == 1
    call = warn_calls[0]
    assert call["message"] == "class is deprecated"
    assert call["category"] is category


# noinspection PyDeprecation
def test_unstable_method_uses_deprecated_metadata(
    warn_calls: list[dict[str, object]],
) -> None:
    category = unstable_category(
        reason="method",
        replacement=None,
        since="3.0.0",
    )

    class Container:
        @unstable()
        @deprecated(
            "method is deprecated",
            category=category,
        )
        def do_work(self) -> str:
            return "method"

    with _ignore_typing_warnings():
        assert Container().do_work() == "method"

    assert len(warn_calls) == 1
    call = warn_calls[0]
    assert call["message"] == "method is deprecated"
    assert call["category"] is category
