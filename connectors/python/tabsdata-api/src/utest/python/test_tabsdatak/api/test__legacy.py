#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import importlib
import warnings
from datetime import datetime, timezone

import pytest

from tabsdatak import api, spi
from tabsdatak._legacy import FlowCtxLegacy, TransactionCtxLegacy

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


class _Ctx(FlowCtxLegacy, TransactionCtxLegacy):
    collection = "collection"
    function = "function"
    execution_id = "execution"
    transaction_id = "transaction"
    triggered_on = datetime(2026, 6, 4, 8, 34, 28, 376000, tzinfo=timezone.utc)


def deprecations(action):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        value = action()
    return value, [w for w in caught if issubclass(w.category, DeprecationWarning)]


@pytest.mark.parametrize(
    ("module", "name", "target"),
    [
        (api, "TrxCtx", api.TransactionCtx),
        (spi, "FlowCtx", api.FlowCtx),
        (spi, "FunctionCtx", api.FunctionCtx),
    ],
)
def test_a_moved_or_renamed_name_still_resolves_and_warns(module, name, target):
    value, caught = deprecations(lambda: getattr(module, name))
    assert value is target
    assert len(caught) == 1
    assert f"{module.__name__}.{name}" in str(caught[0].message)


def test_a_moved_name_can_still_be_imported():
    def import_it():
        namespace = {}
        exec("from tabsdatak.api import TrxCtx", namespace)
        return namespace["TrxCtx"]

    value, caught = deprecations(import_it)
    assert value is api.TransactionCtx
    assert caught


def test_an_unknown_name_is_still_an_error():
    with pytest.raises(AttributeError, match="NoSuchName"):
        getattr(importlib.import_module("tabsdatak.api"), "NoSuchName")


@pytest.mark.parametrize(
    ("old", "expected"),
    [
        ("collection_name", "collection"),
        ("function_name", "function"),
        ("exec_plan_id", "execution"),
        ("trx_id", "transaction"),
        ("exec_plan_ts", "20260604T083428.376Z"),
    ],
)
def test_a_renamed_attribute_still_reads_and_warns(old, expected):
    value, caught = deprecations(lambda: getattr(_Ctx(), old))
    assert value == expected
    assert len(caught) == 1


def test_the_contexts_carry_the_renamed_attributes():
    assert issubclass(api.FlowCtx, FlowCtxLegacy)
    assert issubclass(api.TransactionCtx, TransactionCtxLegacy)
    assert issubclass(spi.PluginCtx, FlowCtxLegacy)


def test_the_current_names_never_warn():
    def use_current_names():
        class CurrentCtx(api.FunctionCtx):
            project_id = "project"
            project = "project"
            collection_id = "collection"
            collection = "collection"
            function_id = "function"
            function = "function"
            function_version_id = "function_version"
            execution_id = "execution"
            execution = None
            transaction_id = "transaction"
            logical_function_run_id = "logical_function_run"
            physical_function_run_id = "physical_function_run"
            triggered_on = datetime(2026, 6, 4, 8, 34, 28, 376000, tzinfo=timezone.utc)
            secret_names = []

            def secret_keys(self, name):
                return []

            def secret(self, name, key=None):
                return ""

            def get_attr(self, attr):
                return None

            def set_attr(self, attr, val):
                pass

            def add_metadata(self, attr, val):
                pass

        ctx = CurrentCtx()
        assert isinstance(ctx, api.TransactionCtx)
        assert isinstance(ctx, api.FlowCtx)
        return (
            ctx.collection,
            ctx.function,
            ctx.execution_id,
            ctx.transaction_id,
            ctx.triggered_on,
            api.TransactionCtx,
            api.FlowCtx,
            api.FunctionCtx,
            spi.PluginCtx,
        )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        use_current_names()
    assert caught == []
