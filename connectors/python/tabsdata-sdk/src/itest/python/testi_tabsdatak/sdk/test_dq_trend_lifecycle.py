#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end lifecycle of the system trend transformers, against a live
apiserver.

Registering a function that declares a ``Summary`` with a trend registers a
second, platform-owned function beside it -- one that reads the summary table
and maintains the rolling trend. These tests drive that through the SDK and
assert what the server actually stored: the derived name, the ``DQST`` class,
the parent links, and what a later update or removal does to it.

The owning publishers live in ``functions/``, checked in as a user project
would have them; that directory is the ``project_root`` they bundle from. They
are registered but never triggered -- what is under test is what registration
derives, not what an execution computes.

The collections are ``sources`` collections on purpose: a transformer is
normally refused there, and the trend transformer's ``DQSummaryTrends`` class
is what exempts it. Registering into a data-factory collection would not
exercise that rule.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.functions.harness import CollectionFactory
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from tabsdatak.conn.dev import DataGenSrcConn
from tabsdatak.dataquality._symbols import FunctionType, TableType
from tabsdatak.sdk.tabsdata_server import Collection
from testi_tabsdatak.conn.common.function_project import function_module

pytestmark = pytest.mark.sdk

FUNCTIONS = Path(__file__).parent / "functions"
publisher = function_module(FUNCTIONS, "publisher")

# The name the owning function is registered under. Fixed rather than taken
# from the python function's name, so a test can update one declaration onto
# another without the owner appearing to be renamed.
OWNER = "dq_trend_owner"

TREND_SUFFIX = "_trend"

# `TableStatus::Frozen` on the wire; the SDK exposes no symbol for it.
FROZEN = "F"
DEFAULT_TREND_FUNCTION = f"{publisher.DEFAULT_TREND_TABLE}{TREND_SUFFIX}"
CUSTOM_TREND_FUNCTION = f"{publisher.CUSTOM_TREND_TABLE}{TREND_SUFFIX}"


def source_collection(factory: CollectionFactory) -> Collection:
    return factory("sources", DataGenSrcConn())


def register(collection: Collection, info, name: str = OWNER):
    return collection.register_function(
        info=info, project_root=str(FUNCTIONS), function_name=name
    )


def update(collection: Collection, info, name: str = OWNER, reuse_tables: bool = False):
    return collection.update_function(
        name,
        info=info,
        project_root=str(FUNCTIONS),
        new_function_name=name,
        reuse_tables=reuse_tables,
    )


def function_names(collection: Collection) -> set[str]:
    return {function.name for function in collection.list_functions()}


def table_names(collection: Collection) -> set[str]:
    return {table.name for table in collection.list_tables()}


def trend_functions(collection: Collection, owner_id: str) -> list:
    """The trend functions the server says belong to ``owner_id`` -- found by
    the parent link, which is what the reconciler itself keys on."""
    return collection.list_functions(
        filter=[
            f"parent_function_id:eq:{owner_id}",
            f"function_type:eq:{FunctionType.DQSummaryTrends.value}",
        ]
    )


# --------------------------------------------------------------------------- #
# Registration
# --------------------------------------------------------------------------- #
@tabsdata_instance(consumer="tdtest")
def test_registering_a_summary_registers_its_trend_function(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_with_default_trend)

    assert DEFAULT_TREND_FUNCTION in function_names(collection)


@tabsdata_instance(consumer="tdtest")
def test_the_trend_function_is_registered_as_a_platform_function(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    # `DQST` is what exempts it from the rule refusing a transformer in a
    # source collection, so a regression here fails registration outright.
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_with_default_trend)

    trend = collection.get_function(DEFAULT_TREND_FUNCTION)
    assert trend.function_type == FunctionType.DQSummaryTrends.value


@tabsdata_instance(consumer="tdtest")
def test_the_trend_function_is_parented_to_the_owning_function(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_with_default_trend)

    owner = collection.get_function(OWNER)
    owned = trend_functions(collection, owner.kwargs.get("function_id"))
    assert [function.name for function in owned] == [DEFAULT_TREND_FUNCTION]


@tabsdata_instance(consumer="tdtest")
def test_the_trend_table_is_typed_as_a_summary_trend(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_with_default_trend)

    trend_table = collection.get_table(publisher.DEFAULT_TREND_TABLE)
    assert trend_table.kwargs.get("table_type") == TableType.DQSummaryTrends.value


@tabsdata_instance(consumer="tdtest")
def test_a_declared_trend_table_name_is_used_verbatim(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_with_custom_trend)

    assert CUSTOM_TREND_FUNCTION in function_names(collection)
    assert publisher.CUSTOM_TREND_TABLE in table_names(collection)


@tabsdata_instance(consumer="tdtest")
def test_retention_does_not_change_the_derived_names(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    # Retention is baked into the bundled module, not into the naming.
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_with_custom_retention)

    assert DEFAULT_TREND_FUNCTION in function_names(collection)


@tabsdata_instance(consumer="tdtest")
def test_a_summary_that_opted_out_registers_no_trend_function(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_without_trend)

    assert function_names(collection) == {OWNER}
    assert publisher.SUMMARY_TABLE in table_names(collection)


@tabsdata_instance(consumer="tdtest")
def test_a_function_without_data_quality_registers_no_trend_function(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_without_data_quality)

    assert function_names(collection) == {OWNER}


# --------------------------------------------------------------------------- #
# Update
# --------------------------------------------------------------------------- #
@tabsdata_instance(consumer="tdtest")
def test_updating_an_unchanged_declaration_keeps_one_trend_function(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    # The reconciler runs on every update, so re-declaring the same summary
    # must update the existing trend function rather than register a second.
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_with_default_trend)
    update(collection, publisher.pub_with_default_trend)

    assert function_names(collection) == {OWNER, DEFAULT_TREND_FUNCTION}


@tabsdata_instance(consumer="tdtest")
def test_updating_keeps_the_parent_link_intact(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    # The link is carried forward by the server rather than resent, so this is
    # what catches it being dropped on the way through.
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_with_default_trend)
    update(collection, publisher.pub_with_default_trend)

    owner = collection.get_function(OWNER)
    owned = trend_functions(collection, owner.kwargs.get("function_id"))
    assert [function.name for function in owned] == [DEFAULT_TREND_FUNCTION]


@tabsdata_instance(consumer="tdtest")
def test_renaming_the_trend_table_replaces_the_trend_function(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_with_default_trend)
    update(collection, publisher.pub_with_custom_trend)

    assert function_names(collection) == {OWNER, CUSTOM_TREND_FUNCTION}


@tabsdata_instance(consumer="tdtest")
def test_adding_a_trend_to_an_existing_summary_registers_one(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_without_trend)
    assert function_names(collection) == {OWNER}

    update(collection, publisher.pub_with_default_trend)
    assert function_names(collection) == {OWNER, DEFAULT_TREND_FUNCTION}


# --------------------------------------------------------------------------- #
# Deletion
# --------------------------------------------------------------------------- #
@tabsdata_instance(consumer="tdtest")
def test_dropping_the_trend_deletes_its_function(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_with_default_trend)
    update(collection, publisher.pub_without_trend)

    assert function_names(collection) == {OWNER}


@tabsdata_instance(consumer="tdtest")
def test_dropping_data_quality_entirely_deletes_the_trend_function(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_with_default_trend)
    update(collection, publisher.pub_without_data_quality)

    assert function_names(collection) == {OWNER}


@tabsdata_instance(consumer="tdtest")
def test_a_deleted_trend_function_leaves_its_table_behind(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    # Deleting a function freezes its tables rather than dropping them, so the
    # accumulated trend data stays adoptable if the summary comes back.
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_with_default_trend)
    update(collection, publisher.pub_without_trend)

    assert DEFAULT_TREND_FUNCTION not in function_names(collection)
    assert publisher.DEFAULT_TREND_TABLE in table_names(collection)

    trend_table = collection.get_table(publisher.DEFAULT_TREND_TABLE)
    assert trend_table.kwargs.get("status") == FROZEN
