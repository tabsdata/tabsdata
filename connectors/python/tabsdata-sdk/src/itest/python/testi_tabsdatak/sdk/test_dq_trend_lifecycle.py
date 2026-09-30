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


def is_enabled(collection: Collection, name: str) -> bool:
    return collection.get_function(name).enabled


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


# --------------------------------------------------------------------------- #
# Deleting the owner
#
# Nothing cascades in the database -- the parent link is a plain column -- so
# deleting an owner has to follow it, or the trend function is left behind
# reading a summary table nobody writes any more.
# --------------------------------------------------------------------------- #
@tabsdata_instance(consumer="tdtest")
def test_deleting_the_owner_deletes_its_trend_function(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    collection = source_collection(collection_factory)
    owner = register(collection, publisher.pub_with_default_trend)
    assert DEFAULT_TREND_FUNCTION in function_names(collection)

    owner.delete()

    assert function_names(collection) == set()


@tabsdata_instance(consumer="tdtest")
def test_deleting_the_owner_leaves_no_orphan_behind(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    # The same check from the server's side of the link: nothing still claims
    # the deleted function as its parent.
    collection = source_collection(collection_factory)
    owner = register(collection, publisher.pub_with_default_trend)
    owner_id = collection.get_function(OWNER).kwargs.get("function_id")

    owner.delete()

    assert trend_functions(collection, owner_id) == []


@tabsdata_instance(consumer="tdtest")
def test_deleting_a_trend_function_directly_is_allowed(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    # A trend function has no children of its own, so the cascade terminates
    # rather than recursing -- and the owner is left standing.
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_with_default_trend)

    collection.get_function(DEFAULT_TREND_FUNCTION).delete()

    assert function_names(collection) == {OWNER}


@tabsdata_instance(consumer="tdtest")
def test_deleting_an_owner_without_a_trend_function_is_unaffected(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    collection = source_collection(collection_factory)
    owner = register(collection, publisher.pub_without_trend)

    owner.delete()

    assert function_names(collection) == set()


@tabsdata_instance(consumer="tdtest")
def test_re_registering_after_a_delete_restores_the_trend_function(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    # The delete freezes both functions' tables rather than dropping them, so
    # coming back has to adopt them -- which is what `reuse_tables` allows.
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_with_default_trend).delete()

    collection.register_function(
        info=publisher.pub_with_default_trend,
        project_root=str(FUNCTIONS),
        function_name=OWNER,
        reuse_tables=True,
    )

    assert function_names(collection) == {OWNER, DEFAULT_TREND_FUNCTION}


# --------------------------------------------------------------------------- #
# Enabling and disabling the owner
#
# A disabled function is dropped from the execution graph, so a trend function
# left enabled under a disabled owner would never be reached anyway. What the
# cascade settles is the recorded state, and the cron/stage workers that are
# stamped from it.
# --------------------------------------------------------------------------- #
@tabsdata_instance(consumer="tdtest")
def test_disabling_the_owner_disables_its_trend_function(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    collection = source_collection(collection_factory)
    owner = register(collection, publisher.pub_with_default_trend)

    owner.enable(False)

    assert is_enabled(collection, DEFAULT_TREND_FUNCTION) is False


@tabsdata_instance(consumer="tdtest")
def test_enabling_the_owner_enables_its_trend_function_again(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    # The cascade follows both ways, so a maintenance disable/enable of the
    # owner leaves the pair in the state it started in.
    collection = source_collection(collection_factory)
    owner = register(collection, publisher.pub_with_default_trend)

    owner.enable(False)
    owner.enable(True)

    assert is_enabled(collection, OWNER) is True
    assert is_enabled(collection, DEFAULT_TREND_FUNCTION) is True


@tabsdata_instance(consumer="tdtest")
def test_disabling_a_trend_function_leaves_its_owner_enabled(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    # The walk is downward only: a child never decides its parent's state.
    collection = source_collection(collection_factory)
    register(collection, publisher.pub_with_default_trend)

    collection.get_function(DEFAULT_TREND_FUNCTION).enable(False)

    assert is_enabled(collection, OWNER) is True
    assert is_enabled(collection, DEFAULT_TREND_FUNCTION) is False


@tabsdata_instance(consumer="tdtest")
def test_enabling_the_owner_overrides_a_separately_disabled_trend_function(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    # The owner's state wins. A trend function switched off on its own is
    # brought back by the next enable of its owner -- it has no enabled state
    # of its own to preserve -- so disabling the owner, not the trend
    # function, is what keeps one off.
    collection = source_collection(collection_factory)
    owner = register(collection, publisher.pub_with_default_trend)
    collection.get_function(DEFAULT_TREND_FUNCTION).enable(False)

    owner.enable(True)

    assert is_enabled(collection, DEFAULT_TREND_FUNCTION) is True


@tabsdata_instance(consumer="tdtest")
def test_disabling_an_owner_without_a_trend_function_is_unaffected(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
) -> None:
    collection = source_collection(collection_factory)
    owner = register(collection, publisher.pub_without_trend)

    owner.enable(False)

    assert is_enabled(collection, OWNER) is False
    assert function_names(collection) == {OWNER}
