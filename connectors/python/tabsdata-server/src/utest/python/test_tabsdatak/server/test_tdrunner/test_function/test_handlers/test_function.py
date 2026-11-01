#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import importlib
import os
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from tabsdatak._info import PublisherInfo, SubscriberInfo
from tabsdatak.api import FunctionCtx, TableFrameSpec, TableFramesSpec
from tabsdatak.server._tdrunner.function._handlers.function import (
    FunctionFailedError,
    InputModeError,
    NotATupleError,
    ResultCountError,
    ResultTypeError,
    TypeHintsError,
    UnknownDestinationError,
    UserFunction,
)
from tabsdatak.spi import TableMode
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode
from test_tabsdatak.server.conftest import TESTING_RESOURCES_FOLDER

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

HANDLER_MODULE = "tabsdatak.server._tdrunner.function._handlers.function"

PROJECTS = Path(TESTING_RESOURCES_FOLDER) / "run_function"


def copy_project(
    tmp_path: Path,
    monkeypatch,
    name: str,
) -> Path:
    project = tmp_path / name
    shutil.copytree(
        PROJECTS / name,
        project,
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    monkeypatch.syspath_prepend(str(project))
    importlib.invalidate_caches()
    return project


def forget(package: str) -> None:
    for name in list(sys.modules):
        if name == package or name.startswith(f"{package}."):
            del sys.modules[name]


@pytest.fixture
def fixture_publisher(tmp_path, monkeypatch) -> PublisherInfo:
    (tmp_path / "srcdata").mkdir()
    copy_project(
        tmp_path,
        monkeypatch,
        "publisher_project",
    )
    from my_pkg.publisher import my_publisher  # noqa: E402

    yield my_publisher
    forget("my_pkg")


@pytest.fixture
def fixture_subscriber(tmp_path, monkeypatch) -> SubscriberInfo:
    copy_project(
        tmp_path,
        monkeypatch,
        "sub_project",
    )
    from sub_pkg.subscriber import my_subscriber  # noqa: E402

    yield my_subscriber
    forget("sub_pkg")


def user_function(function) -> UserFunction:
    return UserFunction(SimpleNamespace(func=function))


def fake_table_frame() -> TableFrame:
    return TableFrame._build(pl.DataFrame({"x": [1]}).lazy(), CreationMode.MUT)


# --- kind -------------------------------------------------------------------


def test_a_publisher_is_not_a_subscriber(fixture_publisher):
    function = UserFunction(fixture_publisher)
    assert function.publisher
    assert not function.subscriber


def test_a_subscriber_is_not_a_publisher(fixture_subscriber):
    function = UserFunction(fixture_subscriber)
    assert function.subscriber
    assert not function.publisher


# --- table_modes ------------------------------------------------------------


def single_slot_function(in_1: TableFrameSpec):
    pass


def array_slot_function(in_1: TableFramesSpec):
    pass


def mixed_slot_function(
    first: TableFrameSpec,
    second: TableFramesSpec,
    third: TableFrameSpec,
):
    pass


def function_without_parameters():
    pass


def unannotated_function(in_1):
    pass


def wrongly_annotated_function(in_1: int):
    pass


def unresolvable_function(in_1: NoSuchType):  # noqa: F821
    pass


def test_table_modes_of_a_single_slot():
    assert user_function(single_slot_function).table_modes() == [TableMode.SINGLE]


def test_table_modes_of_an_array_slot():
    assert user_function(array_slot_function).table_modes() == [TableMode.ARRAY]


def test_table_modes_of_mixed_slots():
    assert user_function(mixed_slot_function).table_modes() == [
        TableMode.SINGLE,
        TableMode.ARRAY,
        TableMode.SINGLE,
    ]


def test_table_modes_without_parameters_are_empty():
    assert user_function(function_without_parameters).table_modes() == []


def test_table_modes_of_an_unannotated_parameter_raise_input_mode():
    with pytest.raises(InputModeError) as error:
        user_function(unannotated_function).table_modes()
    assert error.value.parameter == "in_1"


def test_table_modes_of_a_wrong_annotation_raise_input_mode():
    with pytest.raises(InputModeError) as error:
        user_function(wrongly_annotated_function).table_modes()
    assert error.value.parameter == "in_1"


def test_table_modes_of_unresolvable_hints_raise_type_hints():
    with pytest.raises(TypeHintsError, match="NoSuchType"):
        user_function(unresolvable_function).table_modes()


def test_table_modes_skip_the_trailing_context():
    def function_with_context(
        in_1: TableFrameSpec,
        context: FunctionCtx,
    ) -> TableFrameSpec:
        return None

    function = user_function(function_with_context)
    assert function.takes_context
    assert function.table_modes() == [TableMode.SINGLE]


def test_table_modes_skip_nothing_without_context():
    def function_without_context(
        in_1: TableFrameSpec,
        in_2: TableFrameSpec,
    ) -> TableFrameSpec:
        return None

    function = user_function(function_without_context)
    assert not function.takes_context
    assert function.table_modes() == [TableMode.SINGLE, TableMode.SINGLE]


# --- output_tables ----------------------------------------------------------


def test_output_tables_of_a_publisher_are_the_declared_names(fixture_publisher):
    assert UserFunction(fixture_publisher).output_tables() == ["my_table", "untouched"]


def test_output_tables_of_a_subscriber_are_synthesised_from_its_cardinality(
    fixture_subscriber,
):
    assert UserFunction(fixture_subscriber).output_tables() == ["out_0"]


def test_output_tables_of_an_unregistered_destination_raise_unknown_destination(
    fixture_subscriber,
    monkeypatch,
):
    def missing_destination(_destination_class):
        raise KeyError("no DestDef registered")

    monkeypatch.setattr(f"{HANDLER_MODULE}.get_dest", missing_destination)
    with pytest.raises(UnknownDestinationError) as error:
        UserFunction(fixture_subscriber).output_tables()
    assert error.value.destination_type == type(fixture_subscriber.destination).__name__
    assert "no DestDef registered" in error.value.detail


# --- call -------------------------------------------------------------------


def test_call_returns_the_results():
    def function(in_1):
        return (in_1, None)

    result = user_function(function).call([fake_table_frame()], ["t1", "t2"])
    assert isinstance(result, list)
    assert len(result) == 2
    assert isinstance(result[0], TableFrame)
    assert result[1] is None


def test_call_runs_without_the_storage_secrets(monkeypatch):
    variable = "SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AWS_SECRET_ACCESS_KEY"
    monkeypatch.setenv(variable, "secret")
    seen = {}

    def function(in_1):
        seen["value"] = os.environ.get(variable)
        return in_1

    user_function(function).call([fake_table_frame()], ["t1"])

    assert seen == {"value": None}
    assert os.environ[variable] == "secret"


def test_call_of_a_raising_function_raises_function_failed():
    def function(in_1):
        raise RuntimeError("boom")

    with pytest.raises(FunctionFailedError) as error:
        user_function(function).call([fake_table_frame()], ["t1"])
    assert error.value.detail == "boom"


def test_call_of_a_bare_result_for_many_outputs_raises_not_a_tuple():
    def function(in_1):
        return fake_table_frame()

    with pytest.raises(NotATupleError) as error:
        user_function(function).call([fake_table_frame()], ["t1", "t2"])
    assert error.value.actual_type == "TableFrame"


def test_call_wraps_a_bare_single_output():
    def function(in_1):
        return in_1

    result = user_function(function).call([fake_table_frame()], ["t1"])
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], TableFrame)


def test_call_wraps_a_bare_single_none():
    def function(in_1):
        return None

    assert user_function(function).call([fake_table_frame()], ["t1"]) == [None]


def test_call_of_a_wrong_result_count_raises_result_count():
    def function(in_1):
        return (fake_table_frame(), fake_table_frame())

    with pytest.raises(ResultCountError) as error:
        user_function(function).call([fake_table_frame()], ["t1"])
    assert error.value.returned == 2
    assert error.value.tables == ["t1"]


def test_call_of_a_wrong_result_type_raises_result_type():
    def function(in_1):
        return ("not a table frame",)

    with pytest.raises(ResultTypeError) as error:
        user_function(function).call([fake_table_frame()], ["t1"])
    assert error.value.slot == 0
    assert error.value.table == "t1"
    assert error.value.actual_type == "str"
