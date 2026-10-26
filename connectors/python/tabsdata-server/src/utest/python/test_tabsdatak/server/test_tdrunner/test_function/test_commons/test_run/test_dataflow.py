#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tabsdatak.server._tdrunner.function._commons.run import dataflow
from tabsdatak.server._tdrunner.function._commons.run.dataflow import (
    CONTAINER_ROOT,
    NO_SPILL_FRACTION,
    POLARS_OOC_DISK_BUDGET_MB,
    POLARS_OOC_MEMORY_BUDGET_FRACTION,
    POLARS_OOC_SPILL_DIR,
    POLARS_OOMKILL_THRESHOLD_MB,
    POLARS_OVERRIDE_TOTAL_MEMORY_MB,
    POLARS_TEMP_DIR,
    POLARS_VARIABLES,
    TD_DATAFLOW__ABORT__MEMORY_THRESHOLD_PERCENT,
    TD_DATAFLOW__SPILL__DISK_LIMIT_PERCENT,
    TD_DATAFLOW__SPILL__ENABLED,
    TD_DATAFLOW__SPILL__FOLDER,
    TD_DATAFLOW__SPILL__MEMORY_THRESHOLD_PERCENT,
    TD_DATAFLOW__TEMPORARY_FOLDER,
    UNWRITABLE_SPILL_FOLDER,
    Dataflow,
    MemoryLimit,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

GIBIBYTE = 1024 * 1024 * 1024


def configured(**overrides: str) -> dict[str, str]:
    return {
        TD_DATAFLOW__TEMPORARY_FOLDER: "/tabsdata/dataflow/temporary",
        TD_DATAFLOW__SPILL__ENABLED: "true",
        TD_DATAFLOW__SPILL__FOLDER: "/tabsdata/dataflow/spill",
        TD_DATAFLOW__SPILL__MEMORY_THRESHOLD_PERCENT: "40",
        TD_DATAFLOW__SPILL__DISK_LIMIT_PERCENT: "80",
        TD_DATAFLOW__ABORT__MEMORY_THRESHOLD_PERCENT: "95",
        **overrides,
    }


def ten_gibibytes_free(_: Path) -> int:
    return 10 * GIBIBYTE


@pytest.fixture
def clean_environment(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for name in list(os.environ):
        if name.startswith(dataflow.TD_DATAFLOW_PREFIX) or name in POLARS_VARIABLES:
            monkeypatch.delenv(name)
    monkeypatch.delenv(CONTAINER_ROOT, raising=False)
    return monkeypatch


def test_the_settings_become_the_polars_environment():
    settings = Dataflow(configured())

    environment = settings.polars_environment(4 * GIBIBYTE, ten_gibibytes_free)

    assert environment == {
        POLARS_TEMP_DIR: str(Path("/tabsdata/dataflow/temporary")),
        POLARS_OVERRIDE_TOTAL_MEMORY_MB: "4294",
        POLARS_OOC_SPILL_DIR: str(Path("/tabsdata/dataflow/spill")),
        POLARS_OOC_MEMORY_BUDGET_FRACTION: "0.4",
        POLARS_OOC_DISK_BUDGET_MB: "8589",
        POLARS_OOMKILL_THRESHOLD_MB: "4080",
    }


def test_a_missing_setting_takes_its_default():
    settings = Dataflow({TD_DATAFLOW__SPILL__ENABLED: "true"})

    assert settings.temporary_folder == Path("/tabsdata/dataflow/temporary")
    assert settings.spill_enabled
    assert settings.spill_folder == Path("/tabsdata/dataflow/spill")
    assert settings.spill_memory_threshold_percent == 40
    assert settings.spill_disk_limit_percent == 80
    assert settings.abort_memory_threshold_percent == 95


def test_folders_live_under_the_container_root():
    settings = Dataflow(configured(**{CONTAINER_ROOT: "/km/pod"}))

    assert settings.temporary_folder == Path("/km/pod/tabsdata/dataflow/temporary")
    assert settings.spill_folder == Path("/km/pod/tabsdata/dataflow/spill")


def test_spill_off_keeps_every_query_in_memory():
    settings = Dataflow(configured(**{TD_DATAFLOW__SPILL__ENABLED: "false"}))

    environment = settings.polars_environment(4 * GIBIBYTE, ten_gibibytes_free)

    assert environment[POLARS_OOC_MEMORY_BUDGET_FRACTION] == NO_SPILL_FRACTION
    assert environment[POLARS_OOC_SPILL_DIR] == UNWRITABLE_SPILL_FOLDER
    assert POLARS_OOC_DISK_BUDGET_MB not in environment
    assert settings.folders() == [settings.temporary_folder]


@pytest.mark.parametrize(
    "name, value",
    [
        (TD_DATAFLOW__SPILL__MEMORY_THRESHOLD_PERCENT, "0"),
        (TD_DATAFLOW__SPILL__DISK_LIMIT_PERCENT, "101"),
        (TD_DATAFLOW__ABORT__MEMORY_THRESHOLD_PERCENT, "ninety"),
        (TD_DATAFLOW__SPILL__ENABLED, "yes"),
    ],
)
def test_a_malformed_setting_names_itself(name: str, value: str):
    with pytest.raises(ValueError, match=name):
        Dataflow(configured(**{name: value}))


def cgroup_tree(root: Path, membership: str, limits: dict[str, str]) -> Path:
    process_cgroup = root / "proc-self-cgroup"
    process_cgroup.write_text(membership)
    for relative, value in limits.items():
        limit_file = root / "cgroup" / relative
        limit_file.parent.mkdir(parents=True, exist_ok=True)
        limit_file.write_text(value)
    return process_cgroup


@pytest.mark.parametrize(
    "membership, limits, expected",
    [
        ("0::/\n", {"memory.max": "4294967296\n"}, 4 * GIBIBYTE),
        ("0::/\n", {"memory.max": "max\n"}, "physical"),
        (
            "0::/pod/container\n",
            {
                "memory.max": "max",
                "pod/memory.max": "2147483648",
                "pod/container/memory.max": "max",
            },
            2 * GIBIBYTE,
        ),
        (
            "12:cpu:/\n7:memory:/\n",
            {"memory/memory.limit_in_bytes": "1073741824\n"},
            1 * GIBIBYTE,
        ),
        (
            "7:memory:/\n",
            {"memory/memory.limit_in_bytes": "9223372036854771712\n"},
            "physical",
        ),
        ("0::/\n", {"memory.max": "999999999999999999"}, "physical"),
    ],
)
def test_the_memory_limit_is_the_tightest_cgroup_limit_or_the_machine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    membership: str,
    limits: dict[str, str],
    expected: int | str,
):
    monkeypatch.setattr(MemoryLimit, "physical", lambda _: 64 * GIBIBYTE)
    process_cgroup = cgroup_tree(tmp_path, membership, limits)

    limit = MemoryLimit(process_cgroup, tmp_path / "cgroup").in_bytes()

    assert limit == (64 * GIBIBYTE if expected == "physical" else expected)


def test_without_cgroups_the_memory_limit_is_the_machine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(MemoryLimit, "physical", lambda _: 64 * GIBIBYTE)

    limit = MemoryLimit(tmp_path / "missing", tmp_path / "cgroup").in_bytes()

    assert limit == 64 * GIBIBYTE


def test_apply_does_nothing_outside_a_function_pod(clean_environment):
    Dataflow().apply()

    assert not any(name in os.environ for name in POLARS_VARIABLES)


def test_apply_creates_the_folders_and_sets_polars(
    clean_environment: pytest.MonkeyPatch, tmp_path: Path
):
    for name, value in configured(**{CONTAINER_ROOT: str(tmp_path)}).items():
        clean_environment.setenv(name, value)

    Dataflow().apply()

    assert (tmp_path / "tabsdata/dataflow/temporary").is_dir()
    assert (tmp_path / "tabsdata/dataflow/spill").is_dir()
    assert os.environ[POLARS_TEMP_DIR] == str(tmp_path / "tabsdata/dataflow/temporary")
    assert os.environ[POLARS_OOC_SPILL_DIR] == str(tmp_path / "tabsdata/dataflow/spill")
    assert os.environ[POLARS_OOC_MEMORY_BUDGET_FRACTION] == "0.4"
    assert int(os.environ[POLARS_OOC_DISK_BUDGET_MB]) > 0


def test_an_explicit_polars_variable_wins(
    clean_environment: pytest.MonkeyPatch, tmp_path: Path
):
    for name, value in configured(**{CONTAINER_ROOT: str(tmp_path)}).items():
        clean_environment.setenv(name, value)
    clean_environment.setenv(POLARS_OOMKILL_THRESHOLD_MB, "1234")

    Dataflow().apply()

    assert os.environ[POLARS_OOMKILL_THRESHOLD_MB] == "1234"
    assert os.environ[POLARS_OOC_MEMORY_BUDGET_FRACTION] == "0.4"


def test_the_settings_in_effect_are_the_polars_variables_set():
    environment = {
        POLARS_TEMP_DIR: "/t",
        POLARS_OOMKILL_THRESHOLD_MB: "9",
        "HOME": "/h",
    }

    assert Dataflow.settings_in_effect(environment) == {
        POLARS_TEMP_DIR: "/t",
        POLARS_OOMKILL_THRESHOLD_MB: "9",
    }


def test_this_machine_reports_its_memory():
    assert MemoryLimit().in_bytes() > 0
