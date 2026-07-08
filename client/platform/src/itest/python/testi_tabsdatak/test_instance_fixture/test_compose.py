#
#  Copyright 2026 Tabs Data Inc.
#

#
#

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.instances.tabsdata import (
    Instance,
    registered_consumers,
    tabsdata_instance,
)

WORDS = ["alpha", "beta"]


@pytest.fixture
def greeting() -> str:
    return "Hello"


@pytest.fixture
def apiserver_url(instance_tdtest: Instance) -> str:
    server = instance_tdtest.apiserver
    return f"http://{server.host}:{server.port}"


@pytest.fixture
def workspace(tmp_path: Path, instance_tdtest: Instance):
    folder = tmp_path / instance_tdtest.name
    folder.mkdir()
    yield folder


@tabsdata_instance(consumer="tdtest")
def test_single_with_plain_fixture(instance_tdtest: Instance, greeting: str) -> None:
    assert greeting == "Hello"
    assert instance_tdtest.introduce().startswith(f"Instance '{instance_tdtest.name}'")


@pytest.mark.parametrize("word", WORDS)
@tabsdata_instance(consumer="tdtest")
def test_single_with_parametrized_argument(
    instance_tdtest: Instance, word: str
) -> None:
    assert word in WORDS
    assert instance_tdtest.name


@tabsdata_instance(consumer="tdtest")
def test_single_with_builtin_fixture(instance_tdtest: Instance, tmp_path: Path) -> None:
    record = tmp_path / f"{instance_tdtest.name}.txt"
    record.write_text(instance_tdtest.introduce())
    assert record.read_text().startswith(f"Instance '{instance_tdtest.name}'")


@tabsdata_instance(consumer="tdtest")
def test_fixture_layered_on_instance(
    apiserver_url: str, instance_tdtest: Instance
) -> None:
    server = instance_tdtest.apiserver
    assert apiserver_url == f"http://{server.host}:{server.port}"


@tabsdata_instance(consumer="tdtest")
def test_yield_fixture_layered_on_instance(
    workspace: Path, instance_tdtest: Instance
) -> None:
    assert workspace.name == instance_tdtest.name
    assert workspace.is_dir()


@tabsdata_instance(consumer="tdtest")
def test_sugar_and_named_are_same(
    td_instance: Instance, instance_tdtest: Instance
) -> None:
    assert td_instance is instance_tdtest


@pytest.mark.skipif(
    "aitest" not in registered_consumers(),
    reason="Consumer 'aitest' has no registered instance",
)
@pytest.mark.parametrize("word", WORDS)
@tabsdata_instance(consumer="tdtest")
@tabsdata_instance(consumer="aitest")
def test_multiple_with_parametrized_argument(
    instance_tdtest: Instance,
    instance_aitest: Instance,
    word: str,
    tmp_path: Path,
) -> None:
    assert instance_tdtest is not instance_aitest
    record = tmp_path / f"{word}.txt"
    record.write_text(f"{instance_tdtest.name}\n{instance_aitest.name}\n")
    assert len(record.read_text().splitlines()) == 2


@pytest.mark.skipif(
    "aitest" not in registered_consumers(),
    reason="Consumer 'aitest' has no registered instance",
)
@tabsdata_instance(consumer="tdtest")
@tabsdata_instance(consumer="aitest")
def test_multiple_with_layered_fixture(
    apiserver_url: str,
    instance_tdtest: Instance,
    instance_aitest: Instance,
    greeting: str,
) -> None:
    assert greeting == "Hello"
    server = instance_tdtest.apiserver
    assert apiserver_url == f"http://{server.host}:{server.port}"
    assert instance_aitest.name != instance_tdtest.name
