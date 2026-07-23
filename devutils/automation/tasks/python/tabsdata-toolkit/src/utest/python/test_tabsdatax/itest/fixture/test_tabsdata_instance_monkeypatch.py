#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest

# `td_tabsdata_instance` (a consumer shorthand) and `meta_instance` (registered
# as the `instance` meta-relay) normally generate at plugin import from the itest
# registry, which does not exist in-process; import them so the tests exercising
# the shorthand and the relay can request them.
from tabsdatax.itest.fixture.instances import tabsdata
from tabsdatax.itest.fixture.instances.tabsdata import (
    Instance,
    Port,
    consumer_instance_fixture,
    tabsdata_instance,
    td_tabsdata_instance,
)
from tabsdatax.itest.fixture.instances.tabsdata import meta_instance as instance

MONKEYPATCH_CONSUMERS = ("tdtest", "aitest")

for monkeypatch_consumer in MONKEYPATCH_CONSUMERS:
    globals()[f"instance_{monkeypatch_consumer}"] = consumer_instance_fixture(
        monkeypatch_consumer
    )


def fake_instance(consumer: str) -> Instance:
    return Instance(
        name=f"{consumer}-monkeypatch",
        folder=Path(f"/tmp/{consumer}-monkeypatch"),
        settings={},
        ports=[Port(service="apiserver_public_port", caption="apiserver", port=0)],
    )


@pytest.fixture(autouse=True)
def offline_instances(monkeypatch):
    monkeypatch.setattr(
        tabsdata, "loaded_instance", lambda consumer, check: fake_instance(consumer)
    )


@tabsdata_instance("tdtest")
def test_single_instance(instance_tdtest: Instance) -> None:
    assert instance_tdtest.name == "tdtest-monkeypatch"


@tabsdata_instance("tdtest")
def test_single_instance_via_shorthand(td_tabsdata_instance: Instance) -> None:
    assert td_tabsdata_instance.name == "tdtest-monkeypatch"


@tabsdata_instance("tdtest", check=False)
def test_single_instance_unchecked(instance_tdtest: Instance) -> None:
    assert instance_tdtest.name == "tdtest-monkeypatch"


@tabsdata_instance("tdtest")
def test_instance_declared_but_unused_in_body(instance_tdtest: Instance) -> None:
    result = 1 + 1
    assert result == 2


@tabsdata_instance("tdtest")
@tabsdata_instance("aitest")
def test_multiple_instances(
    instance_tdtest: Instance, instance_aitest: Instance
) -> None:
    assert instance_tdtest.name != instance_aitest.name


@pytest.fixture
def workspace(instance_tdtest: Instance, tmp_path: Path) -> Path:
    folder = tmp_path / instance_tdtest.name
    folder.mkdir()
    return folder


@tabsdata_instance("tdtest")
def test_helper_forwards_but_test_still_lists_instance(
    workspace: Path, instance_tdtest: Instance
) -> None:
    assert workspace.name == instance_tdtest.name


@pytest.fixture
def relayed_name(instance: Instance) -> str:
    return instance.name


@tabsdata_instance("aitest")
def test_meta_instance_relay(instance_aitest: Instance, relayed_name: str) -> None:
    assert relayed_name == instance_aitest.name


@pytest.fixture
def make_label():
    def build(instance: Instance) -> str:
        return f"label::{instance.name}"

    return build


@tabsdata_instance("tdtest")
@tabsdata_instance("aitest")
def test_factory_helper_with_multiple_instances(
    instance_tdtest: Instance, instance_aitest: Instance, make_label
) -> None:
    labels = {make_label(instance_tdtest), make_label(instance_aitest)}
    assert labels == {"label::tdtest-monkeypatch", "label::aitest-monkeypatch"}
