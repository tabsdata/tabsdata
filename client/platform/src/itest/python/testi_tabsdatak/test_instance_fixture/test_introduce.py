#
#  Copyright 2026 Tabs Data Inc.
#

import pytest
from tabsdatax.itest.fixture.instances.tabsdata import (
    Instance,
    make_instance,
    registered_consumers,
    tabsdata_instance,
    verify_serving,
)


@tabsdata_instance(consumer="tdtest")
def test_introduce_declared(instance_tdtest: Instance) -> None:
    assert instance_tdtest.name in instance_tdtest.introduce()


@tabsdata_instance()
def test_introduce_default(instance_tdtest: Instance) -> None:
    assert instance_tdtest.name in instance_tdtest.introduce()


@tabsdata_instance(consumer="tdtest", check=False)
def test_introduce_unchecked(instance_tdtest: Instance) -> None:
    assert instance_tdtest.name in instance_tdtest.introduce()


@tabsdata_instance(consumer="tdtest")
def test_introduce_named(instance_tdtest: Instance) -> None:
    assert instance_tdtest.name in instance_tdtest.introduce()


@tabsdata_instance(consumer="tdtest")
def test_introduce_via_fixture(td_tabsdata_instance: Instance) -> None:
    assert td_tabsdata_instance.name in td_tabsdata_instance.introduce()


@pytest.mark.skipif(
    "aitest" not in registered_consumers(),
    reason="Consumer 'aitest' has no registered instance",
)
@tabsdata_instance(consumer="tdtest")
@tabsdata_instance(consumer="aitest")
def test_introduce_two_at_once(
    instance_tdtest: Instance, instance_aitest: Instance
) -> None:
    assert instance_tdtest.name in instance_tdtest.introduce()
    assert instance_aitest.name in instance_aitest.introduce()


def pytest_generate_tests(metafunc) -> None:
    if "consumer" in metafunc.fixturenames:
        consumers = registered_consumers()
        metafunc.parametrize("consumer", consumers, ids=consumers)


def test_introduce_all_registered(consumer: str) -> None:
    instance = make_instance(consumer)
    verify_serving(instance)
    assert instance.name in instance.introduce()
