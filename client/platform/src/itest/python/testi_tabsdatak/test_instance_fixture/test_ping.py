#
#  Copyright 2026 Tabs Data Inc.
#

import pytest
from tabsdatax.itest.fixture.instances.tabsdata import (
    Instance,
    consumer_of,
    make_instance,
    registered_consumers,
    tabsdata_instance,
    verify_serving,
)


@tabsdata_instance(consumer="tdtest")
def test_ping_declared(instance_tdtest: Instance) -> None:
    instance_tdtest.ping()
    assert consumer_of(instance_tdtest.name) == "tdtest"


@tabsdata_instance()
def test_ping_default(instance_tdtest: Instance) -> None:
    instance_tdtest.ping()
    assert consumer_of(instance_tdtest.name) == "tdtest"


@tabsdata_instance(consumer="tdtest", check=False)
def test_ping_unchecked(instance_tdtest: Instance) -> None:
    instance_tdtest.ping()
    assert consumer_of(instance_tdtest.name) == "tdtest"


@tabsdata_instance(consumer="tdtest")
def test_ping_named(instance_tdtest: Instance) -> None:
    instance_tdtest.ping()
    assert consumer_of(instance_tdtest.name) == "tdtest"


@tabsdata_instance(consumer="tdtest")
def test_ping_via_fixture(td_tabsdata_instance: Instance) -> None:
    td_tabsdata_instance.ping()
    assert consumer_of(td_tabsdata_instance.name) == "tdtest"


@pytest.mark.skipif(
    "aitest" not in registered_consumers(),
    reason="consumer 'aitest' has no registered instance",
)
@tabsdata_instance(consumer="tdtest")
@tabsdata_instance(consumer="aitest")
def test_ping_two_at_once(instance_tdtest: Instance, instance_aitest: Instance) -> None:
    instance_tdtest.ping()
    instance_aitest.ping()
    assert consumer_of(instance_tdtest.name) == "tdtest"
    assert consumer_of(instance_aitest.name) == "aitest"


def pytest_generate_tests(metafunc) -> None:
    if "consumer" in metafunc.fixturenames:
        consumers = registered_consumers()
        metafunc.parametrize("consumer", consumers, ids=consumers)


def test_ping_all_registered(consumer: str) -> None:
    instance = make_instance(consumer)
    verify_serving(instance)
    instance.ping()
    assert consumer_of(instance.name) == consumer
