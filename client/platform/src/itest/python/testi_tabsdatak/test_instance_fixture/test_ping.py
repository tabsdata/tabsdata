#
#  Copyright 2026 Tabs Data Inc.
#

#
#

import pytest
from tabsdatax.itest.fixture.instances.tabsdata import (
    Instance,
    make_instance,
    registered_consumers,
    verify_serving,
    with_instance,
)


@with_instance(consumer="tdtest")
def test_ping_declared(instance: Instance) -> None:
    instance.ping()


@with_instance()
def test_ping_default(instance: Instance) -> None:
    instance.ping()


@with_instance(consumer="tdtest", check=False)
def test_ping_unchecked(instance: Instance) -> None:
    instance.ping()


@with_instance(consumer="tdtest")
def test_ping_named(instance_tdtest: Instance) -> None:
    instance_tdtest.ping()


@pytest.mark.skipif(
    "aitest" not in registered_consumers(),
    reason="consumer 'aitest' has no registered instance",
)
@with_instance(consumer="tdtest")
@with_instance(consumer="aitest")
def test_ping_two_at_once(instance_tdtest: Instance, instance_aitest: Instance) -> None:
    instance_tdtest.ping()
    instance_aitest.ping()


def pytest_generate_tests(metafunc) -> None:
    if "consumer" in metafunc.fixturenames:
        consumers = registered_consumers()
        metafunc.parametrize("consumer", consumers, ids=consumers)


def test_ping_all_registered(consumer: str) -> None:
    instance = make_instance(consumer)
    verify_serving(instance)
    instance.ping()
