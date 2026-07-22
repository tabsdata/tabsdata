#
#  Copyright 2026 Tabsdata Inc.
#


import pytest
from tabsdatax.itest.fixture.instances.tabsdata import (
    Instance,
    consumer_of,
    registered_consumers,
    tabsdata_instance,
)


@tabsdata_instance("tdtest")
def test_single_instance(instance_tdtest: Instance) -> None:
    instance_tdtest.ping()
    assert consumer_of(instance_tdtest.name) == "tdtest"


@tabsdata_instance("tdtest")
def test_single_instance_via_shorthand(td_tabsdata_instance: Instance) -> None:
    td_tabsdata_instance.ping()
    assert consumer_of(td_tabsdata_instance.name) == "tdtest"


@tabsdata_instance("tdtest", check=False)
def test_single_instance_unchecked(instance_tdtest: Instance) -> None:
    assert consumer_of(instance_tdtest.name) == "tdtest"


@tabsdata_instance("tdtest")
def test_instance_declared_but_unused_in_body(instance_tdtest: Instance) -> None:
    outcome = do_offline_work()
    assert outcome == "done"


def do_offline_work() -> str:
    return "done"


@pytest.fixture
def admin_url(instance_tdtest: Instance) -> str:
    server = instance_tdtest.apiserver
    return f"http://{server.host}:{server.port}"


@tabsdata_instance("tdtest")
def test_helper_forwards_but_test_still_lists_instance(
    admin_url: str, instance_tdtest: Instance
) -> None:
    server = instance_tdtest.apiserver
    assert admin_url == f"http://{server.host}:{server.port}"


@pytest.fixture
def relayed_instance(instance: Instance) -> Instance:
    return instance


@tabsdata_instance("tdtest")
def test_meta_instance_relay(
    instance_tdtest: Instance, relayed_instance: Instance
) -> None:
    assert relayed_instance is instance_tdtest


@pytest.fixture
def make_admin_url():
    def build(instance: Instance) -> str:
        server = instance.apiserver
        return f"http://{server.host}:{server.port}"

    return build


@pytest.mark.skipif(
    "aitest" not in registered_consumers(),
    reason="Consumer 'aitest' has no registered instance",
)
@tabsdata_instance("tdtest")
@tabsdata_instance("aitest")
def test_multiple_instances(
    instance_tdtest: Instance, instance_aitest: Instance
) -> None:
    instance_tdtest.ping()
    instance_aitest.ping()
    assert consumer_of(instance_tdtest.name) == "tdtest"
    assert consumer_of(instance_aitest.name) == "aitest"
    assert instance_tdtest.name != instance_aitest.name


@pytest.mark.skipif(
    "aitest" not in registered_consumers(),
    reason="Consumer 'aitest' has no registered instance",
)
@tabsdata_instance("tdtest")
@tabsdata_instance("aitest")
def test_factory_helper_with_multiple_instances(
    instance_tdtest: Instance, instance_aitest: Instance, make_admin_url
) -> None:
    urls = {make_admin_url(instance_tdtest), make_admin_url(instance_aitest)}
    assert len(urls) == 2


@pytest.mark.skipif(
    "uitest" not in registered_consumers(),
    reason="Consumer 'uitest' has no registered instance",
)
@tabsdata_instance("uitest")
def test_single_ui_instance(instance_uitest: Instance) -> None:
    instance_uitest.ping()
    assert consumer_of(instance_uitest.name) == "uitest"
