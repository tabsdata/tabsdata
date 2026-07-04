#
# Copyright 2026 Tabs Data Inc.
#

import requests

from testi_tabsdatak.common.backend import (
    Instance,
    InstanceEnv,
    InstanceMaker,
    registered_roles,
    role_of,
    with_instance,
)

PING_PATH = "/api/v1/ping"
PING_TIMEOUT_SECONDS = 10
LOCALHOST = "127.0.0.1"


def verify_envs(instance: Instance) -> None:
    role = role_of(instance.name)
    for env in InstanceEnv:
        assert env.get(role) is not None, (
            f"Expected env '{env.key(role)}' to be set for instance '{instance.name}'."
        )


def ping(instance: Instance) -> None:
    verify_envs(instance)
    url = f"http://{LOCALHOST}:{instance.apiserver_public_port}{PING_PATH}"
    response = requests.get(url, timeout=PING_TIMEOUT_SECONDS)
    assert response.status_code == 200, (
        f"Ping to {url} returned {response.status_code}, expected 200."
    )


@with_instance(role="pytest")
def test_ping_one_via_decorator(instance: Instance) -> None:
    ping(instance)


@with_instance()
def test_ping_one_via_decorator_default(instance: Instance) -> None:
    ping(instance)


def test_ping_one_via_fixture(instance_maker: InstanceMaker) -> None:
    ping(instance_maker("pytest"))


def test_ping_one_via_fixture_unchecked(instance_maker: InstanceMaker) -> None:
    ping(instance_maker("pytest", check=False))


def pytest_generate_tests(metafunc) -> None:
    if "role" in metafunc.fixturenames:
        roles = registered_roles()
        metafunc.parametrize("role", roles, ids=roles)


def test_ping_all_via_fixture(instance_maker: InstanceMaker, role: str) -> None:
    ping(instance_maker(role))
