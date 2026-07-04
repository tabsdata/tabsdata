#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import os
from pathlib import Path

import pytest

from testi_tabsdatak.common.backend.tdkserver import (
    APISERVER_SUPERUSER_NAME,
    APISERVER_SUPERUSER_PASSWORD,
    ENV_ROOT_PREFIX,
    LOCALHOST,
    AssignedPort,
    Instance,
    InstanceEnv,
    env_key,
    export_instance_envs,
    role_of,
)

ROLE = "pytest"
APISERVER_PORT = 2457
FOLDER = Path("/tmp") / ROLE


def sample_instance() -> Instance:
    return Instance(
        name=ROLE,
        folder=FOLDER,
        settings={"state": "created"},
        ports=[
            AssignedPort(
                service="apiserver_public_port",
                caption="apiserver public port",
                port=APISERVER_PORT,
            )
        ],
    )


def test_instance_envs_exist(monkeypatch: pytest.MonkeyPatch) -> None:
    instance = sample_instance()
    role = role_of(instance.name)
    export_instance_envs(instance, monkeypatch)

    for env in InstanceEnv:
        assert env.get(role) is not None, f"expected env '{env.key(role)}' to be set"

    assert InstanceEnv.NAME.get(role) == ROLE
    assert InstanceEnv.FOLDER.get(role) == str(FOLDER)
    assert InstanceEnv.APISERVER_HOST.get(role) == LOCALHOST
    assert InstanceEnv.APISERVER_PORT.get(role) == str(APISERVER_PORT)
    assert InstanceEnv.APISERVER_SUPERUSER_USER.get(role) == APISERVER_SUPERUSER_NAME
    assert (
        InstanceEnv.APISERVER_SUPERUSER_PASSWORD.get(role)
        == APISERVER_SUPERUSER_PASSWORD
    )

    prefix = env_key(ENV_ROOT_PREFIX, role)
    for excluded in (f"{prefix}_SETTINGS_STATE", f"{prefix}_PORTS_0_SERVICE"):
        assert excluded not in os.environ, f"env '{excluded}' should not be exported"
