#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import json
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

import pytest
import yaml

Role = Literal["pytest", "aitest", "uitest"]
Service = Literal[
    "apiserver_public_port",
    "apiserver_private_port",
    "aiagent_public_port",
    "database_port",
    "kminus_api_port",
]

TABSDATA_HOME_FOLDER = ".tabsdata"
INSTANCES_FOLDER = "instances"
SETTINGS_FILE = ".settings.json"
REGISTRY_KEY_INSTANCES = "instances"
LOCALHOST = "127.0.0.1"
PORT_PROBE_TIMEOUT_SECONDS = 2.0
DEFAULT_INSTANCE: Role = "pytest"
SERVICE_APISERVER_PUBLIC_PORT: Service = "apiserver_public_port"
SERVICE_APISERVER_PRIVATE_PORT: Service = "apiserver_private_port"
SERVICE_AIAGENT_PUBLIC_PORT: Service = "aiagent_public_port"
SERVICE_DATABASE_PORT: Service = "database_port"
SERVICE_KMINUS_API_PORT: Service = "kminus_api_port"


@dataclass(frozen=True)
class AssignedPort:
    service: str
    caption: str
    port: int


@dataclass(frozen=True)
class Instance:
    name: str
    folder: Path
    settings: dict
    ports: list[AssignedPort]

    def port_for_service(self, service: Service) -> int | None:
        for assigned in self.ports:
            if assigned.service == service:
                return assigned.port
        return None

    @property
    def apiserver_public_port(self) -> int | None:
        return self.port_for_service(SERVICE_APISERVER_PUBLIC_PORT)

    @property
    def apiserver_private_port(self) -> int | None:
        return self.port_for_service(SERVICE_APISERVER_PRIVATE_PORT)

    @property
    def aiagent_public_port(self) -> int | None:
        return self.port_for_service(SERVICE_AIAGENT_PUBLIC_PORT)

    @property
    def database_port(self) -> int | None:
        return self.port_for_service(SERVICE_DATABASE_PORT)

    @property
    def kminus_api_port(self) -> int | None:
        return self.port_for_service(SERVICE_KMINUS_API_PORT)


def project_root() -> Path:
    current = Path(__file__).absolute()
    for parent in current.parents:
        if (parent / ".root").exists() or (parent / ".git").exists():
            return parent
    raise RuntimeError(
        f"Could not locate the project root (.root or .git) above {current}."
    )


def registry_file() -> Path:
    return project_root() / "target" / "tdkpytest.yaml"


def registered_instance_names() -> list[str]:
    file = registry_file()
    if not file.exists():
        return []
    with open(file, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        return []
    names = data.get(REGISTRY_KEY_INSTANCES)
    if not isinstance(names, list):
        return []
    return [str(name) for name in names]


def instance_folder(name: str) -> Path:
    return Path.home() / TABSDATA_HOME_FOLDER / INSTANCES_FOLDER / name


def role_of(name: str) -> str:
    return name.split("-", 1)[0]


def registered_roles() -> list[str]:
    return sorted({role_of(name) for name in registered_instance_names()})


def name_for_role(role: str) -> str:
    matches = [name for name in registered_instance_names() if role_of(name) == role]
    if not matches:
        pytest.fail(
            f"No registered instance with role '{role}' "
            f"in '{registry_file()}'. "
            f"Registered roles: {registered_roles() or 'none'}.",
            pytrace=False,
        )
    return matches[-1]


def port_listening(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(PORT_PROBE_TIMEOUT_SECONDS)
        return probe.connect_ex((LOCALHOST, port)) == 0


def load_instance(name: str) -> Instance:
    file = registry_file()
    names = registered_instance_names()
    if name not in names:
        pytest.fail(
            f"Instance '{name}' "
            f"is not recorded in the itest registry '{file}'. "
            f"Recorded instances: {names or 'none'}.",
            pytrace=False,
        )

    folder = instance_folder(name)
    if not folder.exists():
        pytest.fail(
            f"Instance '{name}' folder '{folder}' does not exist.",
            pytrace=False,
        )

    settings_file = folder / SETTINGS_FILE
    if not settings_file.exists():
        pytest.fail(
            f"Instance '{name}' settings file '{settings_file}' does not exist.",
            pytrace=False,
        )

    with open(settings_file, "r", encoding="utf-8") as handle:
        settings = json.load(handle)

    ports = [
        AssignedPort(
            service=entry.get("role", ""),
            caption=entry.get("caption", ""),
            port=int(entry["port"]),
        )
        for entry in settings.get("ports", [])
    ]
    return Instance(name=name, folder=folder, settings=settings, ports=ports)


def make_instance(role: str) -> Instance:
    return load_instance(name_for_role(role))


def verify_serving(instance: Instance) -> None:
    for assigned_port in instance.ports:
        if not port_listening(assigned_port.port):
            pytest.fail(
                f"Instance '{instance.name}' "
                f"is not serving: its {assigned_port.caption} "
                f"(service '{assigned_port.service}') "
                f"on port {assigned_port.port} is not open.",
                pytrace=False,
            )


class InstanceMaker(Protocol):
    def __call__(self, role: str, check: bool = True) -> Instance: ...


@pytest.fixture
def instance_maker() -> InstanceMaker:
    def make(role: str, check: bool = True) -> Instance:
        instance = make_instance(role)
        if check:
            verify_serving(instance)
        return instance

    return make


@pytest.fixture
def instance(request) -> Instance:
    instance = make_instance(request.param)
    verify_serving(instance)
    return instance


def with_instance(role: Role = DEFAULT_INSTANCE):
    return pytest.mark.parametrize("instance", [role], indirect=True)
