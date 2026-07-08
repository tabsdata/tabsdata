#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import json
import os
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest
import yaml

from tabsdatax.itest.tdksystems.store import instances_dir

Consumer = Literal["tdtest", "aitest", "uitest"]
MARKER = "tabsdata_instance"

CONSUMER_TDTEST: Consumer = "tdtest"
CONSUMER_AITEST: Consumer = "aitest"
CONSUMER_UITEST: Consumer = "uitest"
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
PING_PATH = "/api/v1/ping"
PING_TIMEOUT_SECONDS = 10
DEFAULT_INSTANCE: Consumer = CONSUMER_TDTEST
SERVICE_APISERVER_PUBLIC_PORT: Service = "apiserver_public_port"
SERVICE_APISERVER_PRIVATE_PORT: Service = "apiserver_private_port"
SERVICE_AIAGENT_PUBLIC_PORT: Service = "aiagent_public_port"
SERVICE_DATABASE_PORT: Service = "database_port"
SERVICE_KMINUS_API_PORT: Service = "kminus_api_port"

APISERVER_SUPERUSER_NAME = "admin"
APISERVER_SUPERUSER_PASSWORD = "tabsdata"


@dataclass(frozen=True)
class Port:
    service: str
    caption: str
    port: int


@dataclass(frozen=True)
class User:
    user: str
    password: str


@dataclass(frozen=True)
class Server:
    host: str
    port: int | None
    superuser: User | None = None


@dataclass(frozen=True)
class Instance:
    name: str
    folder: Path
    settings: dict
    ports: list[Port]

    def port_for_service(self, service: Service) -> int | None:
        for assigned in self.ports:
            if assigned.service == service:
                return assigned.port
        return None

    @property
    def apiserver(self) -> Server:
        return Server(
            host=LOCALHOST,
            port=self.port_for_service(SERVICE_APISERVER_PUBLIC_PORT),
            superuser=User(
                user=APISERVER_SUPERUSER_NAME,
                password=APISERVER_SUPERUSER_PASSWORD,
            ),
        )

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

    def introduce(self) -> str:
        lines = [
            f"Instance '{self.name}' (consumer '{consumer_of(self.name)}')",
            f"  folder: {self.folder}",
            f"  apiserver: {self.apiserver.host}:{self.apiserver.port}",
        ]
        for assigned in self.ports:
            caption = assigned.caption or assigned.service
            lines.append(f"  {caption}: port {assigned.port}")
        text = "\n".join(lines)
        print(text)
        return text

    def ping(self) -> None:
        url = f"http://{self.apiserver.host}:{self.apiserver.port}{PING_PATH}"
        try:
            with urlopen(url, timeout=PING_TIMEOUT_SECONDS) as response:
                status = response.status
        except HTTPError as error:
            status = error.code
        except OSError as error:
            pytest.fail(f"Ping to {url} failed: {error}", pytrace=False)
        if status != 200:
            pytest.fail(
                f"Ping to {url} returned {status}, expected 200.",
                pytrace=False,
            )


REGISTRY_FILE = "tdkpytest.yaml"
REGISTRY_FILE_ENV = "ITEST_REGISTRY_FILE"


def registry_file() -> Path:
    override = os.environ.get(REGISTRY_FILE_ENV)
    if override:
        return Path(override)
    return instances_dir() / REGISTRY_FILE


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


def consumer_of(name: str) -> str:
    return name.split("-", 1)[0]


def registered_consumers() -> list[str]:
    return sorted({consumer_of(name) for name in registered_instance_names()})


def name_for_consumer(consumer: str) -> str:
    matches = [
        name for name in registered_instance_names() if consumer_of(name) == consumer
    ]
    if not matches:
        pytest.fail(
            f"No registered instance with consumer '{consumer}' "
            f"in '{registry_file()}'. "
            f"Registered consumers: {registered_consumers() or 'none'}.",
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
        Port(
            service=entry.get("role", ""),
            caption=entry.get("caption", ""),
            port=int(entry["port"]),
        )
        for entry in settings.get("ports", [])
    ]
    return Instance(name=name, folder=folder, settings=settings, ports=ports)


def make_instance(consumer: str) -> Instance:
    return load_instance(name_for_consumer(consumer))


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


def with_instance(consumer: Consumer = DEFAULT_INSTANCE, *, check: bool = True):
    return getattr(pytest.mark, MARKER)(consumer, check=check)


_CACHE: dict[str, Instance] = {}
_VERIFIED: set[str] = set()


def loaded_instance(consumer: str, check: bool = True) -> Instance:
    if consumer not in _CACHE:
        _CACHE[consumer] = make_instance(consumer)
    instance = _CACHE[consumer]
    if check and consumer not in _VERIFIED:
        verify_serving(instance)
        _VERIFIED.add(consumer)
    return instance


def declared_consumers(request) -> dict[str, bool]:
    declared: dict[str, bool] = {}
    for marker in request.node.iter_markers(MARKER):
        if marker.args:
            consumer = marker.args[0]
        else:
            consumer = marker.kwargs.get("consumer", DEFAULT_INSTANCE)
        declared.setdefault(consumer, marker.kwargs.get("check", True))
    return declared


@pytest.fixture
def instance(request) -> Instance:
    declared = declared_consumers(request)
    if not declared:
        pytest.fail(
            "No instance declared: decorate the test with @with_instance(...)",
            pytrace=False,
        )
    if len(declared) > 1:
        pytest.fail(
            f"Multiple instances declared ({', '.join(declared)}): "
            "use the `instance_<consumer>` fixtures instead",
            pytrace=False,
        )
    consumer, check = next(iter(declared.items()))
    return loaded_instance(consumer, check)


def consumer_instance_fixture(consumer: str):
    @pytest.fixture(name=f"instance_{consumer}")
    def fixture(request) -> Instance:
        declared = declared_consumers(request)
        if consumer not in declared:
            pytest.fail(
                f"Instance consumer {consumer!r} not declared: add "
                f"@with_instance(consumer={consumer!r}) to the test",
                pytrace=False,
            )
        return loaded_instance(consumer, declared[consumer])

    return fixture


for registered_consumer in registered_consumers():
    globals()[f"instance_{registered_consumer}"] = consumer_instance_fixture(
        registered_consumer
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        f"{MARKER}(consumer, check=True): the test uses the registered tabsdata"
        " instance of `consumer`; read it through the `instance` fixture (single"
        " declaration) or the `instance_<consumer>` fixtures (one per declared"
        " consumer).",
    )
