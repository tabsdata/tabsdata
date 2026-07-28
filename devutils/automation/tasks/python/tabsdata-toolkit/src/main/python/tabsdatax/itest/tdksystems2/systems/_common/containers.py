#
# Copyright 2026 Tabsdata Inc.
#

"""``ContainersBackend`` — a multi-container system driven by docker compose.

Where a single-container system uses ``docker run``, a compose stack (e.g. Druid:
zookeeper + coordinator/broker/historical/middleManager/router) is brought up
through ``docker compose`` against an explicit project name so concurrent stacks
stay isolated. ``plan_system`` picks the host ports up front (so the full
descriptor is written before ``up``) and ``provision_system`` brings the stack up
and waits for readiness, tearing it down on failure. Subclasses supply the
compose file + image list and the ``plan_endpoints`` / ``compose_env`` /
``wait_ready`` / ``system_health`` hooks (and their own domain plane).
"""

from __future__ import annotations

from tabsdatax.itest.tdksystems2 import _venv
from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.shell import (
    compose_container_ids,
    docker_compose,
    require_docker,
)
from tabsdatax.itest.tdksystems2.systems._common.ports import PortPoolMixin

_PROJECT_PREFIX = "tdx2_"


class ContainersBackend(PortPoolMixin):
    is_container: bool = True
    supports_purge: bool = True
    supports_models: bool = False
    supports_data: bool = False
    supports_changes: bool = False

    # A compose stack driven by docker, administered over HTTP.
    sys_env: str = _venv.ENV_C
    dom_env: str = _venv.ENV_C

    slug: str = ""
    label: str = ""
    kind: str = "containers"
    weight: int = 1
    domain_word: str = "domain"
    max_entity_name_length: int = 255
    ready_timeout: int = 300

    compose_file: str = ""
    compose_images: tuple[tuple[str, str], ...] = ()

    def docker_images(self) -> list[tuple[str, str]]:
        return list(self.compose_images)

    def options(self) -> tuple:
        return ()  # declarative create options; overridden by backends that have any

    def _project(self, name: str) -> str:
        return f"{_PROJECT_PREFIX}{name}".lower()  # compose rejects upper-case projects

    # ---- subclass hooks ------------------------------------------------

    def plan_endpoints(self) -> dict[str, str]:
        """Pick host ports / mint credentials and return the endpoint meta (no
        side effects — runs in plan_system)."""
        raise NotImplementedError

    def compose_env(self, meta: dict[str, str]) -> dict[str, str]:
        """Env vars the compose file interpolates (the planned ports, secrets)."""
        return {}

    def wait_ready(self, meta: dict[str, str], timeout: int) -> None:
        """Block until the stack is ready (poll a status endpoint), raise on
        timeout. Default: no wait."""

    def system_health(self, meta: dict[str, str]) -> tuple[bool, str]:
        project = meta.get("PROJECT", "")
        running = bool(compose_container_ids(project, self.compose_file))
        return (
            running,
            f"{self.label} stack {project} {'running' if running else 'down'}",
        )

    # ---- system lifecycle (compose) ------------------------------------

    def plan_system(
        self, name: str, overrides: dict[str, str] | None = None
    ) -> dict[str, str]:
        meta = {"PROJECT": self._project(name)}
        meta.update(self.plan_endpoints())
        return meta

    def provision_system(
        self, meta: dict[str, str], *, force: bool = False
    ) -> dict[str, str]:
        require_docker()
        project = meta["PROJECT"]
        if compose_container_ids(project, self.compose_file):
            if force:
                docker_compose(project, self.compose_file, "down", "-v")
                print(f">>> --force: removed existing stack {project}")
            else:
                raise TdxError(f"compose stack already exists: {project} (use --force)")
        env = self.compose_env(meta)
        print(f">>> pulling docker images for {self.label} …", flush=True)
        docker_compose(project, self.compose_file, "pull", env=env)
        print(f">>> starting {self.label} stack {project}")
        docker_compose(project, self.compose_file, "up", "-d", env=env)
        try:
            print(f">>> waiting for {self.label} to become ready …")
            self.wait_ready(meta, self.ready_timeout)
        except BaseException:
            print(f">>> provision failed — removing stack {project}")
            try:
                docker_compose(project, self.compose_file, "down", "-v")
            except Exception:  # noqa: BLE001 - teardown is best-effort
                pass
            raise
        return {}

    def destroy_system(self, meta: dict[str, str]) -> None:
        docker_compose(meta["PROJECT"], self.compose_file, "down", "-v")

    def system_status(self, meta: dict[str, str]) -> tuple[bool, str]:
        return self.system_health(meta)

    def restart_system(self, meta: dict[str, str]) -> str:
        docker_compose(meta["PROJECT"], self.compose_file, "restart")
        return "restarted"
