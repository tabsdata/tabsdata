#
# Copyright 2026 Tabsdata Inc.
#

"""Shared lifecycle for a *multi-container* system — a docker-compose stack.

Where `ContainerBase` starts one `docker run` container with a single
HOST/PORT/USER/PASSWORD endpoint, a `containers` system is a compose stack of
several cooperating containers (e.g. Druid: zookeeper + coordinator / broker /
historical / middleManager / router) reached through one or more HTTP
endpoints. This base drives the compose lifecycle (up / down / stop / start,
addressed by an explicit project name so concurrent stacks stay isolated) on
the unified `plan_system` / `provision_system` contract: `plan_system` picks the
stack's host ports up front and derives its endpoint URLs (no side effects) so
the FULL descriptor — endpoints included — is written before the stack is
started, and `provision_system` brings the stack up bound to those ports and
waits for it to become ready. Each `systems/<slug>/` subclass supplies the
compose file, the image list, and the endpoint / readiness hooks.
"""

from __future__ import annotations

from ..errors import DatagenError
from ..shell import (
    compose_container_ids,
    docker_compose,
    require_docker,
)
from .base import (
    CreateSystemSpec,
    Health,
    Setting,
    System,
    SystemContext,
)

# Compose project names must be lowercase; prefix tdx-managed stacks so they're
# recognisable and their `<project>_<service>` containers / volumes don't
# collide with unrelated compose projects.
PROJECT_NAME_PREFIX = "tdx_"


def project_name(sys_name: str) -> str:
    """The compose project for a system named `sys_name`: the name prefixed with
    ``tdx_`` and lower-cased (compose rejects uppercase project names)."""
    return f"{PROJECT_NAME_PREFIX}{sys_name}".lower()


class ContainersBase(System):
    category = "containers"
    supports_pause = True

    # --- compose config (subclass attributes) ---
    image = ""  # primary service image:tag (recorded + exported as IMAGE)
    image_platform = ""  # informational, for `types` display
    compose_images: tuple[tuple[str, str], ...] = ()  # every (image, platform)
    compose_file = ""  # absolute path to the stack's compose yaml

    # Exported connection keys (endpoints) the subclass bakes; the rest of what
    # plan_system / plan_domain returns is stored as unexported back-refs.
    system_endpoints: tuple[str, ...] = ()
    domain_endpoints: tuple[str, ...] = ()

    options = ()  # ports are planned, not declared; no connection options

    def docker_images(self) -> list[tuple[str, str]]:
        return list(self.compose_images)

    def add_create_options(self, parser) -> None:
        super().add_create_options(parser)
        parser.add_argument(
            "--ready-timeout",
            type=int,
            default=300,
            help="optional; seconds to wait for the stack to become ready "
            "(default: 300)",
        )

    # ==================================================================
    # System lifecycle (compose) — plan / provision split
    # ==================================================================

    def plan_system(self, spec: CreateSystemSpec) -> dict[str, Setting]:
        require_docker()
        project = project_name(spec.name)
        # Pick the stack's host ports and derive its endpoint URLs now, so the
        # full descriptor (endpoints included) is written before `up`; the same
        # ports are reused if provisioning is retried.
        endpoints = self._plan_endpoints()
        out: dict[str, Setting] = {
            "PROJECT": Setting("PROJECT", value=project),
            "IMAGE": Setting("IMAGE", value=self.image),
        }
        for k, v in endpoints.items():
            out[k] = Setting(k, value=v)
        return out

    def provision_system(
        self, spec: CreateSystemSpec, settings: dict[str, Setting]
    ) -> None:
        require_docker()
        project = settings["PROJECT"].value or ""
        if compose_container_ids(project, self.compose_file):
            if spec.force:
                self._compose_down(project)
                print(f">>> --force: removed existing stack {project}")
            else:
                raise DatagenError(
                    f"compose stack already exists: {project} (use --force)"
                )
        env = self._compose_env(settings)
        print(f">>> pulling docker images for {self.label} …", flush=True)
        self._compose_pull(project, env)
        print(f">>> starting {self.label} stack {project}")
        self._compose_up(project, env)
        # The stack is up; any failure (readiness timeout, Ctrl-C) must not leave
        # its containers running.
        try:
            print(
                f">>> waiting for {self.label} to become ready "
                f"(timeout: {spec.ready_timeout}s)…"
            )
            self._wait_ready(settings, spec.ready_timeout)
        except BaseException:
            print(f">>> provision failed — removing stack {project}")
            try:
                self._compose_down(project)
            except Exception:
                pass
            raise

    def destroy_system(self, ctx: SystemContext) -> None:
        self._compose_down(ctx.meta["PROJECT"])

    def system_status(self, ctx: SystemContext) -> Health:
        return self._system_health(ctx.meta)

    def pause(self, ctx: SystemContext) -> str:
        docker_compose(ctx.meta["PROJECT"], self.compose_file, "stop")
        return "stopped"

    def resume(self, ctx: SystemContext) -> str:
        docker_compose(ctx.meta["PROJECT"], self.compose_file, "start")
        return "started"

    def restart(self, ctx: SystemContext) -> str:
        docker_compose(ctx.meta["PROJECT"], self.compose_file, "restart")
        return "restarted"

    # ==================================================================
    # Compose primitives (overridable for testing)
    # ==================================================================

    def _compose_pull(self, project: str, env: dict[str, str] | None = None) -> None:
        docker_compose(project, self.compose_file, "pull", env=env)

    def _compose_up(self, project: str, env: dict[str, str] | None = None) -> None:
        docker_compose(project, self.compose_file, "up", "-d", env=env)

    def _compose_down(self, project: str) -> None:
        docker_compose(project, self.compose_file, "down", "-v")

    # ==================================================================
    # Subclass hooks
    # ==================================================================

    def _plan_endpoints(self) -> dict[str, str]:
        """Pick the stack's host ports and return its exported endpoint settings
        (keys in `system_endpoints`, e.g. Druid's OVERLORD_URL / ROUTER_URL),
        with NO external side effects — called during `plan_system`."""
        raise NotImplementedError

    def _compose_env(self, settings: dict[str, Setting]) -> dict[str, str]:
        """Environment variables the compose file interpolates when the stack is
        brought up — typically the planned host ports the services bind, derived
        from the endpoint settings. Default: none."""
        return {}

    def _wait_ready(self, settings: dict[str, Setting], timeout: int) -> None:
        """Block until the started stack is ready (e.g. poll each endpoint's
        status URL), raising on timeout. Default: no wait."""
        return None

    def _system_health(self, meta: dict[str, str]) -> Health:
        """Reachability of the stack. Default: whether its containers exist;
        subclasses that expose a status endpoint override with an HTTP probe."""
        project = meta.get("PROJECT", "")
        running = bool(compose_container_ids(project, self.compose_file))
        return Health(
            running,
            f"{self.label} stack {project} {'running' if running else 'not running'}",
        )
