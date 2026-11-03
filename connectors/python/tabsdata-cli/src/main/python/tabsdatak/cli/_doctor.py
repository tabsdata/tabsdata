#
# Copyright 2026 Tabsdata Inc.
#

"""The ``tdk doctor`` checks. Each returns ``{name, status, detail, fix}``
with status ``pass``, ``warn``, ``fail`` or ``skip``; a check that raises is
reported as ``fail`` and never stops the others."""

from __future__ import annotations

import shutil
import sys
import time
from collections.abc import Callable

PASS, WARN, FAIL, SKIP = "pass", "warn", "fail", "skip"
EXPIRY_WARNING_SECONDS = 10 * 60
MAX_BUNDLED_EXTRAS = 25
LOGIN_FIX = "Run 'tdk login', or set TDK_SERVER together with TDK_TOKEN or TDK_BEARER."
VERSION_FIX = "Install the tabsdata release that matches the server."


def _result(name: str, status: str, detail: str, fix: str | None = None) -> dict:
    return {"name": name, "status": status, "detail": detail, "fix": fix}


def _local_python() -> str:
    return f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"


def _client_version() -> str | None:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("tabsdata-cli")
    except PackageNotFoundError:
        return None


def _classify(error: Exception):
    from tabsdatak.cli._errors import classify

    return classify(error)


def _describe(error: Exception) -> str:
    return _classify(error).message


class Probe:
    """What the checks share: the server connection (``None`` when offline or
    logged out) and its runtime info, fetched at most once."""

    def __init__(self, server, *, offline: bool):
        self.offline = offline
        self.server = None if offline else server
        self._runtime: dict | None = None
        self._runtime_error: Exception | None = None

    @property
    def unavailable(self) -> str | None:
        if self.offline:
            return "skipped with --offline"
        if not self.server:
            return "not logged in"
        return None

    def runtime(self) -> dict:
        if self._runtime_error is not None:
            raise self._runtime_error
        if self._runtime is None:
            try:
                response = self.server.connection.runtime_info_get()
                self._runtime = response.json().get("data") or {}
            except Exception as error:
                self._runtime_error = error
                raise
        return self._runtime


def check_python(probe: Probe) -> dict:
    local = _local_python()
    reason = probe.unavailable
    if reason is None:
        try:
            allowed = [str(v) for v in probe.runtime().get("python_versions") or []]
        except Exception as error:
            reason = f"server unavailable: {_describe(error)}"
        else:
            if not allowed:
                return _result(
                    "python",
                    WARN,
                    f"local {local}; the server reports no Python versions",
                )
            if local in allowed:
                return _result(
                    "python", PASS, f"local {local}, server accepts {allowed}"
                )
            target = allowed[0]
            return _result(
                "python",
                FAIL,
                f"local {local}, server accepts {allowed}",
                f"uv python install {target} && uv venv --python {target}",
            )
    from tabsdatak.common._compatibility import python_version_supported

    if not python_version_supported(sys.version_info[:3]):
        return _result(
            "python",
            FAIL,
            f"local {local} is not supported by this tabsdata release",
            "Install a supported Python and recreate the virtual environment.",
        )
    return _result(
        "python",
        WARN,
        f"local {local}; not compared with the server ({reason})",
        LOGIN_FIX if reason == "not logged in" else None,
    )


def check_server(probe: Probe) -> dict:
    if probe.unavailable:
        return _result(
            "server", SKIP, probe.unavailable, None if probe.offline else LOGIN_FIX
        )
    try:
        status = probe.server.status
        server_version = probe.runtime().get("version")
    except Exception as error:
        if _classify(error).type == "version_mismatch":
            from tabsdatak.cli.cli_utils import server_version as read_version

            return _result(
                "server",
                FAIL,
                f"server {read_version(probe.server)}, client {_client_version()}: "
                "the server refuses this client's version",
                VERSION_FIX,
            )
        return _result(
            "server",
            FAIL,
            f"unreachable: {_describe(error)}",
            "Start the server ('tdkserver quickstart' locally) or fix the URL "
            "with 'tdk login'.",
        )
    detail = f"status {status.status}, server {server_version}"
    client = _client_version()
    if client and server_version and str(server_version) != client:
        return _result(
            "server",
            WARN,
            f"{detail}, client {client}: versions differ",
            VERSION_FIX,
        )
    return _result("server", PASS, f"{detail}, client {client}")


def check_login(probe: Probe) -> dict:
    if probe.offline:
        return _result("login", SKIP, "skipped with --offline")
    if not probe.server:
        return _result("login", FAIL, "no credentials", LOGIN_FIX)
    try:
        connection = probe.server.connection
        expires_at = connection.expiration_time
        renewable = bool(connection.refresh_token)
        info = probe.server.auth_info()
    except Exception as error:
        refused = _classify(error).type == "version_mismatch"
        return _result(
            "login", FAIL, _describe(error), VERSION_FIX if refused else LOGIN_FIX
        )
    detail = f"logged in as {info.get('name')}"
    if connection.project:
        detail += f", project {connection.project}"
    if expires_at and not renewable:
        remaining = expires_at - time.time()
        if remaining < EXPIRY_WARNING_SECONDS:
            return _result(
                "login",
                WARN,
                f"{detail}; the session expires in {max(int(remaining), 0)}s",
                LOGIN_FIX,
            )
    return _result("login", PASS, detail)


def bundle_analysis() -> tuple[list[str], list[str]]:
    """What ``PythonEnvironment.synthesise()`` would do with this venv,
    without resolving: the registry packages it snapshots into bundles, and
    the installed packages it skips because they come from no registry."""
    from tabsdatak.common._interpreter.python.package import Package
    from tabsdatak.common._interpreter.python.recipe import InstallRecipe
    from tabsdatak.common._interpreter.python.venv import (
        TABSDATA_PACKAGE,
        PythonEnvironment,
    )

    locked = PythonEnvironment.locked_packages()
    extras, skipped = [], []
    for name in sorted(PythonEnvironment.installed_packages()):
        if name.startswith(TABSDATA_PACKAGE) or name in locked:
            continue
        recipe = Package(name).recipe()
        if recipe is None:
            continue
        if recipe.recipe == InstallRecipe.INDEX and recipe.version is not None:
            extras.append(f"{name}=={recipe.version}")
        else:
            skipped.append(f"{name} ({recipe.recipe.value.lower()})")
    return extras, skipped


def check_venv(probe: Probe) -> dict:
    if sys.prefix == sys.base_prefix:
        return _result(
            "venv",
            FAIL,
            f"not in a virtual environment ({sys.executable})",
            "Create and activate one: uv venv && source .venv/bin/activate",
        )
    extras, skipped = bundle_analysis()
    if skipped:
        return _result(
            "venv",
            WARN,
            f"{len(skipped)} non-registry packages will not be bundled: "
            f"{', '.join(skipped)}",
            "Pass --dir-to-bundle or install from an index.",
        )
    if len(extras) > MAX_BUNDLED_EXTRAS:
        return _result(
            "venv",
            WARN,
            f"{len(extras)} extra registry packages will be bundled into every "
            "function",
            "Register from a leaner virtual environment.",
        )
    detail = f"{sys.prefix}; {len(extras)} extra registry packages will be bundled"
    return _result("venv", PASS, detail + (f": {', '.join(extras)}" if extras else ""))


def check_uv(probe: Probe) -> dict:
    path = shutil.which("uv")
    if path is None:
        return _result(
            "uv",
            FAIL,
            "uv is not on PATH; function registration resolves bundle "
            "requirements with it",
            "Install uv: https://docs.astral.sh/uv/getting-started/installation/",
        )
    return _result("uv", PASS, path)


def _installed_connection_types() -> set[str]:
    from tabsdatak._connectors import avail_dests, avail_srcs, avail_stream_srcs

    return {*avail_srcs(), *avail_stream_srcs(), *avail_dests()}


def check_connectors(probe: Probe) -> dict:
    if probe.unavailable:
        return _result("connectors", SKIP, probe.unavailable)
    try:
        project = probe.server.connection.project
        if not project:
            return _result(
                "connectors", SKIP, "no project pinned", "tdk set --proj NAME"
            )
        installed = _installed_connection_types()
        missing: dict[str, list[str]] = {}
        unreadable = []
        for collection in probe.server.list_collections_generator():
            data = collection._data or {}
            if data.get("collection_type") not in ("S", "D"):
                continue
            name = data.get("name")
            try:
                document = probe.server.get_collection_connection(name)
            except Exception:
                unreadable.append(name)
                continue
            type_ = (document.get("connection") or {}).get("type")
            if type_ and type_ not in installed:
                missing.setdefault(type_, []).append(name)
    except Exception as error:
        return _result("connectors", FAIL, _describe(error))
    notes = [
        f"{type_} (collections {', '.join(names)})"
        for type_, names in sorted(missing.items())
    ]
    if notes:
        return _result(
            "connectors",
            WARN,
            f"no local connector package for: {'; '.join(notes)}",
            "Install the connector packages; registering a function that uses "
            "them fails at import.",
        )
    if unreadable:
        return _result(
            "connectors",
            WARN,
            f"connections not readable: {', '.join(unreadable)}",
            "Reading a collection's connection needs ManageProject on the project.",
        )
    return _result(
        "connectors", PASS, f"every connection type in project {project} is installed"
    )


CHECKS: dict[str, Callable[[Probe], dict]] = {
    "python": check_python,
    "server": check_server,
    "login": check_login,
    "venv": check_venv,
    "uv": check_uv,
    "connectors": check_connectors,
}


def run_checks(probe: Probe, names: list[str]) -> list[dict]:
    results = []
    for name in names:
        try:
            results.append(CHECKS[name](probe))
        except Exception as error:
            results.append(_result(name, FAIL, f"check crashed: {_describe(error)}"))
    return results
