#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from tabsdatak.common._interpreter.python.venv import (
    BinaryNotFoundError,
    PythonEnvironment,
)
from tabsdatak.server._tdserver.render.assess import Check, Help, State
from tabsdatak.server._tdserver.render.assess.core.provider import Provider
from tabsdatak.server._tdserver.render.assess.core.vantage import Tier

KCHECK_PROGRAM = "tdk_kcheck"


class TaskError(Exception):
    pass


@dataclass(frozen=True)
class Target:
    instance: str
    provider: str | None = None
    context: str | None = None
    kubeconfig: str | None = None
    current: bool = False
    namespace: str | None = None
    given: frozenset[str] = field(default_factory=frozenset)

    def as_arguments(self) -> list[str]:
        arguments = ["--instance", self.instance]
        for name, value in (
            ("provider", self.provider),
            ("context", self.context),
            ("kubeconfig", self.kubeconfig),
            ("namespace", self.namespace),
        ):
            if value and name in self.given:
                arguments += [f"--{name}", value]
        if self.current:
            arguments.append("--current")
        return arguments


@dataclass(frozen=True)
class Task:
    concern: str
    check: str
    run: Callable[[Target], Check]
    needs: Sequence[str] = field(default_factory=tuple)
    tier: Tier = Tier.LOCAL
    providers: frozenset[Provider] = field(default_factory=lambda: frozenset(Provider))
    gate: bool = False
    asserts: str = ""
    detail: Callable[[], str] | str = ""

    @property
    def key(self) -> str:
        return f"{self.concern}/{self.check}"

    @property
    def particulars(self) -> str:
        return self.detail() if callable(self.detail) else self.detail

    def applies_to(self, provider: Provider | None) -> bool:
        return provider is None or provider in self.providers


def kcheck(name: str) -> Callable[[Target], Check]:
    def run(target: Target) -> Check:
        try:
            program = PythonEnvironment().binary(KCHECK_PROGRAM)
        except BinaryNotFoundError as error:
            raise TaskError(
                f"The {KCHECK_PROGRAM} program is not in the environment in use, "
                "so the checks that tdkserver owns cannot run"
            ) from error
        finished = subprocess.run(
            [str(program), name, *target.as_arguments()],
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
        )
        if finished.returncode != 0:
            raise TaskError(reason_of(finished.stderr))
        try:
            return Check.model_validate(json.loads(finished.stdout))
        except (json.JSONDecodeError, ValueError) as error:
            raise TaskError(
                f"The outcome of the {name} check could not be read: {error}"
            ) from error

    return run


def reason_of(stderr: str) -> str:
    for line in reversed(stderr.strip().splitlines()):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and payload.get("error"):
            return str(payload["error"])
    return stderr.strip() or "The check failed without saying why"


def failed(task: Task, reason: str) -> Check:
    return Check(
        concern=task.concern,
        check=task.check,
        state=State.FAILED,
        title="The check could not be completed",
        facts=[reason],
        help=[
            Help(
                text="This is a defect in assess itself, not in the instance, "
                "so report it"
            )
        ],
    )


def skipped(task: Task, blocked_by: Sequence[str]) -> Check:
    unmet = ", ".join(blocked_by)
    return Check(
        concern=task.concern,
        check=task.check,
        state=State.SKIPPED,
        reason=f"Waiting on {unmet}",
        title=f"The check could not run because {unmet} did not pass",
        help=[Help(text="Address the check it depends on, then assess again")],
    )


def waived(task: Task, because: str) -> Check:
    return Check(
        concern=task.concern,
        check=task.check,
        state=State.WAIVED,
        reason=because,
        title=f"The check does not apply here: {because}",
    )


def unreachable(task: Task, target: str, reason: str) -> Check:
    return Check(
        concern=task.concern,
        check=task.check,
        state=task.tier.unreachable_state,
        reason=f"{target} could not be reached",
        title=f"{target} could not be reached from this workstation",
        facts=[reason],
        help=[
            Help(
                text="The pods of the instance reach this from inside the cloud, so "
                "this may be a route your workstation does not have rather than a "
                "problem with the instance"
            )
        ],
    )
