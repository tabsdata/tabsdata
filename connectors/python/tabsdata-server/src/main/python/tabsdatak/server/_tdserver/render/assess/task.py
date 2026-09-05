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

    def as_arguments(self) -> list[str]:
        arguments = ["--instance", self.instance]
        for name, value in (
            ("provider", self.provider),
            ("context", self.context),
            ("kubeconfig", self.kubeconfig),
            ("namespace", self.namespace),
        ):
            if value:
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

    @property
    def key(self) -> str:
        return f"{self.concern}/{self.check}"


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
