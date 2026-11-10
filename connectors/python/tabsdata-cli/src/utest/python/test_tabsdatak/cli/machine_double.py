#
# Copyright 2026 Tabsdata Inc.
#

"""A CLI extension for tests: ``--machine`` in argv runs the invocation as a
:class:`Machine` whose one document, or ``{"error": {...}}``, is printed as JSON
on stdout with the error's exit code; click exiting on its own (help) prints
its text instead. :data:`SETTINGS` sets the other Machine fields for a
test."""

from __future__ import annotations

import json
import sys

import rich_click as click

from tabsdatak.cli._errors import ExitCode, TdkError, classify
from tabsdatak.cli._machine import Machine, running

FLAG = "--machine"
SETTINGS: dict = {}


class MachineDouble:
    def main(self, root, argv, run):
        if FLAG not in argv:
            return run(argv)
        argv = [arg for arg in argv if arg != FLAG]
        emitted = []

        def emit(document):
            if emitted:
                raise RuntimeError("A machine run emitted more than one result.")
            emitted.append(document)
            click.echo(json.dumps(document, default=str))

        error, exited = None, None
        with running(Machine(emit=emit, command=["tdk", *argv], **SETTINGS)):
            try:
                exited = run(argv, standalone_mode=False)
            except click.ClickException as e:
                error = getattr(e, "error", None) or classify(e)
            except (click.Abort, Exception) as e:
                error = classify(e)
        if error is None and not emitted and exited != 0:
            error = TdkError(type="internal", message="The command produced no result.")
        if error is None:
            sys.exit(ExitCode.OK)
        envelope = {
            "type": error.type,
            "status": error.status,
            "code": error.code or f"tdk::{error.type}",
            "message": error.message,
            "hint": error.hint,
            "retry_after": error.retry_after,
            **error.extra,
        }
        click.echo(json.dumps({"error": envelope}, default=str))
        sys.exit(error.exit_code)

    def describe_root(self) -> dict:
        return {}

    def describe(self, command, path) -> dict:
        return {}
