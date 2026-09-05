#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse

from rich.console import Console

from tabsdatak.server._tdserver.render.assess import Report, epilogue, legend
from tabsdatak.server._tdserver.render.assess.live import assess, console_width
from tabsdatak.server._tdserver.render.assess.plan import TASKS
from tabsdatak.server._tdserver.render.assess.task import Target

EXIT_OK = 0
EXIT_BLOCKED = 1


def parse() -> Target:
    parser = argparse.ArgumentParser(
        prog="tdk_assess",
        description="Check whether a tabsdata instance can be created",
    )
    parser.add_argument("--instance", required=True)
    parser.add_argument("--provider")
    parser.add_argument("--context")
    parser.add_argument("--kubeconfig")
    parser.add_argument("--namespace")
    parser.add_argument("--current", action="store_true")
    parsed = parser.parse_args()
    return Target(
        instance=parsed.instance,
        provider=parsed.provider,
        context=parsed.context,
        kubeconfig=parsed.kubeconfig,
        current=parsed.current,
        namespace=parsed.namespace,
    )


def command() -> int:
    target = parse()
    console = Console(width=console_width())

    console.print()
    console.print(legend())
    console.print()

    checks, seconds = assess(TASKS, target, console)
    report = Report(instance=target.instance, seconds=seconds, checks=checks)
    epilogue(report, console)

    return EXIT_BLOCKED if any(check.state.blocking for check in checks) else EXIT_OK
