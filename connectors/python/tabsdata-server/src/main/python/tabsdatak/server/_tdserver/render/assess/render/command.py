#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
from dataclasses import replace

from rich.console import Console

from tabsdatak.server._tdserver.render.assess import Report, epilogue, legend
from tabsdatak.server._tdserver.render.assess.core.plan import TASKS
from tabsdatak.server._tdserver.render.assess.core.task import Target
from tabsdatak.server._tdserver.render.assess.render.explain import explanation
from tabsdatak.server._tdserver.render.assess.render.live import assess, console_width
from tabsdatak.server._tdserver.render.assess.render.subject import settings_of, subject

EXIT_OK = 0
EXIT_BLOCKED = 1


def parse() -> tuple[bool, Target]:
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
    parser.add_argument("--explain", action="store_true")
    parsed = parser.parse_args()
    given = frozenset(
        name
        for name in ("provider", "context", "kubeconfig", "namespace")
        if getattr(parsed, name)
    )
    return parsed.explain, resolved(
        Target(
            instance=parsed.instance,
            provider=parsed.provider,
            context=parsed.context,
            kubeconfig=parsed.kubeconfig,
            current=parsed.current,
            namespace=parsed.namespace,
            given=given,
        )
    )


def resolved(target: Target) -> Target:
    settings = settings_of(target.instance)
    if not settings:
        return target
    cluster = settings.get("cluster") or {}
    adjusted = cluster.get("adjusted") or {}
    provided = cluster.get("provided") or {}

    def taken(given, *names):
        if given:
            return given
        for name in names:
            found = adjusted.get(name) or provided.get(name)
            if found:
                return found
        return given

    return replace(
        target,
        provider=target.provider or settings.get("provider"),
        context=taken(target.context, "context"),
        kubeconfig=taken(target.kubeconfig, "kubeconfig"),
        namespace=taken(target.namespace, "namespace"),
    )


def command() -> int:
    explaining, target = parse()
    console = Console(width=console_width())

    console.print()
    console.print(legend())
    console.print()
    console.print(subject(target))

    checks, seconds = assess(TASKS, target, console)
    report = Report(instance=target.instance, seconds=seconds, checks=checks)
    epilogue(report, console)

    if explaining:
        console.print(explanation(TASKS, target))
        console.print()

    return EXIT_BLOCKED if any(check.state.blocking for check in checks) else EXIT_OK
