#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

import yaml

from tabsdatak.server._tdserver.render.assess import Check, Help, State
from tabsdatak.server._tdserver.render.assess.core.task import Target

CONCERN = "Coherence"

AGENT_KEYS = "Agent keys"
SECRET_SHAPE = "Secret placeholders"
PACKAGE_INDEX = "Package index"

CODE_AGENT = "assess::coherence::agent"
CODE_SECRETS = "assess::coherence::secrets"
CODE_INDEX = "assess::coherence::index"

AGENT_FILE = "aiagent.yaml"
SCHEDULING_FILE = "tabsdata.yaml"

REACH_TIMEOUT = 10

PLACEHOLDER = re.compile(r"\$\{\{[^}]*\}\}")

OPTIONAL = re.compile(r"\$\{\{secret:([^}?]+)\?\}\}")
REQUIRED = re.compile(r"\$\{\{secret:([^}?]+)\}\}")
GENERATED = re.compile(r"\$\{\{secretAuto:([^}?]+)\}\}")

AGENT_ENVIRONMENT = {
    "anthropic": "ANTHROPIC__API_KEY",
    "openai": "OPENAI__API_KEY",
}


def configs_of(instance: str) -> Path:
    return (
        Path.home()
        / ".tabsdata"
        / "instances"
        / instance
        / "init"
        / "input"
        / "configs"
    )


def passed(check: str, started: float) -> Check:
    return Check(
        concern=CONCERN,
        check=check,
        state=State.PASSED,
        seconds=time.monotonic() - started,
    )


def unknown(check: str, started: float, instance: str) -> Check:
    return Check(
        concern=CONCERN,
        check=check,
        state=State.SKIPPED,
        seconds=time.monotonic() - started,
        reason="the configuration of the instance is not readable yet",
        facts=[f"Expected folder: {configs_of(instance)}"],
    )


def assess_package_index(target: Target) -> Check:
    started = time.monotonic()
    folder = configs_of(target.instance)
    if not folder.is_dir():
        return unknown(PACKAGE_INDEX, started, target.instance)

    path = folder / SCHEDULING_FILE
    if not path.is_file():
        return unknown(PACKAGE_INDEX, started, target.instance)

    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return unknown(PACKAGE_INDEX, started, target.instance)

    named = ((document.get("pods") or {}).get("global") or {}).get("envs") or {}
    index = str(named.get("UV_INDEX") or "").strip()
    if not index or PLACEHOLDER.search(index):
        return Check(
            concern=CONCERN,
            check=PACKAGE_INDEX,
            state=State.WAIVED,
            reason="the instance names no package index of its own",
        )

    try:
        request = urllib.request.Request(index, method="HEAD")
        with urllib.request.urlopen(request, timeout=REACH_TIMEOUT) as answer:
            code = answer.status
    except urllib.error.HTTPError as error:
        code = error.code
    except Exception as error:
        return Check(
            concern=CONCERN,
            check=PACKAGE_INDEX,
            state=State.WARNING,
            seconds=time.monotonic() - started,
            code=CODE_INDEX,
            title="The package index could not be reached from this workstation",
            facts=[index, str(error).strip().splitlines()[0]],
            help=[
                Help(
                    text="The pods build their environments from this, so they reach "
                    "it from inside the cloud rather than from here"
                )
            ],
        )

    if code < 400:
        return Check(
            concern=CONCERN,
            check=PACKAGE_INDEX,
            state=State.PASSED,
            seconds=time.monotonic() - started,
            facts=[index],
        )

    return Check(
        concern=CONCERN,
        check=PACKAGE_INDEX,
        state=State.WARNING,
        seconds=time.monotonic() - started,
        code=CODE_INDEX,
        title=f"The package index answered {code}",
        facts=[index],
        help=[
            Help(
                text="An instance builds a python environment for every function it "
                "runs, and each of those resolves against this index"
            )
        ],
    )


def assess_agent_keys(target: Target) -> Check:
    started = time.monotonic()
    folder = configs_of(target.instance)
    if not folder.is_dir():
        return unknown(AGENT_KEYS, started, target.instance)

    path = folder / AGENT_FILE
    if not path.is_file():
        return Check(
            concern=CONCERN,
            check=AGENT_KEYS,
            state=State.WAIVED,
            reason="this instance runs no ai agent",
        )

    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as error:
        return Check(
            concern=CONCERN,
            check=AGENT_KEYS,
            state=State.FAILED,
            seconds=time.monotonic() - started,
            code=CODE_AGENT,
            title=f"The file {AGENT_FILE} could not be read",
            facts=[str(error).strip().splitlines()[0]],
        )

    declared = document.get("ai_providers") or {}
    if not declared:
        return Check(
            concern=CONCERN,
            check=AGENT_KEYS,
            state=State.WAIVED,
            reason="the agent declares no model provider",
        )

    silent = []
    for name, settings in declared.items():
        given = str((settings or {}).get("api_key") or "")
        if not OPTIONAL.search(given) and not REQUIRED.search(given):
            continue
        variable = AGENT_ENVIRONMENT.get(name)
        if variable and os.environ.get(variable):
            continue
        silent.append(
            f"{name} has no key, and none is in the {variable} environment variable"
            if variable
            else f"{name} has no key"
        )

    if len(silent) < len(declared):
        return passed(AGENT_KEYS, started)

    return Check(
        concern=CONCERN,
        check=AGENT_KEYS,
        state=State.WARNING,
        seconds=time.monotonic() - started,
        code=CODE_AGENT,
        title="The agent will start with no model it can call",
        facts=silent,
        help=[
            Help(
                text="The agent runs either way, and the features that call a model "
                "are simply unavailable until a key is supplied"
            )
        ],
    )


def assess_secret_shape(target: Target) -> Check:
    started = time.monotonic()
    folder = configs_of(target.instance)
    if not folder.is_dir():
        return unknown(SECRET_SHAPE, started, target.instance)

    wanted: dict[str, set[str]] = {}
    generated: dict[str, set[str]] = {}
    for path in sorted(folder.glob("*.yaml")):
        body = path.read_text(encoding="utf-8")
        for name in REQUIRED.findall(body):
            wanted.setdefault(name, set()).add(path.name)
        for name in GENERATED.findall(body):
            generated.setdefault(name, set()).add(path.name)

    clashing = sorted(set(wanted) & set(generated))
    if not clashing:
        return passed(SECRET_SHAPE, started)

    return Check(
        concern=CONCERN,
        check=SECRET_SHAPE,
        state=State.FAILED,
        seconds=time.monotonic() - started,
        code=CODE_SECRETS,
        title="A secret is asked for and generated at the same time",
        facts=[
            f"{name}: asked for in {', '.join(sorted(wanted[name]))}, "
            f"generated in {', '.join(sorted(generated[name]))}"
            for name in clashing
        ],
        help=[
            Help(
                text="A name is either supplied with ${{secret:}} or generated with "
                "${{secretAuto:}}, and declaring both leaves the value undecided"
            )
        ],
    )
