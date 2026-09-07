#
#  Copyright 2026 Tabsdata Inc.
#

import os
import pathlib
import shlex


def quote(command: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in command)


def make_command() -> list[str]:
    makefile = os.environ.get("CARGO_MAKE_MAKEFILE_PATH", "Makefile.toml")
    profile = os.environ.get("CARGO_MAKE_PROFILE", "development")
    return [
        "cargo",
        "make",
        "--disable-check-for-updates",
        "--no-on-error",
        "--loglevel=info",
        f"--profile={profile}",
        "--allow-private",
        "--skip-init-end-tasks",
        "--makefile",
        makefile,
    ]


def read(target: pathlib.Path) -> str:
    try:
        return target.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def outcome(codes: dict[str, str]) -> tuple[int, list[str]]:
    result = 0
    unknown = []
    for name, code in codes.items():
        if not code.isdigit():
            unknown.append(name)
            continue
        value = int(code)
        if value and not result:
            result = value
    return (result or 1) if unknown else result, unknown
