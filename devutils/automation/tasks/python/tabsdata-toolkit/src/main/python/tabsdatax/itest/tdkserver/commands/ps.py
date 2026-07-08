#
# Copyright 2026 Tabsdata Inc.
#

import os
from pathlib import Path

import psutil
import rich_click as click

INSTANCE_EMOJI = "🏠"
ROOT_EMOJI = "👀"
WORKER_EMOJI = "⚙️"
ACTOR_EMOJI = "🛞"

CYAN = "\033[96m"
UNDERLINE = "\033[4m"
END = "\033[0m"

PROCESS_COLORS = [
    "\033[94m",
    "\033[92m",
    "\033[93m",
    "\033[91m",
    "\033[95m",
]

KM_PROCESS_NAMES = ("km", "km.exe")
WORKDIR_ARGUMENT = "--workdir"
CLUSTER_FOLDER = "cluster"


def instance_of(cmdline: list[str]) -> str:
    if WORKDIR_ARGUMENT in cmdline:
        index = cmdline.index(WORKDIR_ARGUMENT)
        if index + 1 < len(cmdline):
            workdir = Path(cmdline[index + 1])
            if workdir.name == CLUSTER_FOLDER:
                return workdir.parent.name
            return workdir.name
    return "unknown"


def process_name(process: dict) -> str:
    binary = os.path.basename(process["name"] or "?")
    cmdline = process.get("cmdline") or []
    if binary.startswith("python") or binary in ("bash", "sh", "zsh"):
        if "-m" in cmdline:
            try:
                return cmdline[cmdline.index("-m") + 1]
            except IndexError:
                return binary
        if len(cmdline) > 1 and not cmdline[1].startswith("-"):
            return os.path.basename(cmdline[1])
    return binary


@click.command(name="ps")
def ps():
    """List km processes and their components for each kubernetes instance."""
    click.echo(f"{UNDERLINE}Running tabsdata kubernetes instances{END}")
    click.echo("")

    processes = {
        process.pid: process.info
        for process in psutil.process_iter(["pid", "ppid", "name", "cmdline"])
    }

    roots = {}
    for pid, process in processes.items():
        if (process["name"] or "").lower() in KM_PROCESS_NAMES:
            roots[pid] = instance_of(process.get("cmdline") or [])

    children: dict[int, list[int]] = {}
    for pid, process in processes.items():
        children.setdefault(process["ppid"], []).append(pid)

    def print_process_tree(pid: int, level: int = 0, indent: str = "   "):
        process = processes.get(pid)
        if not process:
            return
        color = PROCESS_COLORS[min(level, len(PROCESS_COLORS) - 1)]
        if level == 0:
            emoji = ROOT_EMOJI
        elif level == 1:
            emoji = WORKER_EMOJI
        else:
            emoji = ACTOR_EMOJI
        click.echo(
            f"{indent}{color}{emoji} "
            f"{process['pid']:>6} "
            f"{process['ppid']:>6} "
            f"{process_name(process)}{END}"
        )
        for child in sorted(children.get(pid, [])):
            print_process_tree(child, level + 1, indent)

    for pid, instance in sorted(roots.items()):
        click.echo(f"{INSTANCE_EMOJI} {CYAN}Instance: {instance}{END}")
        print_process_tree(pid)
        click.echo("")
