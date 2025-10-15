#
# Copyright 2025 Tabs Data Inc.
#

import argparse
import importlib
import importlib.util
import os
import sys
from enum import Enum
from types import ModuleType

import psutil

if sys.platform == "win32":
    os.system("")


class Color(str, Enum):
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    UNDERLINE = "\033[4m"
    END = "\033[0m"

    def __str__(self) -> str:
        return self.value


class Emoji(str, Enum):
    INSTANCE = "🏠"
    SUPERVISOR = "👀"
    WORKER = "⚙️"
    ACTOR = "🛞"

    def __str__(self) -> str:
        return self.value


INSTANCE_COLOR = Color.CYAN

PROCESS_COLORS = [
    Color.BLUE,
    Color.GREEN,
    Color.YELLOW,
    Color.RED,
    Color.MAGENTA,
]

END_COLOR = Color.END

PYTHON_MODULE_ALIASES = {
    "tabsdata._tabsserver.function.execute_function_from_bundle_path": "tdfunction",
}


# noinspection DuplicatedCode
def load(module_name) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        module_name,
        os.path.join(
            os.getenv("MAKE_LIBRARIES_PATH"),
            f"{module_name}.py",
        ),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


logger = load("log").get_logger()


def ps():  # noqa: C901
    parser = argparse.ArgumentParser(
        description="List supervisor processes and their workers."
    )
    parser.add_argument(
        "--full-command",
        action="store_true",
        help="Show full command instead of just the executable name.",
    )
    arguments = parser.parse_args()

    logger.info(f"{Color.UNDERLINE}Running tabsdata instances{Color.END}")
    logger.info("")

    processes = {
        process.pid: process.info
        for process in psutil.process_iter(["pid", "ppid", "name", "cmdline"])
    }

    supervisors = {}
    for pid, process in processes.items():
        if process["name"] == "supervisor":
            cmdline = process.get("cmdline", [])
            instance = "unknown"
            if "--instance" in cmdline:
                try:
                    index = cmdline.index("--instance")
                    instance_path = cmdline[index + 1]
                    instances_home = os.path.join(
                        os.path.expanduser("~"),
                        ".tabsdata",
                        "instances",
                    )
                    if instance_path.startswith(instances_home):
                        instance = os.path.basename(instance_path)
                    else:
                        instance = instance_path
                except (IndexError, ValueError):
                    pass
            supervisors[pid] = instance

    workers = {}
    for pid, process in processes.items():
        ppid = process["ppid"]
        if ppid not in workers:
            workers[ppid] = []
        workers[ppid].append(pid)

    def get_process_name(process):
        p_name = process["name"]
        p_binary = os.path.basename(p_name)
        p_cmdline = process.get("cmdline", [])

        if p_binary.startswith("python"):
            if "-m" in p_cmdline:
                try:
                    idx = p_cmdline.index("-m")
                    p_module = p_cmdline[idx + 1]
                    return PYTHON_MODULE_ALIASES.get(p_module, p_module)
                except (ValueError, IndexError):
                    return p_binary
            else:
                if len(p_cmdline) > 1 and not p_cmdline[1].startswith("-"):
                    return os.path.basename(p_cmdline[1])
                else:
                    return p_binary
        elif p_binary in ("bash", "sh", "zsh"):
            if len(p_cmdline) > 1 and not p_cmdline[1].startswith("-"):
                return os.path.basename(p_cmdline[1])
            else:
                return p_binary
        elif p_binary in ("cmd", "cmd.exe"):
            p_script = None
            for arg in p_cmdline[1:]:
                if not arg.startswith("/"):
                    p_script = arg
                    break
            if p_script:
                return os.path.basename(p_script)
            else:
                return p_binary
        else:
            return p_binary

    def print_process_tree(p_pid, p_level=0, indent=""):
        process = processes.get(p_pid)
        if not process:
            return

        if arguments.full_command:
            command = " ".join(process["cmdline"] or [process["name"]])
        else:
            command = get_process_name(process)

        color = PROCESS_COLORS[min(p_level, len(PROCESS_COLORS) - 1)]

        if p_level == 0:
            emoji = Emoji.SUPERVISOR
        elif p_level == 1:
            emoji = Emoji.WORKER
        else:
            emoji = Emoji.ACTOR

        logger.info(
            f"{indent}"
            f"{color}"
            f"{emoji} "
            f'{process["pid"]:>6} '
            f'{process["ppid"]:>6} '
            f"{command}"
            f"{END_COLOR}"
        )

        if p_pid in workers:
            for worker in sorted(workers[p_pid]):
                print_process_tree(worker, p_level + 1, indent)

    for pid, instance in sorted(supervisors.items()):
        logger.info(f"{Emoji.INSTANCE} {INSTANCE_COLOR}Instance: {instance}{END_COLOR}")
        print_process_tree(pid, p_level=0, indent="   ")
        logger.info("")


ps()
