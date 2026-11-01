#
# Copyright 2026 Tabsdata Inc.
#

import itertools
import os
import random
import sys
from datetime import datetime
from pathlib import Path
from typing import Callable

import rich_click as click

from tabsdatax.itest.tdkinstance.commands.registry import (
    INSTANCES_FOLDER,
    TABSDATA_HOME_FOLDER,
)
from tabsdatax.itest.tdkinstance.commands.storage import STORAGE_OPTION

COMPACT_ENV = "TD_KMINUS_COMPACT"
ATTEMPTS_PER_WIDTH = 3


def compact_names() -> bool:
    """Whether instance names are kept short: always on Windows, whose paths
    are capped at 260 characters, and elsewhere when TD_KMINUS_COMPACT is
    true."""
    if sys.platform == "win32":
        return True
    return os.environ.get(COMPACT_ENV, "").strip().lower() == "true"


def instances_folder() -> Path:
    return Path.home() / TABSDATA_HOME_FOLDER / INSTANCES_FOLDER


def stamped_name(prefix: str, now: datetime) -> str:
    stamp = now.strftime("%Y.%m.%d.%H.%M.%S.") + f"{now.microsecond // 1000:03d}"
    return f"{prefix}-{stamp}"


def numbered_name(
    prefix: str,
    instances: Path,
    draw: Callable[[int, int], int] = random.randrange,
) -> str:
    """The prefix and the shortest random number whose instance folder does
    not exist: a few draws of one digit, then of two, and so on."""
    for width in itertools.count(1):
        low = 0 if width == 1 else 10 ** (width - 1)
        for _ in range(ATTEMPTS_PER_WIDTH):
            candidate = f"{prefix}-{draw(low, 10**width)}"
            if not os.path.lexists(instances / candidate):
                return candidate
    raise AssertionError("unreachable")


@click.command(name="name")
@click.argument("role")
@STORAGE_OPTION
def name(role: str, storage: str):
    """Print a collision-proof instance name for an instance role."""
    prefix = f"{role}-{storage}"
    if compact_names():
        click.echo(numbered_name(prefix, instances_folder()))
    else:
        click.echo(stamped_name(prefix, datetime.now()))
