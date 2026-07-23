#
# Copyright 2026 Tabsdata Inc.
#

"""Restart a system's container (e.g. after a host or Docker restart)."""

from __future__ import annotations

import argparse

from tabsdatax.itest.tdksystems2 import cli, console


def add_arguments(parser: argparse.ArgumentParser) -> None:
    cli.add_sys(parser)
    cli.add_sys_dir(parser)
    cli.add_latch(parser)


def run(ctx: cli.Context) -> int:
    assert ctx.info is not None
    ctx.system.restart_system(ctx.info.meta)
    console.good(f"restarted system '{ctx.name}'")
    return 0
