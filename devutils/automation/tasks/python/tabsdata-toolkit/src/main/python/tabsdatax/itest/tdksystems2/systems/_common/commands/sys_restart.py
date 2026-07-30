#
# Copyright 2026 Tabsdata Inc.
#

"""Restart a system's container (e.g. after a host or Docker restart)."""

from __future__ import annotations

import argparse

from tabsdatax.itest.tdksystems2 import cli, console, ssh


def add_arguments(parser: argparse.ArgumentParser) -> None:
    cli.add_sys(parser)
    cli.add_sys_dir(parser)
    cli.add_latch(parser)
    cli.add_manifest(parser, "restart")


def run(ctx: cli.Context) -> int:
    assert ctx.info is not None
    if ssh.is_tunnelled(ctx.info.meta):
        # The container is the far host's to restart; all this end owns is the
        # tunnel, and bouncing that would not restart anything.
        console.warn(f"'{ctx.name}' is reached over an ssh tunnel — nothing to restart")
        return 0
    ctx.system.restart_system(ctx.info.meta)
    console.good(f"restarted system '{ctx.name}'")
    return 0
