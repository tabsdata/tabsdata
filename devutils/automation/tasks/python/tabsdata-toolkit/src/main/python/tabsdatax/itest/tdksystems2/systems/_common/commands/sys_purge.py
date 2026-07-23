#
# Copyright 2026 Tabsdata Inc.
#

"""Drop stale domain entity-containers straight from a system (no descriptor).

Only user domain containers are ever candidates — the system's own schemas are
never listed or dropped, whatever ``--prefix`` is given.
"""

from __future__ import annotations

import argparse

from tabsdatax.itest.tdksystems2 import cli, console, constants, naming


def add_arguments(parser: argparse.ArgumentParser) -> None:
    cli.add_sys(parser)
    cli.add_sys_dir(parser)
    parser.add_argument(
        constants.OPT_EXTERNAL_PREFIX,
        dest="external_prefix",
        default=None,
        metavar="PREFIX",
        help="match this external-entity prefix instead of d_<user>_<build>_ "
        "('' = every container)",
    )
    cli.add_force(parser)
    cli.add_latch(parser)


def run(ctx: cli.Context) -> int:
    assert ctx.info is not None
    meta = ctx.info.meta
    sysobj = ctx.system
    word = sysobj.domain_word
    matched = [
        e
        for e in sysobj.list_domain_entities(meta)
        if naming.purge_match(e, ctx.args.external_prefix)
    ]
    if not matched:
        console.out("nothing to purge")
        return 0
    if not ctx.args.force:
        console.out(f"would purge {len(matched)} {word}(s) (use -f to drop):")
        for e in matched:
            console.out(f"  {e}")
        return 0
    for e in matched:
        sysobj.purge_domain_entity(meta, e)
        console.good(f"purged {e}")
    console.good(f"purged {len(matched)} {word}(s) from '{ctx.name}'")
    return 0
