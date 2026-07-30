#
# Copyright 2026 Tabsdata Inc.
#

"""Tear down a system and remove its descriptor.

A system with domains is not torn down implicitly: ``destroy`` fails and lists
them, and ``-f`` destroys those domains first (each from its own self-contained
descriptor) and then the system.

A *connected* system (see ``sys_connect``) is torn down by closing its tunnel:
the container itself belongs to the far host and is not ours to remove."""

from __future__ import annotations

import argparse
from pathlib import Path

from tabsdatax.itest.tdksystems2 import cli, console, descriptor, ssh, store, systems
from tabsdatax.itest.tdksystems2.errors import TdxError


def add_arguments(parser: argparse.ArgumentParser) -> None:
    cli.add_sys(parser)
    cli.add_sys_dir(parser)
    cli.add_force(parser)
    cli.add_dry_run(parser)
    cli.add_latch(parser)
    cli.add_manifest(parser, "destroy")


def _domains_of(sys_uuid: str) -> list[tuple[descriptor.DomainInfo, Path]]:
    """The domains in the (default) domain store created in the system instance
    `sys_uuid`. Matched by the system's UUID, not its name, so a stale domain from
    an earlier same-named system is not mistaken for this one's."""
    dom_dir = store.domain_dir(None)
    out: list[tuple[descriptor.DomainInfo, Path]] = []
    if not dom_dir.is_dir():
        return out
    for path in sorted(p for p in dom_dir.iterdir() if p.is_file()):
        if not descriptor.is_descriptor(path):
            continue
        try:
            info = descriptor.load(path)
        except TdxError:
            continue
        if (
            isinstance(info, descriptor.DomainInfo)
            and sys_uuid
            and info.sys_uuid == sys_uuid
        ):
            out.append((info, path))
    return out


def _destroy_domain(info: descriptor.DomainInfo, path: Path, *, force: bool) -> None:
    backend = systems.get(info.system)
    bind = getattr(backend, "configure", None)
    if bind is not None:
        bind(info.meta)
    try:
        backend.destroy_domain(info.meta)
    except Exception as exc:  # noqa: BLE001 - -f removes the descriptor regardless
        if not force:
            raise
        console.warn(
            f"  domain '{info.name}' teardown failed ({exc}); "
            f"-f: removing descriptor anyway"
        )
    path.unlink(missing_ok=True)


def run(ctx: cli.Context) -> int:
    assert ctx.info is not None and ctx.path is not None
    domains = _domains_of(ctx.info.uuid)
    if ctx.args.dry_run:
        names = ", ".join(info.name for info, _ in domains)
        if domains and not ctx.args.force:
            console.out(
                f"would refuse: system '{ctx.name}' has {len(domains)} domain(s) "
                f"({names}); needs -f"
            )
        else:
            for info, _ in domains:
                console.out(f"  would destroy domain '{info.name}'")
            console.out(f"would destroy system '{ctx.name}'")
        return 0
    console.out(f"▶ destroying system '{ctx.name}' …")
    if domains and not ctx.args.force:
        names = ", ".join(info.name for info, _ in domains)
        raise TdxError(
            f"system '{ctx.name}' has {len(domains)} domain(s) ({names}); "
            f"use -f to destroy them and the system"
        )
    for info, path in domains:
        _destroy_domain(info, path, force=ctx.args.force)
        console.good(f"  - domain '{info.name}'")
    if ssh.is_tunnelled(ctx.info.meta):
        # The container belongs to the far host; all this machine owns is the
        # tunnel, and closing it is the whole teardown.
        pid = int(ctx.info.meta[ssh.TUNNEL_PID])
        ssh.close_tunnel(pid)
        console.good(f"  - ssh tunnel (pid {pid})")
    else:
        try:
            ctx.system.destroy_system(ctx.info.meta)
        except Exception as exc:  # noqa: BLE001 - -f removes the descriptor regardless
            if not ctx.args.force:
                raise
            console.warn(f"teardown failed ({exc}); -f: removing descriptor anyway")
    ctx.path.unlink(missing_ok=True)
    console.good(f"destroyed system '{ctx.name}'")
    return 0
