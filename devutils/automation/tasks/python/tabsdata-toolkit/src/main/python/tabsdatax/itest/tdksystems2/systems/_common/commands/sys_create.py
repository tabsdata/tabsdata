#
# Copyright 2026 Tabsdata Inc.
#

"""Provision a system.

One create for every system type: the container databases (docker run), the
object stores and warehouses (config from the environment), the credential-only
services and the local filesystem. The system's declarative ``options()`` drive
the create flags — ``--<flag> VALUE`` for a literal, ``--<flag>:env ENV_VAR`` to
read it from an env var — each with a documented default (a free port, a
generated password, a ``$<PREFIX>__<KEY>`` environment variable, …). Every
resolved value is baked into the encrypted descriptor so the system is
self-contained; the source env var (never the value) is recorded so ``manifest``
can re-emit it.
"""

from __future__ import annotations

import argparse

from tabsdatax.itest.tdksystems2 import (
    cli,
    console,
    constants,
    descriptor,
    options,
    store,
)
from tabsdatax.itest.tdksystems2.errors import TdxError


def add_arguments(parser: argparse.ArgumentParser) -> None:
    cli.add_type(parser)
    cli.add_sys(parser, required=False, multi=False)
    cli.add_sys_prefix(parser)
    cli.add_ports(parser)
    cli.add_sys_dir(parser)
    cli.add_force(parser)
    cli.add_dry_run(parser)
    cli.add_latch(parser)
    # the dispatcher attaches the resolved backend (chosen from --type) so its
    # declarative options become create flags; None on a bare `create -h`.
    sysobj = getattr(parser, "tdx_system", None)
    if sysobj is not None:
        options.add_option_flags(
            parser, sysobj.options(), getattr(sysobj, "env_prefix", "")
        )


def _where(meta: dict[str, str], name: str) -> str:
    if meta.get("LOCATION"):
        return meta["LOCATION"]
    if meta.get("PORT"):
        return f"localhost:{meta['PORT']}"
    return meta.get("SYS_PATH") or name


def run(ctx: cli.Context) -> int:
    a = ctx.args
    # default the system name to its type (`create --type mysql` -> system 'mysql')
    name = cli.apply_prefix(a.sys or ctx.slug, a.name_prefix)
    descriptor.validate_name(name)
    sysobj = ctx.system
    path = store.system_path(name, a.sys_dir)
    if descriptor.is_descriptor(path) and not a.force:
        raise TdxError(f"system '{name}' already exists (use {constants.OPT_FORCE})")
    if a.dry_run:
        console.out(f"would create system '{name}' (type {sysobj.slug})")
        return 0
    console.out(f"▶ creating system '{name}' ({sysobj.label}) …")
    use_ports = getattr(sysobj, "use_ports", None)
    if use_ports is not None:
        use_ports(cli.parse_ports(a.ports))
    values, env_refs, literals = options.resolve(sysobj, a, name)
    meta = sysobj.plan_system(name, values)
    meta["SYSTEM_KIND"] = sysobj.kind
    # Persist the descriptor before provisioning, so a partial start can be torn
    # down from disk with `destroy`.
    descriptor.write_system(path, sysobj.slug, name, meta, env_refs, literals)
    extra = sysobj.provision_system(meta, force=a.force)
    if extra:
        meta.update(extra)
        descriptor.write_system(path, sysobj.slug, name, meta, env_refs, literals)
    console.good(f"created system '{name}' ({sysobj.label}) at {_where(meta, name)}")
    return 0
