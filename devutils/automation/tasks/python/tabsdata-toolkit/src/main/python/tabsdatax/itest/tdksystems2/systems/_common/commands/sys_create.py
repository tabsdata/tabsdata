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
from tabsdatax.itest.tdksystems2.shell import port_open
from tabsdatax.itest.tdksystems2.systems._common.host import DEFAULT_HOSTNAME


def add_arguments(parser: argparse.ArgumentParser) -> None:
    cli.add_type(parser)
    cli.add_sys(parser, required=False, multi=False)
    cli.add_sys_prefix(parser)
    cli.add_ports(parser)
    cli.add_sys_hostname(parser)
    cli.add_sys_dir(parser)
    cli.add_force(parser)
    cli.add_dry_run(parser)
    cli.add_latch(parser)
    cli.add_manifest(parser, "create")
    cli.add_sync(parser)
    # the dispatcher attaches the resolved backend (chosen from --type) so its
    # declarative options become create flags; None on a bare `create -h`.
    sysobj = getattr(parser, "tdx_system", None)
    if sysobj is not None:
        options.add_option_flags(
            parser, sysobj.options(), getattr(sysobj, "env_prefix", "")
        )
    else:
        parser.epilog = (
            f"Each type adds its own options (ports, credentials, ...). Name one "
            f"to see them: {constants.SYS_TOOL} {constants.V_CREATE} "
            f"{constants.OPT_TYPE} TYPE -h"
        )


def _where(meta: dict[str, str], name: str) -> str:
    if meta.get("LOCATION"):
        return meta["LOCATION"]
    if meta.get("PORT"):
        return f"{meta.get('HOST') or DEFAULT_HOSTNAME}:{meta['PORT']}"
    return meta.get("SYS_PATH") or name


def _bind_hostname(sysobj, raw: str | None) -> str:
    """Bind the advertised hostname for this create, returning what was bound
    (``""`` when the flag was not given, or the system has no endpoint of its own
    to advertise). Only a container publishes a host port for tdx to name: an
    object store or credential-only service has no endpoint at all, and a remote
    warehouse has one it configures itself. Either way the flag is reported and
    ignored rather than failing, so ``create --type ALL`` works."""
    if raw is None:
        return ""
    hostname = cli.parse_hostname(raw)
    use_hostname = getattr(sysobj, "use_hostname", None)
    if use_hostname is None or not getattr(sysobj, "is_container", False):
        has_own = use_hostname is not None
        why = (
            "takes its host from its own configuration"
            if has_own
            else "has no host of its own"
        )
        console.warn(f"{sysobj.slug} {why} — {constants.OPT_SYS_HOSTNAME} ignored")
        return ""
    use_hostname(hostname)
    return hostname


def _check_reachable(meta: dict[str, str], hostname: str) -> None:
    """Warn when the advertised endpoint does not answer from this machine.

    Every tdx verb past ``create`` — domain create, the data plane, status — is
    an ordinary client of the recorded endpoint, so it has to run somewhere the
    advertised host resolves and routes. That is the point of the flag (the other
    machine is often the only one that qualifies), but silently handing back a
    system this box cannot use turns into a driver timeout much later."""
    port = meta.get("PORT", "")
    if not port.isdigit() or port_open(hostname, int(port)):
        return
    console.warn(
        f"{hostname}:{port} does not answer from this machine — run the domain "
        f"commands where {hostname} resolves and routes to it"
    )


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
    use_ports = getattr(sysobj, "use_ports", None)
    if use_ports is not None:
        use_ports(cli.parse_ports(a.ports))
    hostname = _bind_hostname(sysobj, a.sys_hostname)
    # resolve before announcing: an option with no value is a plain usage error,
    # and saying "creating" first reads as though something was half-built
    values, env_refs, literals = options.resolve(sysobj, a, name)
    console.out(f"▶ creating system '{name}' ({sysobj.label}) …")
    meta = sysobj.plan_system(name, values)
    meta["SYSTEM_KIND"] = sysobj.kind
    # Persist the descriptor before provisioning, so a partial start can be torn
    # down from disk with `destroy`.
    descriptor.write_system(path, sysobj.slug, name, meta, env_refs, literals, hostname)
    extra = sysobj.provision_system(meta, force=a.force)
    if extra:
        meta.update(extra)
        descriptor.write_system(
            path, sysobj.slug, name, meta, env_refs, literals, hostname
        )
    if hostname:
        _check_reachable(meta, hostname)
    console.good(f"created system '{name}' ({sysobj.label}) at {_where(meta, name)}")
    return 0
