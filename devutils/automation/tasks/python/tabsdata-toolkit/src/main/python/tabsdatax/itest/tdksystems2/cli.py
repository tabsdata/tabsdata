#
# Copyright 2026 Tabsdata Inc.
#

"""Shared CLI plumbing for command modules: the ``Context`` handed to every
``run(ctx)``, an argv peeker the dispatcher uses to pick a target before full
parsing, and ``add_*`` helpers so every command declares its options with the
same (constant) flag names. Option help names the env var it overrides.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from tabsdatax.itest.tdksystems2 import constants, descriptor, protocols
from tabsdatax.itest.tdksystems2.errors import TdxError


@dataclass
class Context:
    """What a command's ``run(ctx)`` receives.

    ``info`` is the descriptor the command operates on — the :class:`SystemInfo`
    for system verbs, the :class:`DomainInfo` for domain verbs (except
    ``create``, which receives the *system* info because the domain does not
    exist yet, and system ``create`` / ``fetch-bin``, which have no descriptor
    yet and get ``None``). ``path`` is that descriptor's file (``None`` when
    absent). ``system`` is the backend instance; ``store_dir`` is this tool's
    store directory."""

    tool: str
    verb: str
    slug: str
    system: protocols.Backend
    args: argparse.Namespace
    name: str | None
    info: descriptor.SystemInfo | descriptor.DomainInfo | None
    path: Path | None
    store_dir: Path


def peek(argv: list[str], flag: str) -> str | None:
    """The value of `flag` in raw `argv` (``--flag value`` or ``--flag=value``),
    or None. Used before argparse runs to choose which system / descriptor a verb
    targets."""
    for i, tok in enumerate(argv):
        if tok == flag:
            return argv[i + 1] if i + 1 < len(argv) else None
        if tok.startswith(flag + "="):
            return tok[len(flag) + 1 :]
    return None


def add_type(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        constants.OPT_TYPE,
        metavar="TYPE",
        help="system type (e.g. mysql); a comma list or ALL selects several",
    )


def add_sys(
    parser: argparse.ArgumentParser, required: bool = True, multi: bool = True
) -> None:
    parser.add_argument(
        constants.OPT_SYS,
        required=required,
        metavar="NAME",
        help="system name; a comma list or ALL selects several"
        if multi
        else "system name",
    )


def add_domain(
    parser: argparse.ArgumentParser, required: bool = True, multi: bool = True
) -> None:
    parser.add_argument(
        constants.OPT_DOMAIN,
        required=required,
        metavar="NAME",
        help="domain name; a comma list or ALL selects several"
        if multi
        else "domain name",
    )


def add_sys_dir(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        constants.OPT_SYS_DIR,
        default=None,
        metavar="DIR",
        help=f"override ${constants.ENV_BACKENDS_DIR}/{constants.SYSTEMS_SUBDIR} "
        "(the system store dir)",
    )


def add_manifest(parser: argparse.ArgumentParser, action: str) -> None:
    """Declare ``--manifest FILE`` as an alternative to naming targets.

    The dispatcher peels this off raw argv and fans the verb out over the
    manifest's entries, so the value never reaches this parser — but the option
    is real, and without declaring it here ``-h`` would deny it exists."""
    parser.add_argument(
        constants.OPT_MANIFEST,
        default=None,
        metavar="FILE",
        help=f"{action} every entry the YAML manifest names, in parallel, "
        f"instead of naming targets (- = stdin)",
    )


def add_sync(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        constants.OPT_SYNC,
        action="store_true",
        help=f"with {constants.OPT_MANIFEST}: reconcile — also destroy store "
        f"entries the manifest omits",
    )


def add_report(parser: argparse.ArgumentParser) -> None:
    """Declare ``--report FILE`` — the batch runner's JSON run record.

    Batch verbs (a list / ``ALL`` selector) never reach an argparse parser: the
    dispatcher peeks this off raw argv. A ``--manifest`` run does parse its
    options, so it declares the flag here to accept (and document) it."""
    parser.add_argument(
        constants.OPT_REPORT,
        default=None,
        metavar="FILE",
        help="write a JSON record of the run (per-target status, seconds and "
        "output) to FILE",
    )


def add_fail_fast(parser: argparse.ArgumentParser) -> None:
    """Declare ``--fail-fast`` — see :func:`add_report` on why it is needed."""
    parser.add_argument(
        constants.OPT_FAIL_FAST,
        action="store_true",
        help="stop launching further targets once one fails (default: attempt "
        "every target)",
    )


def add_domain_dir(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        constants.OPT_DOMAIN_DIR,
        default=None,
        metavar="DIR",
        help=f"override ${constants.ENV_BACKENDS_DIR}/{constants.DOMAINS_SUBDIR} "
        "(the domain store dir)",
    )


def add_force(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        constants.OPT_FORCE_SHORT,
        constants.OPT_FORCE,
        action="store_true",
        help="overwrite / drop-first if present",
    )


def add_sys_prefix(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        constants.OPT_SYS_PREFIX,
        dest="name_prefix",
        default=None,
        metavar="PREFIX",
        help="prepend PREFIX_ to the created system name(s)",
    )


def add_domain_prefix(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        constants.OPT_DOMAIN_PREFIX,
        dest="name_prefix",
        default=None,
        metavar="PREFIX",
        help="prepend PREFIX_ to the created domain name(s)",
    )


def apply_prefix(name: str, prefix: str | None) -> str:
    """``PREFIX_name`` when `prefix` is set, else `name` unchanged."""
    return f"{prefix}_{name}" if prefix else name


def add_ports(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        constants.OPT_PORTS,
        dest="ports",
        default=None,
        metavar="PORT,PORT,...",
        help="candidate free host ports to use if the system needs any "
        "(default: find them as needed)",
    )


def add_sys_hostname(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        constants.OPT_SYS_HOSTNAME,
        dest="sys_hostname",
        default=None,
        metavar="HOST",
        help="hostname the system advertises in its descriptor instead of "
        "localhost, so the domains created from it are reachable from other "
        "machines (container systems only)",
    )


def parse_hostname(raw: str) -> str:
    """Validate the ``--sys-hostname`` value: a bare hostname or IPv4 address.

    Every endpoint the descriptor records is built as ``<host>:<port>``, so a
    value carrying a scheme, a port or an IPv6 literal would be spliced into a
    URL that no client — nor tdx's own URL parsing — can read back."""
    host = raw.strip()
    if not host:
        raise TdxError(f"{constants.OPT_SYS_HOSTNAME}: hostname is empty")
    for char, what in ((":", "a port or an IPv6 literal"), ("/", "a scheme or path")):
        if char in host:
            raise TdxError(
                f"{constants.OPT_SYS_HOSTNAME}: {host!r} contains {what} — pass a "
                f"bare hostname or IPv4 address"
            )
    if any(c.isspace() or c in "@?#[]" for c in host):
        raise TdxError(
            f"{constants.OPT_SYS_HOSTNAME}: not a hostname or IPv4 address: {host!r}"
        )
    return host


def parse_ports(raw: str | None) -> list[int]:
    """Parse the ``--ports`` comma list into port numbers (``[]`` if unset)."""
    ports: list[int] = []
    for tok in (raw or "").split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            ports.append(int(tok))
        except ValueError:
            raise TdxError(f"--ports: not a port number: {tok!r}")
    return ports


def add_dry_run(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        constants.OPT_DRY_RUN,
        dest="dry_run",
        action="store_true",
        help="print what would be created/destroyed without doing it",
    )


def add_latch(parser: argparse.ArgumentParser) -> None:
    # Parsed here so the flag is accepted; the dispatcher reads it from raw argv.
    parser.add_argument(
        constants.OPT_LATCH,
        default=None,
        metavar="FILE",
        help="write FILE.ok/FILE.err then touch FILE on completion (background wait)",
    )


def add_model(parser: argparse.ArgumentParser, required: bool = True) -> None:
    parser.add_argument(
        constants.OPT_MODEL, required=required, metavar="MODEL", help="model name"
    )


def add_size(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        constants.OPT_SIZE,
        type=int,
        default=1,
        metavar="N",
        help="size multiplier: 1 = base (~100 rows of each sizeable entity), "
        "N = N x base (default 1)",
    )


def add_out(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        constants.OPT_OUT,
        default=None,
        choices=list(constants.OUT_FORMATS),
        help="machine-readable output on stdout (json = per-entity insert/update/"
        "delete counts); omit for human-readable progress",
    )


def add_info_format(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        constants.OPT_FORMAT,
        default="json",
        choices=list(constants.INFO_FORMATS),
        help="output format (default: json)",
    )
    parser.add_argument(
        constants.OPT_PREFIX,
        default=None,
        metavar="PREFIX",
        help="emit keys as <PREFIX>__<KEY> (one backend; a batch render uses "
        "each backend's own name)",
    )
