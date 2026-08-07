#
# Copyright 2026 Tabsdata Inc.
#

"""Declarative per-system create options.

Each backend declares ``options()`` — the connection knobs its ``create`` takes.
Every option has a DEFAULT source: a generated secret (``RANDOM``), a free host
port (``PORT``), a backend-computed literal (``SYNTH`` — see
``Backend.synth_default``), or an environment variable ``$<PREFIX>__<NAME>``
(``ENV``). Each is overridable on the CLI with ``--<flag> VALUE`` (a literal) or
``--<flag>:env ENV_VAR`` (read from that env var); ``--env-prefix`` changes the
``<PREFIX>`` for the env-sourced ones.

:func:`resolve` turns the parsed args into ``(values, env_refs, literals)``:
``values`` is ``{META_KEY: concrete value}`` baked into the descriptor
(self-contained — only the crypto key is read at use time); ``env_refs`` is
``{META_KEY: ENV_VAR}`` for the keys whose value came from an env var, so
``manifest`` can re-emit the reference (``<flag>$: ENV_VAR``) instead of the
secret; ``literals`` is the list of keys given as a literal ``--<flag> VALUE`` (so
``manifest`` re-emits those, but never the auto-generated defaults a fresh create
re-derives — a free port, a generated password).

A required option left without a value does not stop the walk: every one of them
is collected and reported in a single error naming the env var and the flag that
would supply it, so a system configured entirely from the environment takes one
round trip to fix rather than one per variable.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

from tabsdatax.itest.tdksystems2 import constants
from tabsdatax.itest.tdksystems2.errors import TdxError

# default-source kinds
ENV = "env"  # default: $<PREFIX>__<NAME>
RANDOM = "random"  # default: a generated secret
PORT = "port"  # default: a free host port
SYNTH = "synth"  # default: a backend-computed literal (Backend.synth_default)


@dataclass(frozen=True)
class Option:
    name: str  # META key, e.g. "BUCKET" / "PORT" / "PASSWORD"
    flag: str  # CLI stem, e.g. "bucket"
    source: str  # ENV | RANDOM | PORT | SYNTH — how the default is produced
    default_doc: str  # human description of the default (shown in --help)
    secret: bool = False
    required: bool = True  # ENV options: error if the resolved env var is unset


def add_option_flags(
    parser: argparse.ArgumentParser, opts: tuple[Option, ...], env_prefix: str
) -> None:
    """Register ``--<flag>`` / ``--<flag>:env`` for each option (+ ``--env-prefix``
    when any option is env-sourced)."""
    for opt in opts:
        parser.add_argument(
            f"--{opt.flag}",
            dest=f"opt_{opt.name}",
            default=None,
            metavar="VALUE",
            help=f"default: {opt.default_doc}",
        )
        env_hint = f" (default ${env_prefix}__{opt.name})" if opt.source == ENV else ""
        parser.add_argument(
            f"--{opt.flag}:env",
            dest=f"env_{opt.name}",
            default=None,
            metavar="ENV_VAR",
            help=f"read --{opt.flag} from $ENV_VAR{env_hint}",
        )
    if any(o.source == ENV for o in opts):
        parser.add_argument(
            constants.OPT_ENV_PREFIX,
            dest="env_prefix",
            default=env_prefix,
            metavar="PREFIX",
            help=f"env prefix for env-sourced options (default: {env_prefix})",
        )


def _read_env(var: str) -> str:
    return os.environ.get(var, "").strip()


def _missing_error(backend, missing: list[tuple[str, Option]]) -> TdxError:
    """One error naming every option left without a value, and both ways to
    supply it. Reporting them one at a time costs a round trip per option, and a
    system whose whole connection comes from the environment has several."""
    width = max(len(var) for var, _ in missing)
    lines = "\n".join(
        f"  ${var.ljust(width)}   or  --{opt.flag} VALUE" for var, opt in missing
    )
    return TdxError(
        f"{backend.slug}: {len(missing)} required option(s) have no value — set the "
        f"env var or pass the flag:\n{lines}"
    )


def resolve(backend, ns: argparse.Namespace, name: str) -> tuple[dict, dict, list[str]]:
    """Resolve `backend`'s options from parsed args `ns` into
    ``(values, env_refs, literals)`` — a literal (``--flag``) wins, then a named
    env var (``--flag:env``), then the option's default source. ``env_refs`` maps
    the keys whose (non-empty) value came from an env var to that var; ``literals``
    lists the keys given a literal value."""
    prefix = getattr(ns, "env_prefix", None) or getattr(backend, "env_prefix", "") or ""
    values: dict[str, str] = {}
    env_refs: dict[str, str] = {}
    literals: list[str] = []
    missing: list[tuple[str, Option]] = []
    for opt in backend.options():
        lit = getattr(ns, f"opt_{opt.name}", None)
        env = getattr(ns, f"env_{opt.name}", None)
        if lit is not None and env is not None:
            raise TdxError(f"specify either --{opt.flag} or --{opt.flag}:env, not both")
        if lit is not None:
            values[opt.name] = lit
            literals.append(opt.name)
        elif env is not None:
            val = _read_env(env)
            values[opt.name] = val
            if val:
                env_refs[opt.name] = env
            elif opt.required:
                missing.append((env, opt))
        elif opt.source == ENV:
            var = f"{prefix}__{opt.name}"
            val = _read_env(var)
            values[opt.name] = val
            if val:
                env_refs[opt.name] = var
            elif opt.required:
                missing.append((var, opt))
        elif opt.source == RANDOM:
            values[opt.name] = backend.generate_password()
        elif opt.source == PORT:
            values[opt.name] = str(backend.next_free_port())
        elif opt.source == SYNTH:
            values[opt.name] = backend.synth_default(opt.name, name)
        else:  # pragma: no cover - guarded by the Option contract
            raise TdxError(f"option {opt.name}: unknown source {opt.source!r}")
    if missing:
        raise _missing_error(backend, missing)
    return values, env_refs, literals
