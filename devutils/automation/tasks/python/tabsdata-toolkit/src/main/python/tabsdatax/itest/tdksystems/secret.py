"""Secret providers for `tdx-system secrets --get <PROVIDER>:<REF>`.

A provider fetches a set of named secrets from an external store and returns
them as an ordered ``{<NAME>: <value>}`` dict, where each ``NAME`` follows the
``<ITEM>__<FIELD>`` convention `tdx-system create --env-prefix` reads. `--get`
selects a provider by scheme:

    1p:<VAULT>      1Password, via the `op` CLI  (see :mod:`.secret_1p`)

Adding a provider (ghsecrets, hashicorp, …) is just dropping a
``secret_<scheme>.py`` module that exposes ``fetch(ref) -> dict[str, str]``
and registering its scheme in ``_PROVIDERS`` below.
"""

from __future__ import annotations

import importlib
import json
import shlex

from .errors import DatagenError

# scheme -> module (relative to this package) implementing `fetch(ref)`.
_PROVIDERS: dict[str, str] = {
    "1p": "secret_1p",
}

_OUT_FORMATS = ("json", "export", "unset")

# Set alongside the fetched secrets: the distinct env prefixes (the `<ITEM>`
# part of each `<ITEM>__<FIELD>` name) that now have credentials. It names the
# systems the secrets populate — the same values `tdx-system create --auto` /
# `--env-prefix` use. Part of the `TD_BACKENDS_*` env family.
SYSTEMS_ENV = "TD_BACKENDS_SYSTEMS"


def _prefixes(secrets: dict[str, str]) -> list[str]:
    """Distinct `<ITEM>` prefixes (the part before the first `__`) across the
    fetched secret names, in first-seen order — the env prefixes the secrets
    populate. Ignores the synthetic SYSTEMS_ENV entry (so it stays idempotent)."""
    out: list[str] = []
    for name in secrets:
        if name == SYSTEMS_ENV:
            continue
        prefix = name.split("__", 1)[0]
        if prefix and prefix not in out:
            out.append(prefix)
    return out


def with_systems_var(secrets: dict[str, str]) -> dict[str, str]:
    """`secrets` plus a synthetic ``TD_BACKENDS_SYSTEMS`` entry listing the
    populated env prefixes (space-separated), so a consumer knows which systems
    have credentials set. Skipped when there are no prefixes; idempotent."""
    prefixes = _prefixes(secrets)
    if not prefixes:
        return secrets
    return {**secrets, SYSTEMS_ENV: " ".join(prefixes)}


def parse_ref(get: str) -> tuple[str, str]:
    """Split a `--get` value `<scheme>:<ref>` into (scheme, ref)."""
    scheme, sep, ref = get.partition(":")
    scheme, ref = scheme.strip(), ref.strip()
    if not sep or not scheme or not ref:
        raise DatagenError(
            f"--get expects <PROVIDER>:<REF> (e.g. 1p:dev_xp), got {get!r}"
        )
    return scheme, ref


def _load_provider(scheme: str):
    modname = _PROVIDERS.get(scheme)
    if modname is None:
        raise DatagenError(
            f"unknown secret provider {scheme!r} in --get "
            f"(supported: {', '.join(sorted(_PROVIDERS))})"
        )
    return importlib.import_module(f".{modname}", __package__)


def fetch(get: str) -> dict[str, str]:
    """Resolve `--get <PROVIDER>:<REF>` to an ordered dict of secret env vars.

    Dispatches to the provider module registered for `<PROVIDER>`; the ref is
    provider-specific (a vault name for `1p`, etc.)."""
    scheme, ref = parse_ref(get)
    return _load_provider(scheme).fetch(ref)


def render(secrets: dict[str, str], out: str) -> str:
    """Render fetched secrets as a JSON dict / `export` lines / `unset` lines.

    Names are already valid env-var names (the providers normalise them)."""
    if out == "json":
        return json.dumps(secrets, indent=2)
    if out == "export":
        return "\n".join(
            f"export {name}={shlex.quote(str(v))}" for name, v in secrets.items()
        )
    if out == "unset":
        return "\n".join(f"unset {name}" for name in secrets)
    raise DatagenError(
        f"unknown --out format {out!r} (expected {'|'.join(_OUT_FORMATS)})"
    )
