"""1Password secret provider for `tdx-system secrets --get 1p:<VAULT>`.

`fetch(vault)` reads every item in VAULT via the `op` CLI and returns an
ordered ``{<ITEM_TITLE>__<FIELD_ID>: <value>}`` dict for every non-null,
non-`expires` field. Names are normalised to valid env-var names, matching
the ``<ITEM>__<FIELD>`` convention `tdx-system create --env-prefix` reads.
Prints nothing itself — the caller renders the result to stdout.
"""
from __future__ import annotations

import json
import subprocess

from .errors import DatagenError


def _norm(s: str) -> str:
    """Uppercase, map non-[A-Z0-9_] to '_', and prefix a leading digit with
    '_' so the result is a valid env-var name (same rules as the old
    `1p.sh` __1p_norm)."""
    out = "".join(c if c.isalnum() or c == "_" else "_" for c in s.upper())
    if out and out[0].isdigit():
        out = "_" + out
    return out


def _require(cmd: str) -> None:
    if subprocess.run(["which", cmd], capture_output=True).returncode != 0:
        raise DatagenError(f"'{cmd}' not found in PATH")


def fetch(vault: str) -> dict[str, str]:
    """Read every item/field in `vault` via `op`, returning an ordered
    ``{<ITEM>__<FIELD>: value}`` dict. Skips null values, `expires` fields,
    and any item that can't be read. Prints nothing."""
    _require("op")
    cp = subprocess.run(
        ["op", "--vault", vault, "item", "list", "--format=json"],
        capture_output=True, text=True,
    )
    if cp.returncode != 0:
        raise DatagenError(
            f"failed to list items in vault '{vault}': "
            f"{cp.stderr.strip() or cp.stdout.strip()}"
        )
    items = json.loads(cp.stdout) if cp.stdout.strip() else []
    if not items:
        raise DatagenError(f"vault '{vault}' returned no items")

    secrets: dict[str, str] = {}
    for it in items:
        title = it.get("title", "")
        if not title:
            continue
        cp = subprocess.run(
            ["op", "--vault", vault, "item", "get", title, "--format=json"],
            capture_output=True, text=True,
        )
        if cp.returncode != 0:
            continue
        data = json.loads(cp.stdout)
        title_norm = _norm(data.get("title", title))
        for field in data.get("fields", []) or []:
            fid = field.get("id", "")
            if not fid or fid == "expires":
                continue
            value = field.get("value")
            if value is None:
                continue
            secrets[f"{title_norm}__{_norm(fid)}"] = str(value)
    return secrets
