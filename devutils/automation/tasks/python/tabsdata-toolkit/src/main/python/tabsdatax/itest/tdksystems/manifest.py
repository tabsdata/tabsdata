#
# Copyright 2026 Tabsdata Inc.
#

"""YAML manifests for batch `tdx-system` create.

A manifest is a YAML **list** of entries; ``name`` is the system's file name and
``type`` is the system type, and the remaining keys mirror the CLI flags of
`tdx-system create` (so a manifest is just its invocations written down):

    - a scalar maps to ``--flag value``          (``bucket: my-bucket``)
    - ``true`` maps to the bare flag             (``force: true`` -> ``--force``)
    - a ``{env: VAR}`` mapping maps to ``--env:flag VAR``
    - omit a setting to fall back to ``$PREFIX__OPT`` exactly like the CLI

Each CLI names its manifest with one flag — `tdx-system` via ``--sys-manifest
FILE|-`` and `tdx-domain` via ``--domain-manifest FILE|-`` (``-`` is stdin, or
stdout when writing) — used by three verbs: ``dump`` writes a manifest of every
system / domain in the store, and ``create`` / ``destroy`` replay it
(continue-on-error, one per-entry report). ``create --sync`` instead reconciles
the store to the manifest — creating the entries the store lacks and destroying
the systems / domains the manifest omits, never updating the ones in both. A
SYSTEM entry is
``{name, type, <settings>}``; a DOMAIN entry is ``{name, sys, model?, data?}``
(its keys mirror `tdx-domain create` flags). The domain entity prefix is
deliberately NOT recorded, so a domain manifest replays under each user's own
``<user>_<build>_`` prefix.
"""

from __future__ import annotations

import sys
from pathlib import Path

from .errors import DatagenError


def read(path: str) -> list[dict]:
    """Load a manifest into a list of entry mappings (``[]`` if empty). ``-`` reads
    the manifest from stdin."""
    import yaml

    if path == "-":
        text = sys.stdin.read()
    else:
        try:
            text = Path(path).read_text(encoding="utf-8")
        except OSError as exc:
            raise DatagenError(f"cannot read manifest {path}: {exc}")
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise DatagenError(f"manifest {path} is not valid YAML: {exc}")
    if data is None:
        return []
    if not isinstance(data, list) or not all(isinstance(e, dict) for e in data):
        raise DatagenError(
            f"manifest {path} must be a YAML list of entries (one mapping per "
            f"system/domain)"
        )
    return data


def entry_to_argv(entry: dict) -> list[str]:
    """Turn one manifest entry into the `create` argv it mirrors."""
    argv: list[str] = []
    for key, val in entry.items():
        if not isinstance(key, str):
            raise DatagenError(f"manifest entry has a non-string key: {key!r}")
        if val is None:
            continue
        if isinstance(val, bool):
            if val:
                argv.append(f"--{key}")
            continue
        if isinstance(val, dict):
            if set(val) != {"env"}:
                raise DatagenError(
                    f"manifest key {key!r} must map to a scalar, true, or "
                    f"{{env: VAR}} (got {val!r})"
                )
            argv += [f"--env:{key}", str(val["env"])]
            continue
        argv += [f"--{key}", str(val)]
    return argv


def write(dest: str, entries: list[dict]) -> None:
    """Write `entries` as a YAML manifest to `dest` (``-`` = stdout)."""
    import yaml

    text = yaml.safe_dump(entries, sort_keys=False, default_flow_style=False)
    if dest == "-":
        sys.stdout.write(text)
        return
    p = Path(dest)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")
    n = len(entries)
    print(
        f">>> wrote manifest ({n} entr{'y' if n == 1 else 'ies'}) to {dest}",
        file=sys.stderr,
    )


def system_entry_from_descriptor(doc: dict) -> dict:
    """The manifest entry that reproduces an existing system from its descriptor:
    ``type`` + ``name`` (the system file name) plus each declared option, as a
    literal or a ``{env: VAR}`` reference (preserved as-is, so secrets stay
    references and are never resolved). Container knobs (port / password /
    container name) are auto-generated on create, so they are intentionally
    omitted."""
    from . import systems

    sysobj = systems.get(doc["system"])
    entry: dict = {"name": doc.get("name", ""), "type": doc["system"]}
    raw = {e["name"]: e for e in doc.get("meta", [])}
    for opt in sysobj._all_options():
        e = raw.get(opt.name)
        if e is None:
            continue
        if "env" in e:
            entry[opt.cli_flag] = {"env": e["env"]}
        elif e.get("value") not in (None, ""):
            entry[opt.cli_flag] = e["value"]
    return entry


def domain_entry_from_descriptor(doc: dict) -> dict:
    """The manifest entry that reproduces an existing domain from its descriptor:
    the ``name`` and its ``sys`` (system name), plus ``model`` / ``data`` when
    seeded. The entity prefix is intentionally left out, so a replay creates the
    external entity under the *replaying* run's own ``<user>_<build>_`` prefix."""
    entry: dict = {"name": doc.get("name", ""), "sys": doc.get("sys_name", "")}
    if doc.get("model"):
        entry["model"] = doc["model"]
    if doc.get("data"):
        entry["data"] = doc["data"]
    return entry
