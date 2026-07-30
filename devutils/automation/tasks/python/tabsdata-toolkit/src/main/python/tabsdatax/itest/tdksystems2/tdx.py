#
# Copyright 2026 Tabsdata Inc.
#

"""tdx-system / tdx-domain — modular manager for external systems and domains.

    tdx-system types                       # list system types + envs + images
    tdx-system list [--sys-dir DIR]        # systems in the store
    tdx-system manifest [--out FILE]       # YAML manifest of the store
    tdx-system info --sys NAME|list|ALL | --manifest FILE [--format ...] [--prefix P]
    tdx-system create  --type TYPE [--sys NAME] [--sys-prefix P] [--ports P,..]
    tdx-system create  --type ALL|t1,t2 [--sys-prefix P]   # one per type
                                          # --sys defaults to TYPE; [type options] too
    tdx-system destroy --sys NAME|s1,s2|ALL | --manifest FILE [-f] [--dry-run]
    tdx-system status  --sys NAME|list|ALL | --manifest FILE
    tdx-system purge   --sys NAME|list|ALL [--external-prefix PREFIX] [-f]
    tdx-system purge   --manifest FILE [--external-prefix PREFIX] [-f]
    tdx-system restart --sys NAME|list|ALL | --manifest FILE
    tdx-system fetch-bin [--type TYPE|t1,t2|ALL]
    tdx-system connect --remote-sys [user@]HOST:PATH --remote-secret-env VAR \\
                       --sys NAME [--ssh-port N] [--identity KEYFILE] [-f]
                                          # ssh-tunnel a remote container system;
                                          # destroy closes the tunnel
    tdx-system secrets --set 1p:<vault> [--out json|export|unset]  # fetch creds
    tdx-system clear-venvs                 # delete the managed venvs

    tdx-domain list [--domain-dir DIR]     # domains in the store
    tdx-domain manifest [--out FILE]       # YAML manifest of the store
    tdx-domain info --domain NAME|list|ALL | --manifest FILE [--format ...]
    tdx-domain create      --sys NAME [--domain NAME] [--domain-prefix P] [-f]
    tdx-domain create      --sys ALL|s1,s2 [--domain-prefix P]  # one domain / system
    tdx-domain destroy     --domain NAME|d1,d2|ALL [-f] [--dry-run]
    tdx-domain status      --domain NAME
    tdx-domain models      --domain NAME       # models available for its system
    tdx-domain create-model --domain NAME --model MODEL [--out json]
    tdx-domain create-data  --domain NAME --size N [--out json]  # N = mult (1=base)
    tdx-domain modify-data  --domain NAME --entity NAME|ALL --sets N \\
                             --ops Insert|Update|Delete|Mix --seed N|AUTO [--out json]

``--out json`` on the data verbs prints ``{entity: {insert, update, delete}}`` to
stdout (nothing else) and routes progress to the log; omit it for human output.

Batch: the multi-target selector is ``--type`` for a system ``create`` (one
system per type, named after it), ``--sys`` for a domain ``create`` (one domain
per system, named after it), and ``--sys`` / ``--domain`` for every other verb
(every stored system / domain). Each accepts a single name, a comma list, or
``ALL``; ``--sys-prefix P`` (system create) / ``--domain-prefix P`` (domain
create) prepends ``P_`` to the created name(s), for one, many or ``ALL``.
Multiple targets run in parallel, heaviest ``weight`` first
(``$TD_BACKENDS_PARALLELISM`` bounds concurrency), each in its own child process.
Before launching, the dispatcher pre-finds a *disjoint* pool of free ports per
system and passes it (``--ports``) so no two parallel creates race for the same
port (a single ``create`` accepts ``--ports`` too, else it finds ports as needed).
``--fail-fast`` stops on the first failure and ``--report FILE`` writes a JSON
run record. ``create`` /
``destroy`` accept ``--dry-run`` (print what would happen — for a forced system
destroy, the domains that would be destroyed too). ``create --manifest FILE``
replays a YAML manifest in parallel — one system ``create`` each, or one
``create`` -> ``create-model`` -> ``create-data`` chain per domain — and
``--sync`` reconciles the store to it.

Every command re-execs into one of the managed uv venvs before it runs (see
``_venv``): a system declares the env its outside (``sys_env``) and its inside
(``dom_env``) need, ``types`` shows both, and ``clear-venvs`` deletes them all.

The common verbs (``types`` / ``list`` / ``info``) are handled here; everything
else is *delegated* to the matching command module discovered on disk —
``systems/<slug>/<verb>.py`` for system verbs, ``systems/<slug>/domain/<verb>.py``
for domain verbs. The available commands and their help come straight from those
files. A model is available for a system when its assets are present under
``systems/<slug>/domain/models/<model>/`` — DDL for a SQL system, a snowfakery
recipe and load mapping for the salesforce org. A verb exists when *some* system
exposes it, so one aimed at a system that does not (``restart`` on an object
store, ``modify-data`` on an append-only topic) is rejected naming what that
system does support.

Descriptors are addressed by *name* (a file in the store dir) and stored
encrypted. The long-running verbs accept ``--latch FILE`` to be waitable in the
background (write ``FILE.ok``/``FILE.err`` then touch ``FILE``); their chatty
detail goes to a ``/tmp/tdx-logs/<name>-<timestamp>.log`` while the console shows
brief, colored progress, and each run first deletes logs there older than a day.
Top-level ``-h`` lists the environment variables the tools read and their
defaults (see ``constants.ENV_VARS``).
"""

from __future__ import annotations

import sys
from importlib import import_module
from pathlib import Path
from types import ModuleType

from tabsdatax.itest.tdksystems2 import (
    _venv,
    batch,
    cli,
    console,
    constants,
    descriptor,
    manifest,
    store,
    systems,
)
from tabsdatax.itest.tdksystems2.errors import TdxError, run
from tabsdatax.itest.tdksystems2.shell import ArgumentParser

# Verbs whose chatty detail is redirected to a /tmp log while the console shows
# brief progress.
_LOGGED_VERBS = {
    constants.V_CREATE,
    constants.V_DESTROY,
    constants.V_PURGE,
    constants.V_STATUS,
    constants.V_RESTART,
    constants.V_CREATE_MODEL,
    constants.V_CREATE_DATA,
    constants.V_MODIFY_DATA,
}
# Verbs that accept ``--latch FILE`` (the long-running ones).
_LATCH_VERBS = {
    constants.V_CREATE,
    constants.V_DESTROY,
    constants.V_PURGE,
    constants.V_CREATE_MODEL,
    constants.V_CREATE_DATA,
    constants.V_MODIFY_DATA,
}

# ======================================================================
# Command discovery / loading — a shared catalog gated by capability flags
# ======================================================================

# verb -> (shared command module under _common/commands, capability flag or
# None). A system exposes a verb iff the flag is truthy on its backend class; it
# may override the shared module with its own systems/<slug>[/domain]/<verb>.py.
_SYS_CATALOG: dict[str, tuple[str, str | None]] = {
    constants.V_CREATE: ("sys_create", None),
    constants.V_DESTROY: ("sys_destroy", None),
    constants.V_STATUS: ("sys_status", None),
    constants.V_PURGE: ("sys_purge", "supports_purge"),
    constants.V_RESTART: ("sys_restart", "is_container"),
    constants.V_CONNECT: ("sys_connect", "is_container"),
    # Ungated on purpose: `fetch-bin --type ALL` is the handy way to pull every
    # image at once, so a system with none warns and is skipped rather than
    # failing the run.
    constants.V_FETCH_BIN: ("sys_fetch_bin", None),
}
_DOM_CATALOG: dict[str, tuple[str, str | None]] = {
    constants.V_CREATE: ("dom_create", None),
    constants.V_DESTROY: ("dom_destroy", None),
    constants.V_STATUS: ("dom_status", None),
    constants.V_MODELS: ("dom_models", "supports_models"),
    constants.V_CREATE_MODEL: ("dom_create_model", "supports_models"),
    constants.V_CREATE_DATA: ("dom_create_data", "supports_data"),
    constants.V_MODIFY_DATA: ("dom_modify_data", "supports_changes"),
}

_COMMANDS_PKG = f"{systems.__name__}._common.commands"


def _verb_module(verb: str) -> str:
    return verb.replace("-", "_")


def _catalog(tool: str) -> dict[str, tuple[str, str | None]]:
    return _DOM_CATALOG if tool == constants.DOM_TOOL else _SYS_CATALOG


def _has_verb(cls: type, cap: str | None) -> bool:
    return cap is None or bool(getattr(cls, cap, False))


def _system_verbs(tool: str, slug: str) -> list[str]:
    """The verbs `slug` exposes — those whose capability flag it sets."""
    cls = systems.get_class(slug)
    return [v for v, (_, cap) in _catalog(tool).items() if _has_verb(cls, cap)]


def _require_verb(tool: str, slug: str, verb: str) -> None:
    """Reject a verb the target system does not expose.

    ``_discover`` gates the tool's *vocabulary* — a verb exists when some system
    exposes it — which leaves the per-system half: without this, a verb reaches
    the shared command for a backend that never implements the capability behind
    it, and the run dies somewhere inside on a missing attribute instead of
    saying so up front."""
    _, cap = _catalog(tool)[verb]
    if _has_verb(systems.get_class(slug), cap):
        return
    raise TdxError(
        f"{tool}: system '{slug}' does not support '{verb}' "
        f"(it supports: {', '.join(_system_verbs(tool, slug))})"
    )


def _summary(module: ModuleType) -> str:
    doc = (module.__doc__ or "").strip()
    return doc.splitlines()[0] if doc else ""


def _command_module(tool: str, slug: str, verb: str) -> ModuleType:
    """The command module for (tool, slug, verb): the system's own override if it
    ships one, else the shared implementation in ``_common/commands``."""
    catalog = _catalog(tool)
    if verb not in catalog:
        raise TdxError(f"{tool}: system '{slug}' has no '{verb}' command")
    shared_name, _ = catalog[verb]
    base = f"{systems.__name__}.{slug}"
    if tool == constants.DOM_TOOL:
        base += ".domain"
    for name in (f"{base}.{_verb_module(verb)}", f"{_COMMANDS_PKG}.{shared_name}"):
        try:
            module = import_module(name)
        except ModuleNotFoundError:
            continue
        if hasattr(module, "run") and hasattr(module, "add_arguments"):
            return module
    raise TdxError(f"{tool}: '{verb}' has no command module")


def _discover(tool: str) -> dict[str, str]:
    """Verb -> one-line help for the verbs some system exposes (a verb is exposed
    when its capability flag is set on the backend; the first exposing system
    supplies the help line)."""
    catalog = _catalog(tool)
    out: dict[str, str] = {}
    for slug in systems.all_slugs():
        cls = systems.get_class(slug)
        for verb, (_, cap) in catalog.items():
            if verb in out or not _has_verb(cls, cap):
                continue
            out[verb] = _summary(_command_module(tool, slug, verb))
    return out


def _default_slug(tool: str, verb: str) -> str:
    catalog = _catalog(tool)
    if verb not in catalog:
        raise TdxError(f"{tool}: unknown command '{verb}'")
    _, cap = catalog[verb]
    for slug in systems.all_slugs():
        if _has_verb(systems.get_class(slug), cap):
            return slug
    raise TdxError(f"{tool}: no system provides '{verb}'")


def _sole_slug() -> str:
    slugs = systems.all_slugs()
    if len(slugs) == 1:
        return slugs[0]
    raise TdxError(f"{constants.OPT_TYPE} is required (one of: {', '.join(slugs)})")


def _system_or_none(slug: str | None):
    """The backend instance for `slug`, or None if there is none to resolve (used
    to hand the options-driven create its backend for building its `-h` flags)."""
    if not slug:
        return None
    try:
        return systems.get(slug)
    except TdxError:
        return None


# ======================================================================
# Descriptor resolution
# ======================================================================


def _load_desc(
    path: Path, role: str, name: str
) -> descriptor.SystemInfo | descriptor.DomainInfo:
    if not descriptor.is_descriptor(path):
        raise TdxError(f"no {role} named '{name}' in the store ({path})")
    info = descriptor.load(path)
    if info.role != role:
        raise TdxError(f"'{name}' is not a {role} descriptor")
    return info


def _resolve(
    tool: str, verb: str, rest: list[str]
) -> tuple[
    str,
    str | None,
    descriptor.SystemInfo | descriptor.DomainInfo | None,
    Path | None,
    Path,
]:
    """Pick (slug, name, info, path, store_dir) for a delegated verb, peeking raw
    argv for the target before argparse runs."""
    sys_dir = cli.peek(rest, constants.OPT_SYS_DIR)
    dom_dir = cli.peek(rest, constants.OPT_DOMAIN_DIR)
    store_dir = (
        store.domain_dir(dom_dir)
        if tool == constants.DOM_TOOL
        else store.system_dir(sys_dir)
    )

    if tool == constants.SYS_TOOL:
        if verb == constants.V_CONNECT:
            # The type is whatever the remote descriptor says, which is only known
            # once connect has fetched it over ssh — so it resolves its own
            # backend and this is just a stand-in to dispatch on.
            return _default_slug(tool, verb), None, None, None, store_dir
        if verb in (constants.V_CREATE, constants.V_FETCH_BIN):
            slug = cli.peek(rest, constants.OPT_TYPE) or _sole_slug()
            if verb == constants.V_FETCH_BIN and (
                slug.upper() == constants.TARGET_ALL or "," in slug
            ):
                # a stand-in target; fetch-bin's run() re-derives the full set
                # from --type ALL / the comma list itself.
                slug = _default_slug(tool, verb)
            return slug, None, None, None, store_dir
        name = cli.peek(rest, constants.OPT_SYS)
        if not name:
            raise TdxError(f"{tool} {verb}: {constants.OPT_SYS} NAME is required")
        path = store.system_path(name, sys_dir)
        info = _load_desc(path, constants.ROLE_SYSTEM, name)
        return info.system, name, info, path, store_dir

    # domain tool
    if verb == constants.V_CREATE:
        sname = cli.peek(rest, constants.OPT_SYS)
        if not sname:
            raise TdxError(f"{tool} create: {constants.OPT_SYS} NAME is required")
        sinfo = _load_desc(
            store.system_path(sname, sys_dir), constants.ROLE_SYSTEM, sname
        )
        return sinfo.system, None, sinfo, None, store_dir
    name = cli.peek(rest, constants.OPT_DOMAIN)
    if not name:
        raise TdxError(f"{tool} {verb}: {constants.OPT_DOMAIN} NAME is required")
    path = store.domain_path(name, dom_dir)
    info = _load_desc(path, constants.ROLE_DOMAIN, name)
    return info.system, name, info, path, store_dir


# ======================================================================
# Common verbs (handled here, not delegated)
# ======================================================================


def _store_infos(
    store_dir: Path, role: str
) -> list[descriptor.SystemInfo | descriptor.DomainInfo]:
    """The descriptors of `role` in `store_dir`, sorted by file name, skipping
    non-descriptors, unreadable files and mismatched roles."""
    if not store_dir.is_dir():
        return []
    out: list[descriptor.SystemInfo | descriptor.DomainInfo] = []
    for name in sorted(p.name for p in store_dir.iterdir() if p.is_file()):
        path = store_dir / name
        if not descriptor.is_descriptor(path):
            continue
        try:
            info = descriptor.load(path)
        except TdxError:
            continue
        if info.role == role:
            out.append(info)
    return out


def _list_store(store_dir: Path, role: str) -> None:
    if not store_dir.is_dir():
        console.out(f"(no {role}s: {store_dir} does not exist)")
        return
    shown = 0
    for name in sorted(p.name for p in store_dir.iterdir() if p.is_file()):
        path = store_dir / name
        if not descriptor.is_descriptor(path):
            continue
        try:
            info = descriptor.load(path)
        except TdxError as exc:
            console.warn(f"  {name:20} <unreadable: {exc}>")
            continue
        if info.role != role:
            continue
        extra = ""
        if isinstance(info, descriptor.DomainInfo):
            extra = f"  model={info.model or '-'} data={info.data or '-'}"
        console.out(f"  {name:20} {info.system:10}{extra}")
        shown += 1
    if not shown:
        console.out(f"(no {role}s in {store_dir})")


def _cmn_types(tool: str, rest: list[str]) -> int:
    ArgumentParser(prog=f"{tool} {constants.V_TYPES}").parse_args(rest)
    rows = [("TYPE", "LABEL", "WEIGHT", "SYS-ENV", "DOM-ENV", "IMAGE(S)")]
    for slug in systems.all_slugs():
        sysobj = systems.get(slug)
        imgs = ", ".join(i for i, _ in sysobj.docker_images()) or "-"
        rows.append((
            slug,
            str(getattr(sysobj, "label", slug)),
            str(getattr(sysobj, "weight", 1)),
            _venv.venv_name(sysobj.sys_env),
            _venv.venv_name(sysobj.dom_env),
            imgs,
        ))
    widths = [max(len(r[i]) for r in rows) for i in range(5)]  # last col unpadded
    for type_, label, weight, sys_env, dom_env, imgs in rows:
        console.out(
            f"  {type_:<{widths[0]}}  {label:<{widths[1]}}  "
            f"{weight:<{widths[2]}}  {sys_env:<{widths[3]}}  "
            f"{dom_env:<{widths[4]}}  {imgs}"
        )
    return 0


def _cmn_clear_venvs(tool: str, rest: list[str]) -> int:
    ArgumentParser(prog=f"{tool} {constants.V_CLEAR_VENVS}").parse_args(rest)
    return _venv.clear_venvs()


def _cmn_sys_list(tool: str, rest: list[str]) -> int:
    p = ArgumentParser(prog=f"{tool} {constants.V_LIST}")
    cli.add_sys_dir(p)
    a = p.parse_args(rest)
    _list_store(store.system_dir(a.sys_dir), constants.ROLE_SYSTEM)
    return 0


def _add_info_manifest(p: ArgumentParser) -> None:
    p.add_argument(
        constants.OPT_MANIFEST,
        default=None,
        metavar="FILE",
        help="show every system/domain the YAML manifest names (- = stdin)",
    )


def _info_targets(
    tool: str, target: str | None, manifest_file: str | None, dir_opt: str | None
) -> list[str]:
    """The names an ``info`` invocation should render: the manifest's entries, or
    a single / comma-listed target, or ``ALL`` stored names."""
    role = (
        constants.ROLE_DOMAIN if tool == constants.DOM_TOOL else constants.ROLE_SYSTEM
    )
    store_dir = (
        store.domain_dir(dir_opt)
        if tool == constants.DOM_TOOL
        else store.system_dir(dir_opt)
    )
    if manifest_file:
        entries = manifest.read(manifest_file)
        return [str(e["name"]) for e in entries if e.get("name")]
    selector = constants.OPT_DOMAIN if tool == constants.DOM_TOOL else constants.OPT_SYS
    if not target:
        raise TdxError(
            f"{tool} {constants.V_INFO}: {selector} NAME|list|ALL "
            f"or {constants.OPT_MANIFEST} FILE is required"
        )
    names = [n.strip() for n in target.split(",") if n.strip()]
    if len(names) == 1 and names[0].upper() == constants.TARGET_ALL:
        return [i.name for i in _store_infos(store_dir, role)]
    return names


def _render_infos(
    tool: str, names: list[str], dir_opt: str | None, fmt: str, prefix: str | None
) -> int:
    role = (
        constants.ROLE_DOMAIN if tool == constants.DOM_TOOL else constants.ROLE_SYSTEM
    )
    path_fn = store.domain_path if tool == constants.DOM_TOOL else store.system_path
    chunks = [
        descriptor.info_render(
            _load_desc(path_fn(name, dir_opt), role, name), fmt, prefix
        )
        for name in names
    ]
    console.out("\n".join(chunks))
    return 0


def _cmn_sys_info(tool: str, rest: list[str]) -> int:
    p = ArgumentParser(prog=f"{tool} {constants.V_INFO}")
    cli.add_sys(p, required=False)
    cli.add_sys_dir(p)
    _add_info_manifest(p)
    cli.add_info_format(p)
    a = p.parse_args(rest)
    names = _info_targets(tool, a.sys, a.manifest, a.sys_dir)
    return _render_infos(tool, names, a.sys_dir, a.format, a.prefix)


def _cmn_dom_list(tool: str, rest: list[str]) -> int:
    p = ArgumentParser(prog=f"{tool} {constants.V_LIST}")
    cli.add_domain_dir(p)
    a = p.parse_args(rest)
    _list_store(store.domain_dir(a.domain_dir), constants.ROLE_DOMAIN)
    return 0


def _cmn_dom_info(tool: str, rest: list[str]) -> int:
    p = ArgumentParser(prog=f"{tool} {constants.V_INFO}")
    cli.add_domain(p, required=False)
    cli.add_domain_dir(p)
    _add_info_manifest(p)
    cli.add_info_format(p)
    a = p.parse_args(rest)
    names = _info_targets(tool, a.domain, a.manifest, a.domain_dir)
    return _render_infos(tool, names, a.domain_dir, a.format, a.prefix)


def _add_manifest_out(p: ArgumentParser) -> None:
    p.add_argument(
        constants.OPT_OUT,
        default="-",
        metavar="FILE",
        help="write the manifest to FILE (default: - = stdout)",
    )


def _cmn_sys_manifest(tool: str, rest: list[str]) -> int:
    p = ArgumentParser(prog=f"{tool} {constants.V_MANIFEST}")
    cli.add_sys_dir(p)
    _add_manifest_out(p)
    a = p.parse_args(rest)
    infos = _store_infos(store.system_dir(a.sys_dir), constants.ROLE_SYSTEM)
    manifest.write(a.out, [manifest.system_entry(i) for i in infos])  # type: ignore[arg-type]
    return 0


def _cmn_dom_manifest(tool: str, rest: list[str]) -> int:
    p = ArgumentParser(prog=f"{tool} {constants.V_MANIFEST}")
    cli.add_domain_dir(p)
    _add_manifest_out(p)
    a = p.parse_args(rest)
    infos = _store_infos(store.domain_dir(a.domain_dir), constants.ROLE_DOMAIN)
    manifest.write(a.out, [manifest.domain_entry(i) for i in infos])  # type: ignore[arg-type]
    return 0


def _cmn_secrets(tool: str, rest: list[str]) -> int:
    from tabsdatax.itest.tdksystems2 import secret

    p = ArgumentParser(
        prog=f"{tool} {constants.V_SECRETS}",
        description="fetch a provider's secrets as the systems' env vars and "
        "print them (source the export/unset form to (un)set your shell)",
    )
    p.add_argument(
        constants.OPT_SET,
        dest="ref",
        required=True,
        metavar="PROVIDER:REF",
        help="provider ref, e.g. 1p:<vault> (1Password via the `op` CLI)",
    )
    p.add_argument(
        constants.OPT_OUT,
        dest="out",
        default=secret.OUT_FORMATS[0],
        choices=list(secret.OUT_FORMATS),
        help=f"output format (default: {secret.OUT_FORMATS[0]})",
    )
    a = p.parse_args(rest)
    secrets = secret.with_systems_var(secret.fetch(a.ref))
    console.out(secret.render(secrets, a.out))
    return 0


_COMMON = {
    constants.SYS_TOOL: {
        constants.V_TYPES: _cmn_types,
        constants.V_LIST: _cmn_sys_list,
        constants.V_INFO: _cmn_sys_info,
        constants.V_MANIFEST: _cmn_sys_manifest,
        constants.V_SECRETS: _cmn_secrets,
        constants.V_CLEAR_VENVS: _cmn_clear_venvs,
    },
    constants.DOM_TOOL: {
        constants.V_LIST: _cmn_dom_list,
        constants.V_INFO: _cmn_dom_info,
        constants.V_MANIFEST: _cmn_dom_manifest,
    },
}


# ======================================================================
# Batch create / destroy (operate across the whole store, not one slug)
# ======================================================================


_MANIFEST_KEYS = {"name", "type", "sys", "model", "size"}


def _entry_option_argv(entry: dict) -> list[str]:
    """Turn a manifest entry's option keys into create flags: an env reference is
    ``flag$: VAR`` -> ``--flag:env VAR`` (the ``$`` suffix marks the env ref); a
    literal is ``flag: value`` -> ``--flag value``."""
    argv: list[str] = []
    for key, val in entry.items():
        if key in _MANIFEST_KEYS:
            continue
        if key.endswith("$"):
            argv += [f"--{key[:-1]}:env", str(val)]
        else:
            argv += [f"--{key}", str(val)]
    return argv


def _store_dir_argv(tool: str, store_dir_opt: str | None) -> list[str]:
    """``--sys-dir DIR`` / ``--domain-dir DIR`` for a child invocation. A manifest
    run reads the store the caller named, so its children have to write to that
    same one rather than falling back to the default."""
    if not store_dir_opt:
        return []
    flag = (
        constants.OPT_DOMAIN_DIR
        if tool == constants.DOM_TOOL
        else constants.OPT_SYS_DIR
    )
    return [flag, store_dir_opt]


def _entry_steps(
    tool: str,
    entry: dict,
    *,
    force: bool,
    ports: list[int] | None,
    store_dir_opt: str | None = None,
) -> list[list[str]]:
    """The child invocation(s) that reproduce a manifest entry: a system is one
    ``create``; a domain is a ``create`` -> ``create-model`` -> ``create-data``
    chain (the last two only when the entry records a model / data)."""
    if "name" not in entry:
        raise TdxError(f"manifest entry has no 'name': {entry}")
    name = str(entry["name"])
    f = [constants.OPT_FORCE_SHORT] if force else []
    d = _store_dir_argv(tool, store_dir_opt)
    if tool == constants.SYS_TOOL:
        p = [constants.OPT_PORTS, ",".join(str(x) for x in ports)] if ports else []
        return [
            [
                constants.V_CREATE,
                constants.OPT_TYPE,
                str(entry["type"]),
                constants.OPT_SYS,
                name,
                *p,
                *d,
                *f,
                *_entry_option_argv(entry),
            ]
        ]
    steps = [
        [
            constants.V_CREATE,
            constants.OPT_SYS,
            str(entry["sys"]),
            constants.OPT_DOMAIN,
            name,
            *d,
            *f,
        ]
    ]
    if entry.get("model"):
        steps.append([
            constants.V_CREATE_MODEL,
            constants.OPT_DOMAIN,
            name,
            constants.OPT_MODEL,
            str(entry["model"]),
            *d,
            *f,
        ])
    if entry.get("size"):
        steps.append([
            constants.V_CREATE_DATA,
            constants.OPT_DOMAIN,
            name,
            constants.OPT_SIZE,
            str(entry["size"]),
            *d,
            *f,
        ])
    return steps


def _entry_weight(tool: str, entry: dict) -> int:
    if tool == constants.SYS_TOOL:
        return batch.weight_for(str(entry.get("type", "")))
    return batch.weight_of_stored(constants.ROLE_SYSTEM, str(entry.get("sys", "")))


def _store_names(tool: str, store_dir_opt: str | None) -> set[str]:
    if tool == constants.SYS_TOOL:
        return {
            i.name
            for i in _store_infos(
                store.system_dir(store_dir_opt), constants.ROLE_SYSTEM
            )
        }
    return {
        i.name
        for i in _store_infos(store.domain_dir(store_dir_opt), constants.ROLE_DOMAIN)
    }


def _create_from_manifest(tool: str, rest: list[str]) -> int:
    """``create --manifest FILE`` — create every entry in parallel (each a create
    [-> create-model -> create-data] chain), heaviest first, continue-on-error.
    With ``--sync`` also destroy the store entries the manifest omits (reconcile).
    A system create gets its own disjoint pool of candidate ports (``--ports``)."""
    p = ArgumentParser(prog=f"{tool} {constants.V_CREATE}")
    p.add_argument(
        constants.OPT_MANIFEST,
        required=True,
        metavar="FILE",
        help="create every entry in the YAML manifest (- = stdin)",
    )
    p.add_argument(
        constants.OPT_SYNC,
        action="store_true",
        help="reconcile: also destroy store entries the manifest omits",
    )
    cli.add_force(p)
    dir_opt = _add_store_dir(tool, p)
    a = p.parse_args(rest)
    store_dir_opt = getattr(a, dir_opt)
    entries = manifest.read(a.manifest)
    existing = _store_names(tool, store_dir_opt)
    manifest_names = {str(e["name"]) for e in entries if "name" in e}

    to_create: list[dict] = []
    unchanged = 0
    for entry in entries:
        name = str(entry.get("name", ""))
        if name in existing and a.sync and not a.force:
            console.out(f"  = {name} (present, left unchanged)")
            unchanged += 1
        else:
            to_create.append(entry)

    pools = (
        batch.port_pools(len(to_create))
        if tool == constants.SYS_TOOL
        else [None] * len(to_create)
    )
    tasks: list[batch.Task] = []
    build_failed = 0
    for entry, pool in zip(to_create, pools):
        try:
            steps = _entry_steps(
                tool, entry, force=a.force, ports=pool, store_dir_opt=store_dir_opt
            )
        except (TdxError, KeyError) as exc:
            console.bad(f"  ! {entry.get('name', entry)}: {exc}")
            build_failed += 1
            continue
        tasks.append(batch.Task(str(entry["name"]), _entry_weight(tool, entry), steps))

    rc = batch.execute(tool, constants.V_CREATE, tasks) if tasks else 0
    if a.sync:
        omitted = sorted(existing - manifest_names)
        if omitted:
            selector = (
                constants.OPT_DOMAIN
                if tool == constants.DOM_TOOL
                else constants.OPT_SYS
            )
            rc = (
                batch.run(
                    tool,
                    constants.V_DESTROY,
                    [
                        selector,
                        ",".join(omitted),
                        *_store_dir_argv(tool, store_dir_opt),
                        constants.OPT_FORCE_SHORT,
                    ],
                )
                or rc
            )
    if unchanged:
        console.out(f">>> manifest: {unchanged} unchanged")
    return rc or (1 if build_failed else 0)


def _drop_flag(argv: list[str], flag: str) -> list[str]:
    """`argv` with a ``--flag VALUE`` / ``--flag=VALUE`` pair removed."""
    out: list[str] = []
    i = 0
    while i < len(argv):
        if argv[i] == flag:
            i += 2
            continue
        if argv[i].startswith(f"{flag}="):
            i += 1
            continue
        out.append(argv[i])
        i += 1
    return out


def _verb_from_manifest(tool: str, verb: str, rest: list[str]) -> int:
    """``<verb> --manifest FILE`` — run `verb` against the systems / domains the
    manifest names (in parallel, via the batch runner). Used by
    ``destroy`` / ``restart`` / ``purge`` / ``status``."""
    entries = manifest.read(cli.peek(rest, constants.OPT_MANIFEST) or "")
    names = [str(e["name"]) for e in entries if e.get("name")]
    role = "domains" if tool == constants.DOM_TOOL else "systems"
    if not names:
        console.out(f"manifest lists no {role} to {verb}")
        return 0
    selector = constants.OPT_DOMAIN if tool == constants.DOM_TOOL else constants.OPT_SYS
    new_rest = _drop_flag(rest, constants.OPT_MANIFEST) + [selector, ",".join(names)]
    return batch.run(tool, verb, new_rest)


_MANIFEST_VERBS = {
    constants.V_DESTROY,
    constants.V_RESTART,
    constants.V_PURGE,
    constants.V_STATUS,
}


def _maybe_batch(tool: str, verb: str, rest: list[str]) -> int | None:
    """Route a verb to a batch runner; None to fall through to the single path.

    ``create --manifest`` replays a manifest; ``destroy`` / ``restart`` /
    ``purge`` ``--manifest`` run the verb against the systems / domains it names;
    otherwise a multi-target selector (``--sys``/``--domain`` with a comma list or
    ``ALL``, or a ``--report`` / ``--fail-fast``) runs the verb per target in
    parallel (see :mod:`batch`)."""
    if verb == constants.V_CREATE and cli.peek(rest, constants.OPT_MANIFEST):
        return _create_from_manifest(tool, rest)
    if verb in _MANIFEST_VERBS and cli.peek(rest, constants.OPT_MANIFEST):
        return _verb_from_manifest(tool, verb, rest)
    if batch.is_batch(tool, verb, rest):
        return batch.run(tool, verb, rest)
    return None


# ======================================================================
# Dispatch
# ======================================================================


def _add_store_dir(tool: str, p: ArgumentParser) -> str:
    """Add the tool's store-dir option to `p`; return the parsed attr name."""
    if tool == constants.SYS_TOOL:
        cli.add_sys_dir(p)
        return "sys_dir"
    cli.add_domain_dir(p)
    return "domain_dir"


def _print_top_help(tool: str) -> None:
    common = _COMMON[tool]
    delegated = _discover(tool)
    verbs = list(common) + [v for v in delegated if v not in common]
    console.out(f"usage: {tool} <command> [options]   ({tool} <command> -h for help)")
    console.out("")
    console.out("commands:")
    summaries = {
        constants.V_TYPES: "list system types, their venvs and docker images",
        constants.V_LIST: "list the descriptors in the store",
        constants.V_INFO: "print a descriptor",
        constants.V_MANIFEST: "dump a YAML manifest of the store (--out FILE)",
        constants.V_SECRETS: "fetch a provider's secrets as env vars (--set 1p:VAULT)",
        constants.V_CLEAR_VENVS: "delete the managed venvs (rebuilt on next use)",
    }
    summaries.update(delegated)
    for v in verbs:
        console.out(f"  {v:14} {summaries.get(v, '')}")
    console.out("")
    console.out("environment:")
    for ev in constants.ENV_VARS:
        console.out(f"  {ev.name:24} {ev.help} [default: {ev.default}]")


def _dispatch(tool: str, argv: list[str]) -> int:
    if not argv or argv[0] in constants.HELP_FLAGS:
        _print_top_help(tool)
        return 0
    verb, rest = argv[0], argv[1:]
    common = _COMMON[tool]
    if verb in common:
        return common[verb](tool, rest)
    if verb not in _discover(tool):
        raise TdxError(f"{tool}: unknown command '{verb}' (try `{tool} -h`)")
    wants_help = any(a in constants.HELP_FLAGS for a in rest)
    if wants_help:
        # honour --type so `create --type X -h` shows X's options
        # A system create shows the named type's own options; with no --type
        # there is no type to speak for, and borrowing the first registered
        # system's would advertise them as though every create took them.
        typed_create = verb == constants.V_CREATE and tool == constants.SYS_TOOL
        named_type = cli.peek(rest, constants.OPT_TYPE) if typed_create else None
        help_slug = named_type or _default_slug(tool, verb)
        module = _command_module(tool, help_slug, verb)
        p = ArgumentParser(prog=f"{tool} {verb}", description=_summary(module))
        p.tdx_system = _system_or_none(  # type: ignore[attr-defined]
            None if typed_create and not named_type else help_slug
        )
        module.add_arguments(p)
        p.parse_args(rest)  # prints help and exits
        return 0
    batched = _maybe_batch(tool, verb, rest)
    if batched is not None:
        return batched
    slug, name, info, path, store_dir = _resolve(tool, verb, rest)
    _require_verb(tool, slug, verb)
    system = systems.get(slug)
    if info is not None:
        # Bind the resolved descriptor so a backend that would read config from
        # the environment (object stores) works from the descriptor alone.
        bind = getattr(system, "configure", None)
        if bind is not None:
            bind(info.meta)
    module = _command_module(tool, slug, verb)
    p = ArgumentParser(prog=f"{tool} {verb}", description=_summary(module))
    p.tdx_system = system  # type: ignore[attr-defined]  # for options-driven create
    module.add_arguments(p)
    args = p.parse_args(rest)
    ctx = cli.Context(tool, verb, slug, system, args, name, info, path, store_dir)
    return module.run(ctx) or 0


# ======================================================================
# Latch + console-split wrapper
# ======================================================================


def _latch_files(latch: Path) -> tuple[Path, Path]:
    return Path(f"{latch}.ok"), Path(f"{latch}.err")


def _clear_latch(latch: Path) -> None:
    ok, err = _latch_files(latch)
    for p in (latch, ok, err):
        try:
            p.unlink(missing_ok=True)
        except OSError:
            pass


def _touch_latch(latch: Path, *, ok: bool) -> None:
    ok_file, err_file = _latch_files(latch)
    try:
        latch.parent.mkdir(parents=True, exist_ok=True)
        (ok_file if ok else err_file).touch()
        latch.touch()
    except OSError as exc:
        print(f">>> warning: could not write latch {latch}: {exc}", file=sys.stderr)


def _log_prefix(tool: str, argv: list[str]) -> str:
    """``<name>-`` for a log filename (``<name>-<timestamp>.log``): the targeted
    ``--sys`` (system tool) or ``--domain`` (domain tool, falling back to ``--sys``
    for a create whose domain name is implied); the tool's role is the fallback
    when there is no single target."""
    role = (
        constants.ROLE_DOMAIN if tool == constants.DOM_TOOL else constants.ROLE_SYSTEM
    )
    if tool == constants.DOM_TOOL:
        name = cli.peek(argv, constants.OPT_DOMAIN) or cli.peek(argv, constants.OPT_SYS)
    else:
        name = cli.peek(argv, constants.OPT_SYS)
    return f"{name}-" if name else f"{role}-"


def _run_dispatch(tool: str, argv: list[str]) -> int:
    verb = argv[0] if argv else ""
    wants_help = any(a in constants.HELP_FLAGS for a in argv[1:])
    latch = None
    if verb in _LATCH_VERBS and not wants_help:
        raw = cli.peek(argv[1:], constants.OPT_LATCH)
        if raw:
            latch = Path(raw)
            _clear_latch(latch)
    logged = verb in _LOGGED_VERBS and not wants_help
    quiet = cli.peek(argv[1:], constants.OPT_OUT) == constants.OUT_JSON
    try:
        if logged:
            console.prune_logs(constants.LOG_DIR, constants.LOG_MAX_AGE_DAYS)
            with console.activate(constants.LOG_DIR, _log_prefix(tool, argv), quiet):
                rc = _dispatch(tool, argv)
        else:
            rc = _dispatch(tool, argv)
    except BaseException:
        if latch is not None:
            _touch_latch(latch, ok=False)
        raise
    if latch is not None:
        _touch_latch(latch, ok=(rc == 0))
    return rc


def _main_system() -> int:
    return _run_dispatch(constants.SYS_TOOL, sys.argv[1:])


def _main_domain() -> int:
    return _run_dispatch(constants.DOM_TOOL, sys.argv[1:])


def main_system() -> None:
    run(_main_system)


def main_domain() -> None:
    run(_main_domain)
