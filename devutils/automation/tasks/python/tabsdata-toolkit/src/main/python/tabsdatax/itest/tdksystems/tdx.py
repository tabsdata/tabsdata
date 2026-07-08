#
# Copyright 2026 Tabsdata Inc.
#

"""`tdx-system` / `tdx-domain` — manage external systems and their domains.

    tdx-system types                                # list all system types
    tdx-system fetch-images                         # docker pull all container images
    tdx-system create  --type TYPE --sys NAME [--sys-dir DIR] [type config]
    tdx-system create  --auto ALL|T1,T2,... [--sys-dir DIR]   # one system per
                                                    # type, each named after its
                                                    # type; config $<PREFIX>__<OPT>
    tdx-system list    [--sys-dir DIR]              # names + types in the store
    tdx-system destroy --sys NAME|ALL|N1,N2,... [--sys-dir DIR]  # + rm descriptor
    tdx-system status  --sys NAME|ALL|N1,N2,... [--sys-dir DIR]
    tdx-system info    --sys NAME [--sys-dir DIR] --format json|export|unset
    tdx-system pause | resume --sys NAME [--sys-dir DIR]  # stop / start container
    tdx-system secrets --get 1p:VAULT --out json|export|unset   # pull creds; also
                                                    # emits $TD_BACKENDS_SYSTEMS
    tdx-system connect --remote-sys [user@]HOST:REMOTE_SYS --sys NAME [--sys-dir DIR]
                       --remote-secret-env VAR      # ssh-tunnel; container system;
                                                    # destroy stops the tunnel

    tdx-domain create  --sys NAME [--sys-dir DIR] --domain NAME [--domain-dir DIR] \\
                       [--domain-prefix PREFIX] [--model MODEL [--data xs|s|m|l|xl]]
    tdx-domain create  --auto ALL|S1,S2,... [--sys-dir DIR] [--domain-dir DIR]
                                                    # one empty domain per system,
                                                    # each named after its system;
                                                    # entity named <PREFIX><domain>;
                                                    # PREFIX = --domain-prefix, else
                                                    # $TD_BUILD_USER_$TD_BUILD_NUMBER_,
                                                    # else <os-user>_0_
    tdx-domain seed    --domain NAME [--domain-dir DIR] --model MODEL \\
                       [--data xs|s|m|l|xl]         # DDL/data into an existing domain
    tdx-domain recreate --domain NAME|ALL|N1,N2,... [--domain-dir DIR]
                                                    # reset to clean initial state
                                                    # (replays recorded ddl/data)
    tdx-domain list    [--domain-dir DIR]           # names + types + model/data
    tdx-domain destroy --domain NAME | --auto ALL|N1,N2,... [--domain-dir DIR]
    tdx-domain status  --domain NAME | --auto ALL|N1,N2,... [--domain-dir DIR]
    tdx-domain info    --domain NAME [--domain-dir DIR] --format json|export|unset

Systems and domains are addressed by *name* (a plain identifier), not by path:
each name is a file in a *store directory* (see the `store` module):
`--sys-dir`/`--domain-dir`, else `${TD_BACKENDS_DIR}/{systems,domains}`, else
`~/.tabsdatax/{systems,domains}`. The file name is also the descriptor's
logical name, so the `info` keys are unprefixed.

Each `create` writes a *descriptor* — a JSON document stored as an encrypted
file (AES-256-GCM in PEM armor, keyed by the personal secret; see the
`descriptor` / `crypto` modules). Every descriptor verb reads that secret from
the env var named by `--secret-env` (default $TD_BACKENDS_CRYPTO_KEY) and fails
if it is unset. `--type` names the external system — every backend in the
`container` / `cloud` / `saas` helper packages is a valid type, so adding a new
system is just a new module.
"""

from __future__ import annotations

import argparse
import getpass
import os
import re
import sys
from pathlib import Path

from . import _venv, cli, crypto, data_gen, descriptor, secret, ssh, store, systems
from .errors import DatagenError, DecryptError, run
from .shell import (
    ArgumentParser as _ArgumentParser,
    docker_pull,
    find_free_port,
    require_docker,
)
from .systems.base import (
    CreateDomainSpec,
    CreateSystemSpec,
    DomainContext,
    SystemContext,
    Unsupported,
)


def _unified(slug: str) -> bool:
    """True if `slug` is a known system in the `systems/` registry. (All
    systems now live there; this stays a small predicate for callers/tests.)"""
    return slug in systems.all_slugs()


# =====================================================================
# shared helpers
# =====================================================================


def _all_types() -> str:
    return ", ".join(systems.all_slugs())


def _all_models() -> str:
    return ", ".join(data_gen.list_models())


def _extract_opt(argv: list[str], name: str) -> str | None:
    """The value of `<name> X` / `<name>=X` in argv (first hit), or None. Lets
    `_sys_create` peek at --type / `_domain_create` peek at --sys before adding
    the type-specific options."""
    for i, tok in enumerate(argv):
        if tok == name and i + 1 < len(argv):
            return argv[i + 1]
        if tok.startswith(name + "="):
            return tok.split("=", 1)[1]
    return None


# --- ALL / comma / --auto selectors ---------------------------------------
# `create --auto` and the `status`/`destroy` `--sys`/`--domain` selectors act
# on many targets at once: the `ALL` sentinel (every registered type, or every
# descriptor in the store) or a comma list. `ALL` is therefore a reserved name
# — it can't name a real system/domain on create.

_ALL = "ALL"


def _is_multi(value: str) -> bool:
    """A `--sys`/`--domain` value selecting more than one target: `ALL` or a
    comma list."""
    return value == _ALL or "," in value


def _split_list(value: str, what: str) -> list[str]:
    """Comma list -> de-duplicated, order-preserving, non-empty tokens."""
    items: list[str] = []
    for tok in value.split(","):
        tok = tok.strip()
        if tok and tok not in items:
            items.append(tok)
    if not items:
        raise DatagenError(f"{what} is empty")
    return items


def _reject_reserved_name(name: str, kind: str) -> None:
    """`ALL` is the select-everything selector, so it can't be a real name."""
    if name == _ALL:
        raise DatagenError(
            f"'{_ALL}' is reserved (the select-all selector) and cannot be a "
            f"{kind} name"
        )


def _expand_types(value: str) -> list[str]:
    """A `tdx-system create --auto` value -> the types to create. `ALL` is every
    registered type; else a comma list, each validated against the registry."""
    if value == _ALL:
        return systems.all_slugs()
    slugs = _split_list(value, "--auto")
    unknown = [s for s in slugs if s not in systems.all_slugs()]
    if unknown:
        raise DatagenError(
            f"unknown type(s) in --auto: {', '.join(unknown)} (known: {_all_types()})"
        )
    return slugs


def _store_entries(directory: Path, role: str) -> list[tuple[str, dict]]:
    """(name, doc) for every readable `role` descriptor in `directory`, skipping
    files that aren't a descriptor (sidecars). A descriptor we can't decrypt is a
    hard error, not a silent skip — otherwise an incorrect $TD_BACKENDS_CRYPTO_KEY
    would make `list` / the `ALL` selector quietly report nothing. Shared by
    `list` and the `ALL` selector so both see the same set."""
    entries: list[tuple[str, dict]] = []
    if directory.is_dir():
        for path in sorted(directory.iterdir()):
            if not path.is_file():
                continue
            try:
                doc = descriptor.load(path)
            except DecryptError:
                raise DatagenError(
                    f"could not decrypt {path} — {crypto.ENV_VAR} is incorrect"
                )
            except DatagenError:
                continue  # not a descriptor / sidecar
            if doc.get("role") == role:
                entries.append((path.name, doc))
    return entries


def _expand_names(value: str, directory: Path, role: str, opt: str) -> list[str]:
    """A `--sys`/`--domain`/`--auto` selector -> concrete names. `ALL` is every
    `role` descriptor in `directory`; else a comma list (each validated)."""
    if value == _ALL:
        return [name for name, _ in _store_entries(directory, role)]
    names = _split_list(value, opt)
    for n in names:
        descriptor.validate_name(n)
    return names


def _derive_domain_name(sys_name: str) -> str:
    """`tdx-domain create --auto` names each domain after its system (verbatim;
    systems and domains live in separate stores, so the names can coincide)."""
    descriptor.validate_name(sys_name)
    return sys_name


# --- DOMAIN_PREFIX / external entity name ---------------------------------
# A domain's descriptor name is a plain local identifier, but the entity it
# creates in the *shared* external system (schema / dataset / folder /
# database) is named `<DOMAIN_PREFIX><domain>` so concurrent CI runs and
# developers don't collide. The prefix is `--domain-prefix` if given, else
# `$TD_BUILD_USER`_`$TD_BUILD_NUMBER`_ (the CI identity), else the OS login name
# and build 0.

_NON_IDENT = re.compile(r"[^A-Za-z0-9_]+")


def _ident_token(value: str) -> str:
    """Squeeze an arbitrary string (a username / build number) into an
    identifier-safe token — non-`[A-Za-z0-9_]` runs collapse to a single `_`,
    trimmed at the ends — so it can head a system/schema/database name."""
    tok = _NON_IDENT.sub("_", value).strip("_")
    return tok or "x"


def _os_username() -> str:
    """The OS login name, with env fallbacks for environments where
    `getpass.getuser()` can't read one."""
    try:
        return getpass.getuser()
    except Exception:  # noqa: BLE001 - no controlling user (odd CI); fall back
        return os.environ.get("USER") or os.environ.get("USERNAME") or "user"


def _resolve_domain_prefix(explicit: str | None) -> str:
    """The DOMAIN_PREFIX prepended to a domain's external entity name.
    `--domain-prefix` wins verbatim; otherwise it is
    `<TD_BUILD_USER or os-user>_<TD_BUILD_NUMBER or 0>_` (each auto-resolved
    token sanitised to identifier-safe)."""
    if explicit is not None:
        return explicit
    user = os.environ.get("TD_BUILD_USER") or _os_username()
    build = os.environ.get("TD_BUILD_NUMBER") or "0"
    return f"{_ident_token(user)}_{_ident_token(build)}_"


def _external_name(domain: str, explicit_prefix: str | None) -> str:
    """The entity-container name to create in the external system for `domain`:
    `<DOMAIN_PREFIX><domain>` (see `_resolve_domain_prefix`)."""
    return _resolve_domain_prefix(explicit_prefix) + domain


def _system_meta_from_domain(meta: dict[str, str]) -> dict[str, str]:
    """Rebuild the originating system's connection meta from a DOMAIN's meta, so
    `create_domain` can re-provision the domain (for `recreate`) without the
    system descriptor. Container domains carry the system as `SYS_*` back-refs
    (SYS_CONTAINER / SYS_ADMIN_USER / SYS_ADMIN_PASSWORD -> CONTAINER / USER /
    PASSWORD, the admin fields create_domain reads); cloud/fs domains already
    carry the system's connection settings (SYS_PATH + forwarded creds)."""
    m = dict(meta)
    if meta.get("SYS_CONTAINER"):
        m["CONTAINER"] = meta["SYS_CONTAINER"]
        m["USER"] = meta.get("SYS_ADMIN_USER", "")
        m["PASSWORD"] = meta.get("SYS_ADMIN_PASSWORD", "")
    return m


# --- batch (`--auto` / `ALL`) reporting -----------------------------------
# `--auto` create and `ALL`/comma status/destroy act on many targets
# independently: one target's failure is reported and the rest still run. Each
# returns 0 iff every target succeeded.


def _report_created_batch(kind: str, results: list[tuple[str, str, bool, str]]) -> int:
    """Per-item + summary report for an `--auto` create. Each result is
    (name, type, ok, detail)."""
    print()
    if not results:
        print(f">>> no {kind}s to create")
        print()
        return 0
    ok_n = sum(1 for r in results if r[2])
    for name, typ, ok, detail in results:
        line = f"  {'ok  ' if ok else 'FAIL'}  {name} ({typ})"
        if detail:
            line += f": {detail}"
        print(line)
    print()
    print(f">>> created {ok_n}/{len(results)} {kind}s")
    print()
    return 0 if ok_n == len(results) else 1


def _multi_status(names: list[str], kind: str, per_name) -> int:
    """Run `per_name(name) -> (ok, detail)` over many targets; print each + a
    summary. Returns 0 iff all are ready."""
    print()
    if not names:
        print(f">>> no {kind}s found")
        print()
        return 0
    rc = 0
    ok_n = 0
    for name in names:
        try:
            ok, detail = per_name(name)
        except Exception as exc:  # noqa: BLE001 - one bad target must not stop the rest
            ok, detail = False, str(exc)
        ok_n += 1 if ok else 0
        rc = rc or (0 if ok else 1)
        print(f"  {'ready    ' if ok else 'not ready'}  {name}: {detail}")
    print()
    print(f">>> {ok_n}/{len(names)} {kind}s ready")
    print()
    return rc


def _multi_apply(names: list[str], kind: str, per_name, *, past: str) -> int:
    """Run `per_name(name) -> detail` (raises on failure) over many targets;
    print each + a ``<past> N/M`` summary (e.g. "destroyed 2/3"). Returns 0 iff
    every target succeeded. Used by the `ALL`/comma destroy and recreate paths."""
    print()
    if not names:
        print(f">>> no {kind}s found")
        print()
        return 0
    rc = 0
    ok_n = 0
    for name in names:
        try:
            detail = per_name(name)
            ok = True
        except Exception as exc:  # noqa: BLE001 - keep going across the rest
            detail, ok = str(exc), False
        ok_n += 1 if ok else 0
        rc = rc or (0 if ok else 1)
        print(f"  {'ok  ' if ok else 'FAIL'}  {name}: {detail}")
    print()
    print(f">>> {past} {ok_n}/{len(names)} {kind}s")
    print()
    return rc


# --- per-item status / destroy (shared by the single + multi paths) --------


def _sys_status_one(name: str, sys_dir: str | None) -> tuple[bool, str]:
    doc = descriptor.load(store.system_path(name, sys_dir))
    _require_role(doc, "sys", "tdx-system", "tdx-domain")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    slug = doc["system"]
    h = systems.get(slug).system_status(SystemContext(slug, doc["name"], meta))
    return h.ok, h.detail


def _sys_destroy_one(name: str, sys_dir: str | None) -> str:
    path = store.system_path(name, sys_dir)
    doc = descriptor.load(path)
    _require_role(doc, "sys", "tdx-system", "tdx-domain")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    slug = doc["system"]
    systems.get(slug).destroy_system(SystemContext(slug, doc["name"], meta))
    os.remove(path)
    return f"({slug}); removed {path}"


def _domain_status_one(name: str, domain_dir: str | None) -> tuple[bool, str]:
    doc = descriptor.load(store.domain_path(name, domain_dir))
    _require_role(doc, "env", "tdx-domain", "tdx-system")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    slug = doc["system"]
    h = systems.get(slug).domain_status(DomainContext(slug, doc["name"], meta))
    return h.ok, h.detail


def _domain_destroy_one(name: str, domain_dir: str | None) -> str:
    path = store.domain_path(name, domain_dir)
    doc = descriptor.load(path)
    _require_role(doc, "env", "tdx-domain", "tdx-system")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    slug = doc["system"]
    systems.get(slug).destroy_domain(DomainContext(slug, doc["name"], meta))
    os.remove(path)
    return f"({slug}); removed {path}"


def _recreate_one(name: str, domain_dir: str | None) -> str:
    """Reset one domain to its clean initial state, from the domain descriptor
    alone, then replay the recorded model/data (a domain with no model comes
    back empty). The reset reuses the domain's name and credentials: SQL/mongo
    containers drop their tables/collections in place (`reset_domain`), keeping
    the user/password/URI; systems with no per-domain credentials (object stores
    / fs / kafka) drop + re-provision the resource fresh and rewrite the
    descriptor."""
    path = str(store.domain_path(name, domain_dir))
    doc = descriptor.load(path)
    _require_role(doc, "env", "tdx-domain", "tdx-system")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    slug = doc["system"]
    sysobj = systems.get(slug)
    model, data = doc.get("model"), doc.get("data")
    # 1. Empty the domain, reusing its identity. Prefer an in-place reset (keeps
    #    the credentials + descriptor); fall back to a fresh re-provision for
    #    systems that don't mint per-domain credentials.
    try:
        sysobj.reset_domain(DomainContext(slug, doc["name"], meta))
    except Unsupported:
        # Re-provisioning mints the entity fresh, so keep the SAME external name
        # the domain was created under (the recorded prefix), not a new one.
        external = doc.get("external_name") or doc["name"]
        started = descriptor.now_stamp()
        baked = sysobj.create_domain(
            SystemContext(slug, doc["name"], _system_meta_from_domain(meta)),
            CreateDomainSpec(
                name=doc["name"],
                out=path,
                force=True,
                options={o.name: meta.get(o.name, "") for o in sysobj.domain_options},
                external_name=external,
            ),
        )
        descriptor.write(sysobj, "env", path, baked)
        descriptor.stamp(
            path,
            name=doc["name"],
            started=started,
            model=model,
            data=data,
            external_name=external,
        )
    # 2. Replay DDL/data if the domain recorded a model (the resource is now a
    #    clean slate, so no force needed — mirrors `create --model --data`).
    if model:
        cli._run_data_create(argparse.Namespace(model=model, domain=path, force=False))
    if data:
        size = data if data_gen.is_sizeable(model) else None
        schemas_dir = Path(path).with_suffix("").as_posix() + "-schemas"
        cli._run_data_load(
            argparse.Namespace(
                model=model,
                domain=path,
                size=size,
                force=False,
                format=None,
                chunks=None,
                ser="json",
                schemas=schemas_dir,
                use_schema_registry=False,
            )
        )
    return f"({slug})" + (f" model={model} data={data or '-'}" if model else " (empty)")


# --- name-based descriptor locators ---------------------------------------
# Systems and domains are addressed by name; `store` maps a name (+ optional
# --sys-dir/--domain-dir) to its descriptor path. These add the flags, and
# resolve+validate the name to a path (see `store` for the precedence).


def _add_sys_dir(p: _ArgumentParser) -> None:
    p.add_argument(
        "--sys-dir",
        dest="sys_dir",
        default=None,
        metavar="DIR",
        help=f"system store dir (default: ${store.BACKENDS_DIR_ENV}"
        f"/systems or {store.DEFAULT_BACKENDS_DIR}/systems)",
    )


def _add_domain_dir(p: _ArgumentParser) -> None:
    p.add_argument(
        "--domain-dir",
        dest="domain_dir",
        default=None,
        metavar="DIR",
        help=f"domain store dir (default: ${store.BACKENDS_DIR_ENV}"
        f"/domains or {store.DEFAULT_BACKENDS_DIR}/domains)",
    )


def _add_secret_env(p: _ArgumentParser) -> None:
    """The `--secret-env` option carried by every descriptor verb: names the env
    var holding the personal secret, defaulting to $TD_BACKENDS_CRYPTO_KEY."""
    p.add_argument(
        "--secret-env",
        dest="secret_env",
        default=crypto.ENV_VAR,
        metavar="VAR",
        help=f"env var holding the personal secret that "
        f"encrypts/decrypts the descriptor "
        f"(default: {crypto.ENV_VAR})",
    )


def _apply_secret(ns) -> None:
    """Point crypto at the verb's `--secret-env` and require it: fails if that
    env var (default $TD_BACKENDS_CRYPTO_KEY) is unset or shorter than 12 chars.
    Every descriptor verb calls this right after parsing, before any I/O."""
    crypto.use_secret_env(ns.secret_env)
    crypto.require_secret()


def _add_sys_locator(p: _ArgumentParser) -> None:
    p.add_argument(
        "--sys",
        required=True,
        metavar="NAME",
        help="system name (a file in the system store dir)",
    )
    _add_sys_dir(p)


def _add_domain_locator(p: _ArgumentParser) -> None:
    p.add_argument(
        "--domain",
        required=True,
        metavar="NAME",
        help="domain name (a file in the domain store dir)",
    )
    _add_domain_dir(p)


def _sys_target(ns) -> str:
    """Validate the --sys name and resolve it to a descriptor path."""
    descriptor.validate_name(ns.sys)
    return str(store.system_path(ns.sys, ns.sys_dir))


def _domain_target(ns) -> str:
    """Validate the --domain name and resolve it to a descriptor path."""
    descriptor.validate_name(ns.domain)
    return str(store.domain_path(ns.domain, ns.domain_dir))


def _sys_hint(ns) -> str:
    """`--sys NAME [--sys-dir DIR]` for a runnable post-create hint."""
    return f"--sys {ns.sys}" + (f" --sys-dir {ns.sys_dir}" if ns.sys_dir else "")


def _domain_hint(ns) -> str:
    """`--domain NAME [--domain-dir DIR]` for a runnable post-create hint."""
    return f"--domain {ns.domain}" + (
        f" --domain-dir {ns.domain_dir}" if ns.domain_dir else ""
    )


def _require_role(doc: dict, role: str, this_cmd: str, other_cmd: str) -> None:
    if doc.get("role") != role:
        raise DatagenError(
            f"{doc.get('name', '?')} is a {doc.get('kind')} descriptor — "
            f"use `{other_cmd}`, not `{this_cmd}`"
        )


def _setenv(meta: dict[str, str]) -> None:
    os.environ.update({k: v for k, v in meta.items() if k != "KIND"})


def _print_created(kind: str, path: str, hint: str) -> None:
    doc = descriptor.load(path)
    print()
    print(f">>> {kind} '{doc['name']}' ({doc['system']}) created")
    print(f"    started:    {doc['started']}")
    if doc.get("external_name"):
        print(f"    external:   {doc['external_name']}")
    if "model" in doc:
        model = doc.get("model") or "-"
        data = doc.get("data") or "-"
        print(f"    model/data: {model} / {data}")
    print(f"    descriptor: {path}")
    print()
    verb = "tdx-domain" if kind == "domain" else "tdx-system"
    print(f"    info:  {verb} info {hint} --format json")
    print()


def _print_listing(kind: str, directory: Path, *, role: str) -> None:
    """List the descriptors in `directory`: name + type, plus model/data for
    domains. Files that aren't a descriptor (sidecars) are skipped; a descriptor
    that won't decrypt is a hard error — an incorrect $TD_BACKENDS_CRYPTO_KEY
    fails loudly rather than showing nothing (see `_store_entries`). Missing /
    empty dir prints a friendly note."""
    entries = _store_entries(directory, role)
    print()
    if not entries:
        print(f"no {kind}s in {directory}")
        print()
        return
    # Column titles: the name column then the system-type column. A system's
    # type is its "Type"; a domain lists the "System" it belongs to.
    name_title, type_title = (
        ("Domain", "System") if role == "env" else ("System", "Type")
    )
    print(f"{kind}s in {directory}:")
    width = max(max(len(name) for name, _ in entries), len(name_title))
    print(f"  {name_title:<{width}}  {type_title}")
    print(f"  {'-' * width}  {'-' * len(type_title)}")
    for name, doc in entries:
        line = f"  {name:<{width}}  {doc.get('system', '?')}"
        if role == "env":
            extras = [f"{k}={doc[k]}" for k in ("model", "data") if doc.get(k)]
            if extras:
                line += "   " + " ".join(extras)
        print(line)
    print()


def _prepare_descriptor_path(path: str, force: bool) -> None:
    """Prepare a path to write a descriptor at: refuse to clobber an existing
    one (unless -f/--force), then create its parent directory. The mkdir means a
    ``<dir>/<file>`` target with a not-yet-existing ``<dir>`` works -- both for
    the descriptor itself and for sibling artifacts written before it (e.g. the
    connect ``.ssh.log``, opened by the tunnel before the descriptor is)."""
    if Path(path).exists() and not force:
        raise DatagenError(
            f"descriptor file already exists: {path} "
            f"(use -f/--force to overwrite, or destroy it first)"
        )
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def _report_status(ok: bool, msg: str) -> int:
    print(("ready: " if ok else "not ready: ") + msg)
    return 0 if ok else 1


def _safe_remove(path: str) -> None:
    """Delete a descriptor file if present, swallowing errors (used on the
    cleanup path so it never masks the original failure)."""
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


def _cleanup_after_failed_create(kind, sysobj, name, path, baked) -> None:
    """Best-effort teardown after a create fails partway through: destroy
    whatever the backend provisioned (container / database / schema / dir) and
    remove any descriptor written, so a failed create leaves nothing behind.
    Never raises — the original error is what the caller re-raises.

    `baked` is the freshly-returned ``{name: Setting}`` from create_system /
    create_domain; its literals carry the back-references destroy needs
    (CONTAINER for a system, SYS_CONTAINER / DB for a container domain)."""
    print(f">>> create failed — cleaning up {kind} '{name}' …", file=sys.stderr)
    try:
        meta = {k: (s.value or "") for k, s in (baked or {}).items()}
        ctx = (
            SystemContext(sysobj.slug, name, meta)
            if kind == "system"
            else DomainContext(sysobj.slug, name, meta)
        )
        if kind == "system":
            sysobj.destroy_system(ctx)
        else:
            sysobj.destroy_domain(ctx)
    except Exception as exc:
        print(
            f">>> cleanup: could not fully destroy {kind} '{name}': {exc}",
            file=sys.stderr,
        )
    _safe_remove(path)


# =====================================================================
# tdx-system
# =====================================================================


def _sys_create(rest: list[str]) -> int:
    """`tdx-system create`: provision one system (`--type` + `--sys`) or a batch
    (`--auto`). One parser with a `--type | --auto` mutually-exclusive group, so
    `create -h` lists both modes and `--auto` shows up automatically."""
    p = _ArgumentParser(
        prog="tdx-system create",
        add_help=True,
        description="Provision external system(s) and write their descriptors "
        "(encrypted files, AES-256-GCM in PEM armor, keyed by "
        "$TD_BACKENDS_CRYPTO_KEY). Give --type + --sys for a single system, or "
        "--auto for a batch.",
    )
    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--type",
        metavar="TYPE",
        help=f"single: external system type ({_all_types()})",
    )
    mode.add_argument(
        "--auto",
        metavar="ALL|T1,T2,...",
        help="batch: create one system per type (ALL or a comma list), each "
        "named after its type with settings from its $<PREFIX>__<OPT> env vars",
    )
    p.add_argument(
        "--sys",
        metavar="NAME",
        help="single: system name (a file in the system store dir)",
    )
    _add_sys_dir(p)
    _add_secret_env(p)
    p.add_argument(
        "-f", "--force", action="store_true", help="recreate if it already exists"
    )
    # Single mode lets the chosen type contribute its own options, so
    # `tdx-system create --type s3 -h` also lists --bucket / --region / …. Peek
    # --type from the raw args (not parse_known_args, which would act on a `-h`
    # before those options are added).
    the_type = _extract_opt(rest, "--type")
    if the_type is not None and the_type in systems.all_slugs():
        systems.get(the_type).add_create_options(p)
    args = p.parse_args(rest)
    _apply_secret(args)
    if args.auto is not None:
        if args.sys:
            raise DatagenError(
                "--sys is not used with --auto (each system is named s_<type>)"
            )
        return _sys_create_auto(args)
    # single mode
    if not args.type:
        raise DatagenError("give --type (with --sys), or --auto for a batch")
    if args.type not in systems.all_slugs():
        raise DatagenError(f"unknown --type {args.type!r} (known: {_all_types()})")
    if not args.sys:
        raise DatagenError("--sys is required with --type")
    _reject_reserved_name(args.sys, "system")
    return _sys_create_single(args, the_type)


def _sys_create_single(args, slug: str) -> int:
    """Provision one system from the parsed create namespace; the descriptor is
    persisted by descriptor.write (no category branch)."""
    sysobj = systems.get(slug)
    out = _sys_target(args)
    _prepare_descriptor_path(out, args.force)
    started = descriptor.now_stamp()
    baked = sysobj.create_system(
        CreateSystemSpec(
            name=args.sys,
            out=out,
            force=args.force,
            settings=sysobj.resolve_create_settings(args),
            host_port=getattr(args, "port", None),
            ready_timeout=getattr(args, "ready_timeout", 300),
        )
    )
    # The system is provisioned now; if persisting its descriptor fails, tear
    # it back down rather than leaking a running container / created resource.
    try:
        descriptor.write(sysobj, "sys", out, baked)
        descriptor.stamp(out, name=args.sys, started=started)
    except BaseException:
        _cleanup_after_failed_create("system", sysobj, args.sys, out, baked)
        raise
    _print_created("system", out, _sys_hint(args))
    return 0


def _sys_types(rest: list[str]) -> int:
    """List every registered system type (a `--type` / `--auto` value) with its
    category, and — for container-based systems — the Docker image (with its
    version tag) and platform they run. Descriptor-free: needs no secret and no
    managed venv, so it just reads the registry."""
    p = _ArgumentParser(
        prog="tdx-system types",
        add_help=True,
        description="List all available system types (values for --type / "
        "--auto), plus each container system's image + platform.",
    )
    p.parse_args(rest)
    slugs = systems.all_slugs()
    width = max(len(s) for s in slugs)
    print()
    print("available system types:")
    for slug in slugs:
        sysobj = systems.get(slug)
        line = f"  {slug:<{width}}  {sysobj.category}"
        if sysobj.category == "container":
            # platform first, then the image (which already carries repo:tag).
            line += f"  {sysobj.image_platform}  {sysobj.image}"
        print(line)
    print()
    print(f">>> {len(slugs)} types")
    print()
    return 0


def _sys_fetch_images(rest: list[str]) -> int:
    """Pull the Docker image of every container system, showing download
    progress, so a later `create` starts without waiting on a download.
    Descriptor-free: needs no secret and no managed venv — it reads the registry
    and shells out to `docker pull`. A per-image failure is reported and the rest
    still run; exits non-zero if any pull failed."""
    p = _ArgumentParser(
        prog="tdx-system fetch-images",
        add_help=True,
        description="Pull the Docker image of every container system, with "
        "download progress.",
    )
    p.parse_args(rest)
    require_docker()
    # One pull per unique image, remembering which system types use it (in
    # practice one each, but a shared image is pulled only once).
    images: dict[tuple[str, str], list[str]] = {}
    for slug in systems.all_slugs():
        sysobj = systems.get(slug)
        if sysobj.category == "container":
            images.setdefault((sysobj.image, sysobj.image_platform), []).append(slug)
    total = len(images)
    print()
    print(f">>> fetching {total} docker image(s) for container systems")
    failed = 0
    for i, ((image, plat), slugs) in enumerate(sorted(images.items()), start=1):
        print(
            f">>> [{i}/{total}] {', '.join(slugs)}: pulling {image} ({plat})",
            flush=True,
        )
        try:
            docker_pull(image, plat)
        except DatagenError as exc:
            print(f">>> failed to pull {image}: {exc}")
            failed += 1
    print()
    summary = f">>> fetched {total - failed}/{total} image(s)"
    print(summary + (f"; {failed} failed" if failed else ""))
    print()
    return 1 if failed else 0


def _sys_create_auto(args) -> int:
    """`--auto ALL|T1,T2,...`: create one system per type, named after the type
    (``s3``, ``mysql``, …), reading every connection setting from its
    ``$<PREFIX>__<OPT>`` env vars (no per-type flags — containers need none,
    cloud/saas read the vars `tdx-system secrets` populates). Systems are created
    independently: a per-type failure is reported and the rest run."""
    results: list[tuple[str, str, bool, str]] = []
    for slug in _expand_types(args.auto):
        name = slug
        try:
            _provision_system_auto(slug, name, args.sys_dir, args.force)
            results.append((name, slug, True, ""))
        except Exception as exc:  # noqa: BLE001 - one failed type must not stop the rest
            results.append((name, slug, False, str(exc)))
    return _report_created_batch("system", results)


def _provision_system_auto(
    slug: str, name: str, sys_dir: str | None, force: bool
) -> None:
    """Create the system `name` of type `slug` with settings resolved purely
    from `$<PREFIX>__<OPT>` env vars (the default-prefix fallback), then persist
    its descriptor — tearing the resource back down if persisting fails."""
    sysobj = systems.get(slug)
    settings = sysobj.resolve_create_settings(
        argparse.Namespace(env_prefix=sysobj.default_env_prefix)
    )
    out = str(store.system_path(name, sys_dir))
    _prepare_descriptor_path(out, force)
    started = descriptor.now_stamp()
    baked = sysobj.create_system(
        CreateSystemSpec(name=name, out=out, force=force, settings=settings)
    )
    try:
        descriptor.write(sysobj, "sys", out, baked)
        descriptor.stamp(out, name=name, started=started)
    except BaseException:
        _cleanup_after_failed_create("system", sysobj, name, out, baked)
        raise


def _sys_destroy(rest: list[str]) -> int:
    args = _locator_ns("tdx-system destroy", "sys", rest)
    if _is_multi(args.sys):
        names = _expand_names(args.sys, store.system_dir(args.sys_dir), "sys", "--sys")
        return _multi_apply(
            names,
            "system",
            lambda n: _sys_destroy_one(n, args.sys_dir),
            past="destroyed",
        )
    descriptor.validate_name(args.sys)
    detail = _sys_destroy_one(args.sys, args.sys_dir)
    print()
    print(f">>> destroyed system '{args.sys}' {detail}")
    print()
    return 0


def _sys_status(rest: list[str]) -> int:
    args = _locator_ns("tdx-system status", "sys", rest)
    if _is_multi(args.sys):
        names = _expand_names(args.sys, store.system_dir(args.sys_dir), "sys", "--sys")
        return _multi_status(
            names, "system", lambda n: _sys_status_one(n, args.sys_dir)
        )
    descriptor.validate_name(args.sys)
    ok, detail = _sys_status_one(args.sys, args.sys_dir)
    return _report_status(ok, detail)


def _sys_info(rest: list[str]) -> int:
    args = _locator_ns("tdx-system info", "sys", rest, want_format=True)
    doc = descriptor.load(_sys_target(args))
    _require_role(doc, "sys", "tdx-system", "tdx-domain")
    print(descriptor.info(doc, args.format))
    return 0


def _sys_pause(rest: list[str]) -> int:
    return _sys_lifecycle(rest, "pause")


def _sys_resume(rest: list[str]) -> int:
    return _sys_lifecycle(rest, "resume")


def _sys_lifecycle(rest: list[str], verb: str) -> int:
    """pause / resume a container system (stop / start the container; its data
    persists). Only container systems have a process to pause."""
    args = _locator_ns(f"tdx-system {verb}", "sys", rest)
    doc = descriptor.load(_sys_target(args))
    _require_role(doc, "sys", "tdx-system", "tdx-domain")
    slug = doc["system"]
    meta = descriptor.resolve(doc)
    _setenv(meta)
    sysobj = systems.get(slug)
    ctx = SystemContext(slug, doc["name"], meta)
    try:
        state = sysobj.pause(ctx) if verb == "pause" else sysobj.resume(ctx)
    except Unsupported:
        raise DatagenError(
            f"{verb} only applies to container systems "
            f"({doc['name']} is a {doc['category']} system)"
        )
    print()
    print(f">>> {verb}d {doc['name']} ({slug}): {state}")
    print()
    return 0


def _sys_secrets(rest: list[str]) -> int:
    """Fetch credentials from a pluggable secret provider and print them for
    the shell. Descriptor-free — no $TD_BACKENDS_CRYPTO_KEY, no managed venv; the
    provider (e.g. `1p`) just shells out to its own CLI (e.g. `op`)."""
    p = _ArgumentParser(
        prog="tdx-system secrets",
        add_help=True,
        description="Fetch secrets from a pluggable provider and print them to "
        "stdout as a JSON dict, `export` lines, or `unset` lines.\n"
        "Apply them to the current shell with `source`/`eval`:\n"
        "  source <(tdx-system secrets --get 1p:dev_xp --out export)\n"
        "  source <(tdx-system secrets --get 1p:dev_xp --out unset)\n"
        "The names follow <ITEM>__<FIELD>, matching the "
        "$<PREFIX>__<OPT> env vars `tdx-system create` reads.\n"
        f"Also sets ${secret.SYSTEMS_ENV} to the distinct <ITEM> prefixes "
        "that have secrets (the systems `tdx-system create --auto` can build).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--get",
        required=True,
        metavar="PROVIDER:REF",
        help="secret source as <PROVIDER>:<REF>. Supported providers: "
        "1p (1Password) — e.g. --get 1p:dev_xp reads vault 'dev_xp'",
    )
    p.add_argument(
        "--out",
        choices=secret._OUT_FORMATS,
        default="export",
        help="output format (default: export). json is the portable "
        "path for Windows / tooling",
    )
    args = p.parse_args(rest)
    secrets = secret.with_systems_var(secret.fetch(args.get))
    text = secret.render(secrets, args.out)
    if text:
        print(text)
    return 0


def _sys_connect(rest: list[str]) -> int:
    """Open an SSH tunnel to a remote container system and write a *local*
    system descriptor that reaches it via localhost:<random-port>. Domains
    created against the local descriptor run their admin + data over the tunnel
    (TCP) — no local Docker daemon needed. `tdx-system destroy` on the local
    descriptor stops the tunnel."""
    p = _ArgumentParser(
        prog="tdx-system connect",
        add_help=True,
        description="Open an SSH tunnel to a remote container system and write a "
        "local system descriptor reaching it via localhost:<port>. "
        "Container systems only: cloud and SaaS systems have no TCP "
        "endpoint to tunnel and are rejected.\n"
        "The server does nothing SSH-specific: the endpoint and the "
        "path to its (encrypted) system descriptor are given here as "
        "an scp-style [user@]HOST:PATH.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--remote-sys",
        required=True,
        metavar="[user@]HOST:REMOTE_SYS_PATH",
        help="ssh endpoint + path to the remote (encrypted) system descriptor",
    )
    _add_sys_locator(p)  # local handle: --sys NAME [--sys-dir DIR]
    _add_secret_env(p)  # the local descriptor is written under this secret
    p.add_argument("--ssh-port", type=int, default=22, help="ssh port (default: 22)")
    p.add_argument(
        "--identity", default=None, metavar="KEYFILE", help="ssh identity file (ssh -i)"
    )
    p.add_argument(
        "--remote-secret-env",
        required=True,
        metavar="VAR",
        help="env var holding the secret that decrypts the REMOTE "
        "descriptor (required); the local descriptor is written "
        "under --secret-env",
    )
    p.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="overwrite the local descriptor if it exists",
    )
    args = p.parse_args(rest)
    _apply_secret(args)  # local (client) secret
    local_path = _sys_target(args)
    _prepare_descriptor_path(local_path, args.force)

    ep, remote_path = ssh.parse_remote_spec(args.remote_sys)
    ep.port = args.ssh_port
    ep.identity = args.identity

    # 1. Fetch + decrypt the remote descriptor under the server's secret.
    with crypto.using_secret_env(args.remote_secret_env):
        remote_doc = descriptor.loads(ssh.fetch_remote_file(ep, remote_path))
        remote_meta = descriptor.resolve(remote_doc)

    if remote_doc.get("role") != "sys" or remote_doc.get("category") != "container":
        raise DatagenError(
            f"{args.remote_sys} is not a container system descriptor "
            f"(tdx-system connect is container-only)"
        )
    slug = remote_doc["system"]
    sysobj = systems.get(slug)
    if getattr(sysobj, "uses_file_staging", False):
        raise DatagenError(
            f"{slug} is a file-staging system: its staging root is a server-side "
            f"filesystem path a TCP tunnel can't reach — connect is not supported"
        )
    target_port = remote_meta.get("PORT", "")
    if not target_port.isdigit():
        raise DatagenError(
            f"remote descriptor has no numeric PORT to tunnel to (got {target_port!r})"
        )
    name = args.sys  # the local handle's name is its file name (already validated)

    # 2. Allocate a local port and open the background tunnel.
    local_port = find_free_port()
    log_path = Path(local_path).with_suffix("").as_posix() + ".ssh.log"
    print()
    print(
        f">>> opening ssh tunnel to {ep.target}: localhost:{local_port} -> "
        f"localhost:{target_port} (container {remote_meta.get('CONTAINER', '?')})"
    )
    pid = ssh.open_tunnel(ep, local_port, int(target_port), log_path)

    # 3. Write the local (connected) descriptor under the client's own secret,
    #    pointing HOST/PORT at the tunnel; tear the tunnel down if that fails.
    started = descriptor.now_stamp()
    try:
        descriptor.write_container_system(
            out=local_path,
            system_type=slug,
            system_kind=remote_meta.get("SYSTEM_KIND", ""),
            label=sysobj.label,
            container=remote_meta.get("CONTAINER", ""),
            host="localhost",
            port=str(local_port),
            user=remote_meta.get("USER", ""),
            password=remote_meta.get("PASSWORD", ""),
            domain_desc=remote_meta.get("DOMAIN_DESC", ""),
            image=remote_meta.get("IMAGE") or None,
            location="ssh-tunnel",
            extra_vars={
                "SSH_HOST": ep.host,
                "SSH_USER": ep.user or "",
                "SSH_PORT": str(ep.port),
                "SSH_TARGET_PORT": target_port,
                "SSH_TUNNEL_PID": str(pid),
            },
        )
        descriptor.stamp(local_path, name=name, started=started)
    except BaseException:
        ssh.close_tunnel(pid)
        _safe_remove(local_path)
        raise
    _print_created("system", local_path, _sys_hint(args))
    return 0


def _sys_list(rest: list[str]) -> int:
    """List the systems in the system store dir (name + type)."""
    p = _ArgumentParser(
        prog="tdx-system list",
        add_help=True,
        description="List the systems in the system store dir (name + type).",
    )
    _add_sys_dir(p)
    _add_secret_env(p)
    args = p.parse_args(rest)
    _apply_secret(args)
    _print_listing("system", store.system_dir(args.sys_dir), role="sys")
    return 0


# =====================================================================
# tdx-domain
# =====================================================================


def _domain_create(rest: list[str]) -> int:
    """`tdx-domain create`: create one domain (`--sys` + `--domain`) or a batch
    (`--auto`). One parser with a `--domain | --auto` mutually-exclusive group,
    so `create -h` lists both modes and `--auto` shows up automatically."""
    p = _ArgumentParser(
        prog="tdx-domain create",
        add_help=True,
        description="Create domain(s) and write their descriptors (encrypted "
        "files, AES-256-GCM in PEM armor, keyed by $TD_BACKENDS_CRYPTO_KEY). "
        "Give --sys + --domain for a single domain, or --auto for a batch.",
    )
    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--domain",
        metavar="NAME",
        help="single: domain name to create (a file in the domain store dir)",
    )
    mode.add_argument(
        "--auto",
        metavar="ALL|S1,S2,...",
        help="batch: create one domain per system (ALL or a comma list of "
        "system names), each domain named after its system",
    )
    p.add_argument(
        "--sys", metavar="NAME", help="single: the system to create the domain in"
    )
    _add_sys_dir(p)
    _add_domain_dir(p)
    _add_secret_env(p)
    p.add_argument(
        "--model",
        default=None,
        help=f"single: install this model's schema into the domain ({_all_models()})",
    )
    p.add_argument(
        "--data",
        default=None,
        choices=["xs", "s", "m", "l", "xl"],
        help="single: also load model data at this size (requires --model)",
    )
    p.add_argument(
        "--domain-prefix",
        dest="domain_prefix",
        default=None,
        metavar="PREFIX",
        help="prefix for the entity container's name in the external system "
        "(schema/dataset/folder/db), so runs don't collide; default: "
        "$TD_BUILD_USER_$TD_BUILD_NUMBER_, else <os-user>_0_",
    )
    p.add_argument(
        "-f", "--force", action="store_true", help="recreate if it already exists"
    )
    # Single mode: peek --sys so the target system can contribute its own
    # create-domain options before we parse (mirrors --type peeking in
    # _sys_create). The peek decrypts the system descriptor, so point crypto at
    # --secret-env first (a missing secret is re-raised cleanly by _apply_secret
    # once we've parsed).
    crypto.use_secret_env(_extract_opt(rest, "--secret-env"))
    sys_peek = _extract_opt(rest, "--sys")
    peeked_sys = None
    if sys_peek and not _is_multi(sys_peek):
        peek_path = store.system_path(sys_peek, _extract_opt(rest, "--sys-dir"))
        try:
            peek_doc = descriptor.load(peek_path) if peek_path.exists() else None
        except DatagenError:
            peek_doc = None
        if peek_doc and peek_doc.get("system") in systems.all_slugs():
            peeked_sys = systems.get(peek_doc["system"])
            peeked_sys.add_domain_options(p)
    args = p.parse_args(rest)
    _apply_secret(args)
    if args.auto is not None:
        if args.sys:
            raise DatagenError(
                "--sys is not used with --auto (the systems come from --auto)"
            )
        return _domain_create_auto(args)
    # single mode
    if not args.sys:
        raise DatagenError("give --sys and --domain, or --auto for a batch")
    if _is_multi(args.sys):
        raise DatagenError(
            "--sys takes a single system here; use `--auto ALL|s1,s2,...` to "
            "create a domain for many systems at once"
        )
    if not args.domain:
        raise DatagenError("--domain is required with --sys")
    _reject_reserved_name(args.domain, "domain")
    return _domain_create_single(args, peeked_sys)


def _domain_create_single(args, peeked_sys) -> int:
    """Create one domain from the parsed create namespace (optionally seeding
    its --model/--data)."""
    if args.data and not args.model:
        raise DatagenError("--data requires --model")
    sys_path = _sys_target(args)
    domain_path = _domain_target(args)
    _prepare_descriptor_path(domain_path, args.force)

    sysdoc = descriptor.load(sys_path)
    _require_role(sysdoc, "sys", "tdx-system", "tdx-domain")
    sysmeta = descriptor.resolve(sysdoc)
    _setenv(sysmeta)
    slug = sysdoc["system"]
    started = descriptor.now_stamp()

    sysobj = peeked_sys or systems.get(slug)
    if (args.model or args.data) and not sysobj.supports_models:
        raise DatagenError(f"{slug} does not support --model / --data")
    external = _external_name(args.domain, args.domain_prefix)
    baked = sysobj.create_domain(
        SystemContext(slug, sysdoc["name"], sysmeta),
        CreateDomainSpec(
            name=args.domain,
            out=domain_path,
            force=args.force,
            options=sysobj.resolve_domain_options(args),
            external_name=external,
        ),
    )
    # The domain resource (database / schema / prefix) exists now. If writing
    # its descriptor OR the optional schema-install / data-load fails, drop the
    # resource and remove the descriptor so nothing half-created is left behind.
    try:
        descriptor.write(sysobj, "env", domain_path, baked)
        descriptor.stamp(
            domain_path,
            name=args.domain,
            started=started,
            model=args.model,
            data=args.data,
            external_name=external,
        )

        # Optional model + data — reuse the data pipeline (reads the descriptor
        # we just wrote). `data create` installs the schema; `data load` fills it.
        if args.model:
            cli._run_data_create(
                argparse.Namespace(
                    model=args.model, domain=domain_path, force=args.force
                )
            )
        if args.data:
            # Fixed-data models have no size (they'd reject one); sizeable models
            # take the requested bucket.
            size = args.data if data_gen.is_sizeable(args.model) else None
            schemas_dir = Path(domain_path).with_suffix("").as_posix() + "-schemas"
            cli._run_data_load(
                argparse.Namespace(
                    model=args.model,
                    domain=domain_path,
                    size=size,
                    force=args.force,
                    format=None,
                    chunks=None,
                    ser="json",
                    schemas=schemas_dir,
                    use_schema_registry=False,
                )
            )
    except BaseException:
        _cleanup_after_failed_create("domain", sysobj, args.domain, domain_path, baked)
        raise
    # Announced only once the domain (and any model/data) is fully in place.
    _print_created("domain", domain_path, _domain_hint(args))
    return 0


def _domain_create_auto(args) -> int:
    """`--auto ALL|S1,S2,...`: create one *empty* domain per system, each named
    after its system. No `--model`/`--data` (models are kind-specific — seed
    each afterwards with `tdx-domain seed`), and no per-system domain options (a
    system needing them is skipped with a note). Domains are created
    independently: a per-system failure is reported and the rest run."""
    sys_names = _expand_names(
        args.auto, store.system_dir(args.sys_dir), "sys", "--auto"
    )
    results: list[tuple[str, str, bool, str]] = []
    for sname in sys_names:
        try:
            dname = _derive_domain_name(sname)
        except DatagenError as exc:
            results.append(("?", sname, False, str(exc)))
            continue
        try:
            _provision_domain_auto(sname, dname, args)
            results.append((dname, sname, True, ""))
        except Exception as exc:  # noqa: BLE001 - one failed system must not stop the rest
            results.append((dname, sname, False, str(exc)))
    return _report_created_batch("domain", results)


def _provision_domain_auto(sname: str, dname: str, args) -> None:
    """Create the empty domain `dname` in system `sname` and persist its
    descriptor — tearing the resource back down if persisting fails. Systems
    that need extra create-domain options (e.g. bigquery) are rejected so they
    stay a deliberate, individual `tdx-domain create`."""
    sys_path = store.system_path(sname, args.sys_dir)
    domain_path = str(store.domain_path(dname, args.domain_dir))
    _prepare_descriptor_path(domain_path, args.force)
    sysdoc = descriptor.load(sys_path)
    _require_role(sysdoc, "sys", "tdx-system", "tdx-domain")
    sysmeta = descriptor.resolve(sysdoc)
    _setenv(sysmeta)
    slug = sysdoc["system"]
    sysobj = systems.get(slug)
    if sysobj.domain_options:
        needed = ", ".join(f"--{o.cli_flag}" for o in sysobj.domain_options)
        raise DatagenError(
            f"{slug} needs domain option(s) {needed}; create its domain "
            f"individually (not via --auto)"
        )
    started = descriptor.now_stamp()
    external = _external_name(dname, args.domain_prefix)
    baked = sysobj.create_domain(
        SystemContext(slug, sysdoc["name"], sysmeta),
        CreateDomainSpec(
            name=dname,
            out=domain_path,
            force=args.force,
            options={},
            external_name=external,
        ),
    )
    try:
        descriptor.write(sysobj, "env", domain_path, baked)
        descriptor.stamp(
            domain_path, name=dname, started=started, external_name=external
        )
    except BaseException:
        _cleanup_after_failed_create("domain", sysobj, dname, domain_path, baked)
        raise


def _domain_seed(rest: list[str]) -> int:
    """`tdx-domain seed --domain NAME --model M [--data SIZE]`: install a
    model's schema (and optionally data) into an EXISTING domain — the seeding
    half of `create --model`, runnable later (e.g. after `create --auto`).
    Refuses if the domain already carries a model (destroy + recreate to
    reseed)."""
    p = _ArgumentParser(
        prog="tdx-domain seed",
        add_help=True,
        description="Install a model's schema (and optionally data) into an "
        "existing domain. Fails if the domain was already seeded.",
    )
    _add_domain_locator(p)
    _add_secret_env(p)
    p.add_argument(
        "--model",
        required=True,
        help=f"model whose schema to install ({_all_models()})",
    )
    p.add_argument(
        "--data",
        default=None,
        choices=["xs", "s", "m", "l", "xl"],
        help="also load model data at this size",
    )
    args = p.parse_args(rest)
    _apply_secret(args)
    if _is_multi(args.domain):
        raise DatagenError("seed operates on a single domain (no ALL / comma list)")
    path = _domain_target(args)
    doc = descriptor.load(path)
    _require_role(doc, "env", "tdx-domain", "tdx-system")
    if doc.get("model"):
        raise DatagenError(
            f"domain '{doc['name']}' is already seeded (model={doc['model']}, "
            f"data={doc.get('data') or '-'}); destroy + recreate to reseed"
        )
    slug = doc["system"]
    sysobj = systems.get(slug)
    if not sysobj.supports_models:
        raise DatagenError(f"{slug} does not support --model / --data")
    # Reuse the same pipeline `create --model` runs, with force=False so an
    # already-populated schema/tables make it fail rather than clobber.
    cli._run_data_create(argparse.Namespace(model=args.model, domain=path, force=False))
    if args.data:
        size = args.data if data_gen.is_sizeable(args.model) else None
        schemas_dir = Path(path).with_suffix("").as_posix() + "-schemas"
        cli._run_data_load(
            argparse.Namespace(
                model=args.model,
                domain=path,
                size=size,
                force=False,
                format=None,
                chunks=None,
                ser="json",
                schemas=schemas_dir,
                use_schema_registry=False,
            )
        )
    # Record the model/data so `list` shows it and a second seed is refused.
    descriptor.stamp(
        path,
        name=doc["name"],
        started=doc.get("started", ""),
        model=args.model,
        data=args.data,
    )
    print()
    print(
        f">>> seeded domain '{doc['name']}' ({slug}) with "
        f"model={args.model} data={args.data or '-'}"
    )
    print()
    return 0


def _domain_recreate(rest: list[str]) -> int:
    """`tdx-domain recreate --domain NAME|ALL|N1,N2,...`: reset domain(s) to a
    clean initial state — re-provision the resource fresh and replay the
    model/data each recorded at create time (so a seeded domain comes back with
    its DDL + data, an empty one comes back empty). Works over `ALL` / a comma
    list; a per-domain failure is reported and the rest still run."""
    args = _locator_ns("tdx-domain recreate", "domain", rest)
    if _is_multi(args.domain):
        names = _expand_names(
            args.domain, store.domain_dir(args.domain_dir), "env", "--domain"
        )
        return _multi_apply(
            names,
            "domain",
            lambda n: _recreate_one(n, args.domain_dir),
            past="recreated",
        )
    descriptor.validate_name(args.domain)
    detail = _recreate_one(args.domain, args.domain_dir)
    print()
    print(f">>> recreated domain '{args.domain}' {detail}")
    print()
    return 0


def _domain_destroy(rest: list[str]) -> int:
    args = _domain_batch_ns("tdx-domain destroy", rest)
    if args.auto is not None:
        names = _expand_names(
            args.auto, store.domain_dir(args.domain_dir), "env", "--auto"
        )
        return _multi_apply(
            names,
            "domain",
            lambda n: _domain_destroy_one(n, args.domain_dir),
            past="destroyed",
        )
    _require_single_domain(args.domain)
    detail = _domain_destroy_one(args.domain, args.domain_dir)
    print()
    print(f">>> destroyed domain '{args.domain}' {detail}")
    print()
    return 0


def _domain_status(rest: list[str]) -> int:
    args = _domain_batch_ns("tdx-domain status", rest)
    if args.auto is not None:
        names = _expand_names(
            args.auto, store.domain_dir(args.domain_dir), "env", "--auto"
        )
        return _multi_status(
            names, "domain", lambda n: _domain_status_one(n, args.domain_dir)
        )
    _require_single_domain(args.domain)
    ok, detail = _domain_status_one(args.domain, args.domain_dir)
    return _report_status(ok, detail)


def _domain_info(rest: list[str]) -> int:
    args = _locator_ns("tdx-domain info", "domain", rest, want_format=True)
    doc = descriptor.load(_domain_target(args))
    _require_role(doc, "env", "tdx-domain", "tdx-system")
    print(descriptor.info(doc, args.format))
    return 0


def _domain_list(rest: list[str]) -> int:
    """List the domains in the domain store dir (name + type + model/data)."""
    p = _ArgumentParser(
        prog="tdx-domain list",
        add_help=True,
        description="List the domains in the domain store dir "
        "(name + type, plus the model/data each carries).",
    )
    _add_domain_dir(p)
    _add_secret_env(p)
    args = p.parse_args(rest)
    _apply_secret(args)
    _print_listing("domain", store.domain_dir(args.domain_dir), role="env")
    return 0


# =====================================================================
# small parsers shared by destroy/status/info
# =====================================================================

_ENCRYPTED_DESC = (
    "the named system/domain's (encrypted) descriptor, keyed by $TD_BACKENDS_CRYPTO_KEY"
)


def _require_single_domain(name: str) -> None:
    """`--domain` selects a single domain on destroy/status; `ALL` / a comma
    list go through `--auto` now. Give a clear hint to anyone still typing the
    old `--domain ALL` form, then validate the (single) name."""
    if _is_multi(name):
        raise DatagenError(
            "--domain takes a single domain here; use "
            "--auto ALL|n1,n2,... for a batch"
        )
    descriptor.validate_name(name)


def _domain_batch_ns(prog: str, rest: list[str]) -> argparse.Namespace:
    """Parse a batch-capable domain verb (destroy / status): a single `--domain
    NAME`, or `--auto ALL|N1,N2,...` for a batch (every domain in the store, or
    a comma list of names) — mirroring `create`'s single-vs-`--auto` split.
    Exactly one of the two is required. Applies the secret before returning."""
    p = _ArgumentParser(
        prog=prog, add_help=True, description=f"Reads {_ENCRYPTED_DESC}."
    )
    grp = p.add_mutually_exclusive_group(required=True)
    grp.add_argument(
        "--domain",
        metavar="NAME",
        help="a single domain (a file in the domain store dir)",
    )
    grp.add_argument(
        "--auto",
        metavar="ALL|N1,N2,...",
        help="batch: ALL domains in the store, or a comma list of names",
    )
    _add_domain_dir(p)
    _add_secret_env(p)
    args = p.parse_args(rest)
    _apply_secret(args)
    return args


def _locator_ns(
    prog: str, kind: str, rest: list[str], *, want_format: bool = False
) -> argparse.Namespace:
    """Parse a read-verb's `--sys`/`--domain` name (+ its `--…-dir` and
    `--secret-env`), and `--format` for info. `kind` is "sys" or "domain".
    Applies the secret before returning, so callers can do I/O immediately."""
    p = _ArgumentParser(
        prog=prog, add_help=True, description=f"Reads {_ENCRYPTED_DESC}."
    )
    if kind == "sys":
        _add_sys_locator(p)
    else:
        _add_domain_locator(p)
    if want_format:
        p.add_argument(
            "--format",
            required=True,
            choices=list(descriptor.VALID_INFO_FORMATS),
            help="json | export | unset",
        )
    _add_secret_env(p)
    args = p.parse_args(rest)
    _apply_secret(args)
    return args


# =====================================================================
# entry points
# =====================================================================

_SYS_VERBS = {
    "types": _sys_types,
    "fetch-images": _sys_fetch_images,
    "create": _sys_create,
    "list": _sys_list,
    "destroy": _sys_destroy,
    "status": _sys_status,
    "info": _sys_info,
    "pause": _sys_pause,
    "resume": _sys_resume,
    "secrets": _sys_secrets,
    "connect": _sys_connect,
}
_DOMAIN_VERBS = {
    "create": _domain_create,
    "seed": _domain_seed,
    "recreate": _domain_recreate,
    "list": _domain_list,
    "destroy": _domain_destroy,
    "status": _domain_status,
    "info": _domain_info,
}

# Verbs that touch no (encrypted) descriptor, so they don't need $TD_BACKENDS_CRYPTO_KEY
# and don't need the managed runtime venv: `secrets` only shells out to a
# provider CLI (e.g. `op`), `types` only reads the in-repo registry, and
# `fetch-images` reads the registry and shells out to `docker pull`.
_NO_DESCRIPTOR_VERBS = {"secrets", "types", "fetch-images"}


def _store_help(cmd: str) -> str:
    """The store-dir fallback blurb for the top-level `tdx-system`/`tdx-domain`
    help (the per-verb `-h` already documents the flags)."""
    sys_line = (
        f"--sys-dir, else ${store.BACKENDS_DIR_ENV}/systems, "
        f"else {store.DEFAULT_BACKENDS_DIR}/systems"
    )
    dom_line = (
        f"--domain-dir, else ${store.BACKENDS_DIR_ENV}/domains, "
        f"else {store.DEFAULT_BACKENDS_DIR}/domains"
    )
    if cmd == "tdx-domain":
        return (
            "Systems/domains are addressed by name in a store dir:\n"
            f"  system: {sys_line}\n"
            f"  domain: {dom_line}"
        )
    return f"Systems are addressed by name in a store dir:\n  {sys_line}"


def _dispatch(cmd: str, verbs: dict, argv: list[str]) -> int:
    if not argv or argv[0] in ("-h", "--help"):
        print(
            f"Usage: {cmd} {{{'|'.join(verbs)}}} [args...]  "
            f"(add -h to a verb for its options)"
        )
        print()
        print(_store_help(cmd))
        return 0 if argv else 2
    verb, rest = argv[0], argv[1:]
    if verb not in verbs:
        raise DatagenError(
            f"unknown {cmd} subcommand: {verb!r} (expected {', '.join(verbs)})"
        )
    # Each descriptor verb applies its own `--secret-env` and requires the secret
    # right after parsing (see `_apply_secret`); the descriptor-free `secrets`
    # verb needs no secret. So the dispatcher just routes.
    return verbs[verb](rest)


def _main_system() -> int:
    import sys

    argv = sys.argv[1:]
    # Descriptor-free verbs (secrets) need no managed venv — keep them running
    # in the zero-dependency launcher.
    if not (argv and argv[0] in _NO_DESCRIPTOR_VERBS):
        _venv.bootstrap(module="tabsdatax.itest.tdksystems.tdx_system")
    return _dispatch("tdx-system", _SYS_VERBS, argv)


def _main_domain() -> int:
    _venv.bootstrap(module="tabsdatax.itest.tdksystems.tdx_domain")
    import sys

    return _dispatch("tdx-domain", _DOMAIN_VERBS, sys.argv[1:])


def main_system() -> None:
    run(_main_system)


def main_domain() -> None:
    run(_main_domain)
