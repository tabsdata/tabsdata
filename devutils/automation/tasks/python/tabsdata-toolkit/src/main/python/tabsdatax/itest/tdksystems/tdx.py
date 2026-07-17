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
    tdx-system create  --sys-manifest FILE|- [--sync] [--sys-dir DIR] [-f]  # every
                                                    # system in a YAML manifest (keys
                                                    # mirror the create flags; - =
                                                    # stdin). --sync reconciles the
                                                    # store to the manifest (create /
                                                    # destroy to match; existing ones
                                                    # are never updated)
    tdx-system dump    --sys-manifest FILE|- [--sys-dir DIR]  # write a manifest of
                                                    # every system in the store
    tdx-system list    [--sys-dir DIR]              # names + types in the store
    tdx-system destroy --sys NAME | --auto ALL|N1,N2,... [--sys-dir DIR] [-f]
                                                    # tear down + rm desc; -f removes
                                                    # the desc even if teardown fails
    tdx-system destroy --sys-manifest FILE|- [--sys-dir DIR] [--domain-dir DIR] [-f]
                                                    # destroy every system in a
                                                    # manifest; one still used by a
                                                    # domain fails, a missing one
                                                    # warns and is skipped
    tdx-system status  --sys NAME | --auto ALL|N1,N2,... [--sys-dir DIR]
    tdx-system status  --sys-manifest FILE|- [--sys-dir DIR]  # readiness of every
                                                    # system in a manifest (a
                                                    # missing one warns and is
                                                    # skipped)
    tdx-system restart [--sys-dir DIR]              # docker-restart every container
                                                    # system in the store (after the
                                                    # host machine / Docker was
                                                    # restarted); systems with no
                                                    # container are skipped
    tdx-system info    --sys NAME [--sys-dir DIR] [--format json|export|unset] \\
                       [--prefix PREFIX]            # PREFIX -> <PREFIX>__<KEY>
    tdx-system pause | resume --sys NAME [--sys-dir DIR]  # stop / start container
    tdx-system secrets --set 1p:VAULT --out json|export|unset   # pull creds; also
                                                    # emits $TD_BACKENDS_SYSTEMS
    tdx-system connect --remote-sys [user@]HOST:REMOTE_SYS --sys NAME [--sys-dir DIR]
                       --remote-secret-env VAR      # ssh-tunnel; container system;
                                                    # destroy stops the tunnel

    tdx-domain create  --sys NAME [--domain NAME] [--model MODEL [--data xs|..|xl]] \\
                       [--domain-prefix PREFIX | --name-in-system NAME] \\
                       [--sys-dir DIR] [--domain-dir DIR]
                                                    # domain named <NAME> (defaults
                                                    # to the system); entity =
                                                    # <PREFIX><NAME>; PREFIX =
                                                    # --domain-prefix, else
                                                    # $TD_BUILD_USER_$TD_BUILD_NUMBER_,
                                                    # else <os-user>_0_.
                                                    # --name-in-system NAME sets the
                                                    # entity name verbatim instead
    tdx-domain create  --auto ALL|S1,S2,... [--model MODEL [--data xs|..|xl]] [-f]
                                                    # one domain per system, each
                                                    # named after its system, seeded
                                                    # where the system supports it
    tdx-domain create  --domain-manifest FILE|- [--sync] [--domain-dir DIR] [-f]
                                                    # every domain in a tdx-domain
                                                    # manifest (keys mirror the create
                                                    # flags; - = stdin). --sync
                                                    # reconciles the store to the
                                                    # manifest (create / destroy to
                                                    # match; existing ones are never
                                                    # updated)
    tdx-domain dump    --domain-manifest FILE|- [--domain-dir DIR]  # write a
                                                    # manifest of every domain in
                                                    # the store
    tdx-domain seed    --domain NAME [--domain-dir DIR] --model MODEL \\
                       [--data xs|s|m|l|xl]         # DDL/data into an existing domain
    tdx-domain recreate --domain NAME|ALL|N1,N2,... [--domain-dir DIR]
                                                    # reset to clean initial state
                                                    # (replays recorded ddl/data)
    tdx-domain list    [--domain-dir DIR]           # names + types + model/data
    tdx-domain destroy --domain NAME | --auto ALL|N1,N2,... [--domain-dir DIR]
                       [--sys-dir DIR] [-f]         # drops via the system's conn
                                                    # (found by the domain's sys_name);
                                                    # -f removes the desc even if the
                                                    # teardown fails
    tdx-domain destroy --domain-manifest FILE|- [--domain-dir DIR] [--sys-dir DIR] [-f]
                                                    # destroy every domain in a
                                                    # manifest; a missing one warns
                                                    # and is skipped
    tdx-domain status  --domain NAME | --auto ALL|N1,N2,... [--domain-dir DIR]
    tdx-domain info    --domain NAME [--domain-dir DIR] [--format json|export|unset] \\
                       [--prefix PREFIX]            # PREFIX -> <PREFIX>__<KEY>

Systems and domains are addressed by *name* (a plain identifier), not by path:
each name is a file in a *store directory* (see the `store` module):
`--sys-dir`/`--domain-dir`, else `${TD_BACKENDS_DIR}/{systems,domains}`, else
`~/.tabsdatax/{systems,domains}`. The file name is also the descriptor's
logical name, so the `info` keys are unprefixed by default; pass `info
--prefix PREFIX` to emit every key as `<PREFIX>__<KEY>`.

The long-running verbs — `create`, `destroy`, `seed`, `recreate` — accept
`--latch FILE` to make a background run waitable: on completion the command
writes `FILE.ok` (success) or `FILE.err` (failure) first, then touches `FILE`
last. So a caller can `tdx-… --latch F &`, wait for `F` to appear, then read
whether `F.ok` or `F.err` is present (use a fresh `F` per run — any stale
markers there are cleared at start, but only after the process boots).

Each `create` writes a *descriptor* — a JSON document stored as an encrypted
file (AES-256-GCM in PEM armor, keyed by the personal secret; see the
`descriptor` / `crypto` modules). Every descriptor verb reads that secret from
the env var named by `--secret-env` (default $TD_BACKENDS_CRYPTO_KEY) and fails
if it is unset. `--type` names the external system — every backend in the
`container` / `cloud` / `saas` / `service` helper packages is a valid type, so
adding a new system is just a new module.

The mutating verbs (`create`, `destroy`, `status`, `restart`, and the
`tdx-domain` `recreate` / `seed`) keep the console output brief: colored
per-target progress and success / failure counters, with any docker image
fetch called out so slow first-time downloads are visible. Their full detail
(docker / SQL / per-worker logs) is written to a timestamped
`<YYYYMMDD>T<hhmmss>.log` in the store directory (see the `console` module)."""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path

from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
)

from . import (
    _venv,
    cli,
    console,
    crypto,
    data_gen,
    descriptor,
    dispatch,
    manifest,
    secret,
    ssh,
    store,
    systems,
)
from .errors import DatagenError, DecryptError, run
from .shell import (
    ArgumentParser as _ArgumentParser,
    container_id,
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
    _build_identity,
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
    files that aren't a descriptor (sidecars). A file that IS a descriptor (has
    our PEM armor) but can't be read is a hard error, not a silent skip — a wrong
    $TD_BACKENDS_CRYPTO_KEY or a stale PEM label (written by an older tdx) would
    otherwise make `list` / the `ALL` selector quietly report nothing. Shared by
    `list` and the `ALL` selector so both see the same set."""
    entries: list[tuple[str, dict]] = []
    if directory.is_dir():
        for path in sorted(directory.iterdir()):
            if not path.is_file():
                continue
            try:
                doc = descriptor.load(path)
            except DecryptError as exc:
                raise DatagenError(f"could not read descriptor {path}: {exc}")
            except DatagenError:
                continue  # no descriptor PEM armor — a genuine sidecar
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


# --- DOMAIN_PREFIX / external entity name ---------------------------------
# A domain's descriptor name is a plain local identifier, but the entity it
# creates in the *shared* external system (schema / dataset / folder /
# database) is named `<DOMAIN_PREFIX><domain>` so concurrent CI runs and
# developers don't collide. The prefix is `--domain-prefix` if given, else
# `$TD_BUILD_USER`_`$TD_BUILD_NUMBER`_ (the CI identity), else the OS login name
# and build 0. `--name-in-system` bypasses this entirely, naming the entity
# container verbatim; either way the chosen name is stamped as `external_name`.


def _resolve_domain_prefix(explicit: str | None) -> str:
    """The DOMAIN_PREFIX prepended to a domain's external entity name.
    `--domain-prefix` wins verbatim; otherwise it is the build identity
    (`<user>_<build>`, see `_build_identity`) with a trailing `_`."""
    if explicit is not None:
        return explicit
    return _build_identity() + "_"


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


def _summary(text: str, ok: bool) -> None:
    """A batch summary line on the console: green when everything succeeded,
    red otherwise."""
    (console.good if ok else console.bad)(text)


def _report_created_batch(
    kind: str,
    results: list[tuple[str, str, bool, str]],
    *,
    quiet: bool = False,
    total: int | None = None,
) -> int:
    """Summary (and, unless `quiet`, per-item) report for an `--auto` / manifest
    create. Each result is (name, type, ok, detail). `quiet` skips the per-item
    lines when the caller has already streamed them live via `_progress`. `total`
    is how many creates were *requested*: batch create fails fast, so when fewer
    than `total` ran (`results` is short) the batch stopped after a failure and
    the rest were not attempted — the summary says so and the command fails."""
    if not results:
        console.out()
        console.out(f">>> no {kind}s to create")
        console.out()
        return 0
    ok_n = sum(1 for r in results if r[2])
    if not quiet:
        console.out()
        for name, typ, ok, detail in results:
            line = f"  {'ok  ' if ok else 'FAIL'}  {name} ({typ})"
            if detail:
                line += f": {detail}"
            (console.good if ok else console.bad)(line)
    requested = total if total is not None else len(results)
    not_attempted = requested - len(results)
    console.out()
    if not_attempted > 0:
        _summary(
            f">>> created {ok_n}/{requested} {kind}s — stopped after a failed "
            f"create ({not_attempted} not attempted)",
            False,
        )
    else:
        _summary(f">>> created {ok_n}/{requested} {kind}s", ok_n == requested)
    console.out()
    return 0 if (not_attempted == 0 and ok_n == requested) else 1


def _emit_captured(outcome: dispatch.Outcome) -> None:
    """Replay a forked worker's captured stdout / stderr into this process's own
    stdout / stderr, so a batch's per-target logs print grouped and in order
    (never interleaved) ahead of the target's result line — each on the stream
    the worker wrote it to."""
    for text, stream in ((outcome.stdout, sys.stdout), (outcome.stderr, sys.stderr)):
        if text:
            stream.write(text)
            if not text.endswith("\n"):
                stream.write("\n")


def _batch_banner(verb: str, n: int, kind: str) -> None:
    """Announce a batch before its targets start, so the user sees work is under
    way (and how much of it runs at once) before any per-target line lands."""
    if n == 0:
        return
    console.out()
    console.out(
        f">>> {verb} {n} {kind}{'' if n == 1 else 's'} "
        f"(up to {dispatch.max_workers(n)} in parallel) ..."
    )
    console.out()


def _progress(
    done: int,
    total: int,
    ok: bool,
    mark: str,
    name: str,
    seconds: float,
    detail: str = "",
) -> None:
    """One live per-target line as a batch target completes: ``[k/N] <mark> name
    (1.2s): detail`` — green when `ok`, red otherwise (see `console.progress`)."""
    console.progress(done, total, ok, mark, name, seconds, detail)


def _batch_create(
    entries: list[dict], entry_worker, kind: str
) -> list[tuple[str, str, bool, str]]:
    """Fan ``entry_worker(entry) -> (name, type, ok, detail)`` out across forked
    processes (each entry's create isolated in its own ``os.environ``), streaming
    each entry's captured log + a live `_progress` line as it finishes, and return
    the per-entry result tuples for `_report_created_batch`. Fails fast: an entry
    whose worker raises (or whose process dies) becomes a FAIL tuple here and no
    further entries are launched (see `dispatch.run`), so the returned list holds
    only the entries that actually ran."""
    _batch_banner("creating", len(entries), kind)
    results: list[tuple[str, str, bool, str]] = []

    def on_done(done: int, total: int, entry: dict, outcome: dispatch.Outcome) -> None:
        _emit_captured(outcome)
        if outcome.ok and outcome.value is not None:
            name, typ, ok, detail = outcome.value
        else:
            name = str(entry.get("name") or entry.get("sys") or "?")
            typ = str(entry.get("type") or entry.get("sys") or "?")
            ok, detail = False, outcome.error or "worker process died"
        results.append((name, typ, ok, detail))
        _progress(
            done,
            total,
            ok,
            "ok  " if ok else "FAIL",
            f"{name} ({typ})",
            outcome.seconds,
            detail,
        )

    dispatch.run(entries, entry_worker, on_done=on_done, stop_on_error=True)
    return results


def _create_entry_tolerant(
    entry_worker, entry: dict, name: str, typ: str
) -> tuple[str, str, bool, str]:
    """Run a fail-fast create-entry worker but catch its failure into a FAIL
    result tuple. The `--sync` reconciliation tolerates a per-entry failure and
    keeps going (reported at the end), unlike the fail-fast batch-create verbs;
    `name`/`typ` label the failed entry when the raise gives no tuple."""
    try:
        return entry_worker(entry)
    except Exception as exc:  # noqa: BLE001 - sync reports and continues
        return (name or "?", typ or "?", False, str(exc))


def _multi_status(names: list[str], kind: str, per_name) -> int:
    """Run `per_name(name) -> (ok, detail)` over many targets (each in its own
    forked process, concurrently), streaming a live `_progress` line per target as
    it finishes + a summary. Returns 0 iff all are ready."""
    if not names:
        console.out()
        console.out(f">>> no {kind}s found")
        console.out()
        return 0
    _batch_banner("checking", len(names), kind)

    def on_done(done: int, total: int, name: str, outcome: dispatch.Outcome) -> None:
        _emit_captured(outcome)
        ok, detail = outcome.value if outcome.ok else (False, outcome.error)
        _progress(
            done,
            total,
            ok,
            "ready    " if ok else "not ready",
            name,
            outcome.seconds,
            detail,
        )

    outcomes = dispatch.run(names, per_name, on_done=on_done)
    ok_n = sum(1 for o in outcomes if o.ok and o.value and o.value[0])
    console.out()
    _summary(f">>> {ok_n}/{len(names)} {kind}s ready", ok_n == len(names))
    console.out()
    return 0 if ok_n == len(names) else 1


def _multi_apply(names: list[str], kind: str, per_name, *, past: str) -> int:
    """Run `per_name(name) -> detail` (raises on failure) over many targets (each
    in its own forked process, concurrently), streaming a live `_progress` line
    per target as it finishes + a ``<past> N/M`` summary (e.g. "destroyed 2/3").
    Returns 0 iff every target succeeded. Used by the `ALL`/comma destroy and
    recreate paths."""
    if not names:
        console.out()
        console.out(f">>> no {kind}s found")
        console.out()
        return 0
    _batch_banner("processing", len(names), kind)

    def on_done(done: int, total: int, name: str, outcome: dispatch.Outcome) -> None:
        _emit_captured(outcome)
        detail = outcome.value if outcome.ok else outcome.error
        _progress(
            done,
            total,
            outcome.ok,
            "ok  " if outcome.ok else "FAIL",
            name,
            outcome.seconds,
            detail,
        )

    outcomes = dispatch.run(names, per_name, on_done=on_done)
    ok_n = sum(1 for o in outcomes if o.ok)
    console.out()
    _summary(f">>> {past} {ok_n}/{len(names)} {kind}s", ok_n == len(names))
    console.out()
    return 0 if ok_n == len(names) else 1


def _report_synced(
    kind: str,
    created: list[tuple[str, str, bool, str]],
    destroyed: list[tuple[str, bool, str]],
    unchanged: list[str],
) -> int:
    """Per-section + summary report for a manifest `--sync`: `created` (in the
    manifest, absent from the store), `destroyed` (in the store, absent from the
    manifest), and `unchanged` (in both — `--sync` never updates an existing
    <kind>). `created` items are (name, type, ok, detail); `destroyed` items are
    (name, ok, detail). Returns 0 iff no create/destroy failed."""
    created_ok = sum(1 for _, _, ok, _ in created if ok)
    destroyed_ok = sum(1 for _, ok, _ in destroyed if ok)
    console.out()
    if created:
        console.out("created (in the manifest, not the store):")
        for name, typ, ok, detail in created:
            line = f"  {'ok  ' if ok else 'FAIL'}  {name} ({typ})"
            if detail:
                line += f": {detail}"
            (console.good if ok else console.bad)(line)
    if destroyed:
        console.out("destroyed (in the store, not the manifest):")
        for name, ok, detail in destroyed:
            line = f"  {'ok  ' if ok else 'FAIL'}  {name}"
            if detail:
                line += f": {detail}"
            (console.good if ok else console.bad)(line)
    if unchanged:
        console.out("unchanged (in both — not updated):")
        for name in unchanged:
            console.out(f"  --    {name}")
    console.out()
    failed = (len(created) - created_ok) + (len(destroyed) - destroyed_ok)
    _summary(
        f">>> synced {kind}s: created {created_ok}/{len(created)}, "
        f"destroyed {destroyed_ok}/{len(destroyed)}, "
        f"{len(unchanged)} left unchanged (not updated)",
        failed == 0,
    )
    console.out()
    return 0 if failed == 0 else 1


# --- per-item status / destroy (shared by the single + multi paths) --------


def _sys_status_one(name: str, sys_dir: str | None) -> tuple[bool, str]:
    doc = descriptor.load(store.system_path(name, sys_dir))
    _require_role(doc, "sys", "tdx-system", "tdx-domain")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    reason = _container_stale(meta)
    if reason:
        return False, f"stale: {reason}"
    slug = doc["system"]
    h = systems.get(slug).system_status(SystemContext(slug, doc["name"], meta))
    return h.ok, h.detail


def _remove_stale_descriptor(
    kind: str, name: str, path, slug: str, reason: str, force: bool
) -> str:
    """Destroy of a descriptor whose container was removed/replaced: refuse
    unless `-f`; with `-f`, drop the descriptor WITHOUT touching the foreign
    container (docker rm would hit the wrong one)."""
    if not force:
        raise DatagenError(
            f"refusing to destroy {kind} '{name}': {reason} "
            f"(use -f to remove the stale descriptor without touching docker)"
        )
    os.remove(path)
    print(
        f">>> WARNING: {reason}; removed descriptor {path} without touching "
        f"docker (-f)",
        file=sys.stderr,
    )
    return f"({slug}); stale, removed {path} without touching docker (-f)"


def _teardown_and_remove(
    kind: str, name: str, path: Path, slug: str, teardown, *, force: bool
) -> str:
    """Run a system/domain `teardown`, then remove its descriptor. Normally a
    failed teardown raises and the descriptor is kept. With `force`, a failed
    teardown still removes the descriptor — so the store doesn't keep a stale
    entry for a backend that's already gone / unreachable — and warns on stderr
    instead of raising."""
    try:
        teardown()
    except Exception as exc:  # noqa: BLE001 - force turns any teardown failure into a warning
        if not force:
            raise
        os.remove(path)
        print(
            f">>> WARNING: could not destroy {kind} '{name}' ({exc}); "
            f"removed its descriptor {path} anyway (-f/--force)",
            file=sys.stderr,
        )
        return f"({slug}); teardown FAILED ({exc}); removed {path} anyway (-f)"
    os.remove(path)
    return f"({slug}); removed {path}"


def _sys_destroy_one(
    name: str,
    sys_dir: str | None,
    domain_dir: str | None = None,
    *,
    force: bool = False,
) -> str:
    path = store.system_path(name, sys_dir)
    doc = descriptor.load(path)
    _require_role(doc, "sys", "tdx-system", "tdx-domain")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    slug = doc["system"]
    # Domains built on this system depend on it (and borrow its credentials to
    # tear down). Refuse unless -f; with -f, destroy each domain FIRST — while
    # the system is still around to lend its creds — ignoring per-domain
    # failures, then drop the system.
    users = _domains_using_system(name, domain_dir)
    if users:
        if not force:
            raise DatagenError(
                f"system '{name}' is in use by domain(s): {', '.join(users)} "
                f"(use -f to destroy them too, then the system)"
            )
        for dom in users:
            try:
                _domain_destroy_one(dom, domain_dir, sys_dir, force=True)
                print(f">>> destroyed dependent domain '{dom}'", file=sys.stderr)
            except Exception as exc:  # noqa: BLE001 - cascade ignores per-domain failures
                print(
                    f">>> WARNING: could not destroy domain '{dom}': {exc}",
                    file=sys.stderr,
                )
    stale = _container_stale(meta)
    if stale:
        return _remove_stale_descriptor("system", name, path, slug, stale, force)
    ctx = SystemContext(slug, doc["name"], meta)
    return _teardown_and_remove(
        "system",
        name,
        path,
        slug,
        lambda: systems.get(slug).destroy_system(ctx),
        force=force,
    )


def _domain_status_one(name: str, domain_dir: str | None) -> tuple[bool, str]:
    doc = descriptor.load(store.domain_path(name, domain_dir))
    _require_role(doc, "env", "tdx-domain", "tdx-system")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    reason = _container_stale(meta)
    if reason:
        return False, f"stale: {reason}"
    slug = doc["system"]
    h = systems.get(slug).domain_status(DomainContext(slug, doc["name"], meta))
    return h.ok, h.detail


def _domain_meta_via_system(doc: dict, sys_dir: str | None) -> dict:
    """The domain's teardown meta with its connection settings refreshed from the
    owning system's descriptor (found via the domain's `sys_name`), so destroy
    drops the entity over the system's authoritative connection rather than the
    domain's baked copy. The domain keeps its entity-identity keys (DB / BASE_PATH
    / SCHEMA / DATASET). Falls back to the domain's own meta when `sys_name` is
    absent (older domains) or the system descriptor is gone."""
    meta = descriptor.resolve(doc)
    sys_name = doc.get("sys_name")
    if not sys_name:
        return meta
    sys_path = store.system_path(sys_name, sys_dir)
    if not sys_path.exists():
        return meta
    try:
        sysmeta = descriptor.resolve(descriptor.load(sys_path))
    except DatagenError:
        return meta
    identity = {"BASE_PATH", "DB", "SCHEMA", "DATASET"}
    merged = dict(meta)
    for k, v in sysmeta.items():
        if k not in identity:
            merged[k] = v
    # A container domain reaches its system admin through SYS_ADMIN_* — refresh
    # those from the system's own USER / PASSWORD.
    if "USER" in sysmeta:
        merged["SYS_ADMIN_USER"] = sysmeta["USER"]
    if "PASSWORD" in sysmeta:
        merged["SYS_ADMIN_PASSWORD"] = sysmeta["PASSWORD"]
    return merged


def _domain_destroy_one(
    name: str, domain_dir: str | None, sys_dir: str | None, *, force: bool = False
) -> str:
    path = store.domain_path(name, domain_dir)
    doc = descriptor.load(path)
    _require_role(doc, "env", "tdx-domain", "tdx-system")
    meta = _domain_meta_via_system(doc, sys_dir)
    _setenv(meta)
    slug = doc["system"]
    stale = _container_stale(meta)
    if stale:
        return _remove_stale_descriptor("domain", name, path, slug, stale, force)
    ctx = DomainContext(slug, doc["name"], meta)
    return _teardown_and_remove(
        "domain",
        name,
        path,
        slug,
        lambda: systems.get(slug).destroy_domain(ctx),
        force=force,
    )


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
    _guard_container_fresh(meta)
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
            sys_name=doc.get("sys_name"),
            entity_term=sysobj.domain_flag,
        )
    # 2. Replay DDL/data if the domain recorded a model (the resource is now a
    #    clean slate, so no force needed — mirrors `create --model --data`).
    _install_model_and_data(path, model, data, force=False)
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


def _add_latch(p: _ArgumentParser) -> None:
    """The `--latch FILE` option carried by the long-running verbs (create /
    destroy / seed / recreate). It makes the run waitable from a background
    caller: on completion the command writes the outcome file `FILE.ok`
    (success) or `FILE.err` (failure) first, then touches `FILE` last (see
    `_run_dispatch`). A caller can `tdx-… --latch F &`, wait for `F` to appear,
    then read whether `F.ok` or `F.err` is present. The value is only parsed
    here so it shows in the verb's -h; the dispatcher does the touching."""
    p.add_argument(
        "--latch",
        default=None,
        metavar="FILE",
        help="on completion touch FILE.ok (success) or FILE.err (failure), then "
        "touch FILE last — so a background caller can wait on FILE and then read "
        "the outcome",
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
    console.out()
    console.good(f">>> {kind} '{doc['name']}' ({doc['system']}) created")
    console.out(f"    started:    {doc['started']}")
    if doc.get("external_name"):
        console.out(f"    external:   {doc['external_name']}")
    if "model" in doc:
        model = doc.get("model") or "-"
        data = doc.get("data") or "-"
        console.out(f"    model/data: {model} / {data}")
    console.out(f"    descriptor: {path}")
    console.out()
    verb = "tdx-domain" if kind == "domain" else "tdx-system"
    console.out(f"    info:  {verb} info {hint}")
    console.out()


def _print_listing(kind: str, directory: Path, *, role: str) -> None:
    """List the descriptors in `directory`: name + type, plus model/data for
    domains. Files that aren't a descriptor (sidecars) are skipped; a descriptor
    that won't decrypt is a hard error — a wrong $TD_BACKENDS_CRYPTO_KEY or a
    stale PEM label (an older tdx's descriptor) fails loudly rather than showing
    nothing (see `_store_entries`). Missing / empty dir prints a friendly note."""
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
    # Pad the type/system column too, so a domain's trailing model/data lines up.
    type_width = max(
        max(len(str(doc.get("system", "?"))) for _, doc in entries), len(type_title)
    )
    print(f"  {name_title:<{width}}  {type_title}")
    print(f"  {'-' * width}  {'-' * type_width}")
    for name, doc in entries:
        line = f"  {name:<{width}}  {str(doc.get('system', '?')):<{type_width}}"
        if role == "env":
            extras = [f"{k}={doc[k]}" for k in ("model", "data") if doc.get(k)]
            if extras:
                line += "  " + " ".join(extras)
        if _container_stale(descriptor.resolve(doc)):
            line += "  [STALE: container replaced/removed]"
        print(line.rstrip())
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
    (console.good if ok else console.bad)(("ready: " if ok else "not ready: ") + msg)
    return 0 if ok else 1


def _safe_remove(path: str) -> None:
    """Delete a descriptor file if present, swallowing errors (used on the
    cleanup path so it never masks the original failure)."""
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


# --- descriptor-first create with retry -----------------------------------
# The create verbs write the FULL descriptor before provisioning, so a failed
# provision can be torn down from the descriptor already on disk, then retried.

CREATE_ATTEMPTS = 3
CREATE_BACKOFF_S = 2


def _system_ctx_from_descriptor(slug: str, path) -> SystemContext:
    """Load a just-written system descriptor into the resolved handle destroy
    reads (mirrors `_sys_destroy_one`)."""
    doc = descriptor.load(path)
    meta = descriptor.resolve(doc)
    _setenv(meta)
    return SystemContext(slug, doc["name"], meta)


def _domain_ctx_from_descriptor(slug: str, path, sys_dir: str | None) -> DomainContext:
    """Load a just-written domain descriptor into the resolved handle destroy
    reads, with the admin connection refreshed from the owning system (mirrors
    `_domain_destroy_one`)."""
    doc = descriptor.load(path)
    meta = _domain_meta_via_system(doc, sys_dir)
    _setenv(meta)
    return DomainContext(slug, doc["name"], meta)


def _provision_with_retry(kind, sysobj, name, out, ctx, provision) -> None:
    """Provision a system/domain whose FULL descriptor is already on disk at
    `out`, retrying up to CREATE_ATTEMPTS times with a fixed backoff. Each failed
    attempt first best-effort destroys whatever it provisioned — reachable
    because the complete descriptor was written before provisioning — swallowing
    any teardown error, so the next attempt starts clean. If every attempt
    fails, the descriptor is removed and the last error re-raised."""

    def _destroy_partial() -> None:
        try:
            if kind == "system":
                sysobj.destroy_system(ctx)
            else:
                sysobj.destroy_domain(ctx)
        except Exception as exc:  # noqa: BLE001 - teardown is best-effort
            print(
                f">>> cleanup: could not destroy partial {kind} '{name}': {exc}",
                file=sys.stderr,
            )

    try:
        for attempt in Retrying(
            stop=stop_after_attempt(CREATE_ATTEMPTS),
            wait=wait_fixed(CREATE_BACKOFF_S),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        ):
            with attempt:
                try:
                    provision()
                except Exception as exc:
                    n = attempt.retry_state.attempt_number
                    # Log the actual error each attempt: a retry can convert it
                    # (e.g. attempt 1's real failure leaves an entity that makes
                    # attempt 2 raise "already exists"), so without this the real
                    # first-attempt cause would never be visible.
                    print(
                        f">>> {kind} '{name}' provisioning failed "
                        f"(attempt {n}/{CREATE_ATTEMPTS}): {exc}",
                        file=sys.stderr,
                    )
                    _destroy_partial()
                    raise
    except Exception:
        _safe_remove(out)
        raise


def _record_provisioned_meta(sysobj, out: str, settings) -> None:
    """After a successful provision, bake any post-provision descriptor keys the
    backend reports (e.g. a container's real docker id) into the on-disk
    descriptor. No-op for backends that report none."""
    extra = sysobj.provisioned_meta(settings)
    if extra:
        descriptor.record_meta(out, extra)


def _prefetch_images(slugs: list[str]) -> None:
    """Pull the docker images the given container system types need — one brief
    console line per image, so on a cold cache the user sees the download is
    where the time is going — before provisioning starts. The pull's own layer
    output is detail (it lands in the log). Types that start no container
    (cloud / saas / fs / service) contribute nothing, so a docker-free batch
    never touches docker here. Best-effort: a pull failure is only surfaced —
    `provision_system` re-pulls and reports it per target."""
    images: dict[tuple[str, str], None] = {}
    for slug in slugs:
        for image, plat in systems.get(slug).docker_images():
            images.setdefault((image, plat), None)
    if not images:
        return
    console.out(f">>> fetching {len(images)} docker image(s) …")
    for image, plat in sorted(images):
        console.out(f"    {image}" + (f" ({plat})" if plat else "") + " …")
        try:
            docker_pull(image, plat)
        except Exception as exc:  # noqa: BLE001 - non-fatal; provisioning re-pulls + reports
            console.warn(f"    could not pre-fetch {image}: {exc}")


# --- container staleness guard --------------------------------------------
# A container system descriptor records the container's real docker id
# (CONTAINER_ID; SYS_CONTAINER_ID on its domains). If the live container named in
# the descriptor is gone or has a different id, the container was removed or
# replaced and the descriptor is stale — read commands flag it, mutating
# commands refuse to act on the wrong container.

_CONTAINER_ID_KEYS = (
    ("CONTAINER", "CONTAINER_ID"),
    ("SYS_CONTAINER", "SYS_CONTAINER_ID"),
)


def _container_stale(meta: dict) -> str | None:
    """A staleness reason if a container-backed descriptor's recorded id no
    longer matches the live container, else None. Best-effort: a descriptor with
    no recorded id (non-container / ssh-tunnel / pre-CONTAINER_ID) or an
    unavailable docker daemon returns None (can't determine)."""
    if meta.get("SYSTEM_LOCATION") == "ssh-tunnel":
        return None
    for name_key, id_key in _CONTAINER_ID_KEYS:
        recorded = meta.get(id_key)
        name = meta.get(name_key)
        if not recorded or not name:
            continue
        try:
            live = container_id(name)
        except DatagenError:
            return None  # docker unavailable — can't determine, don't flag
        if live == recorded:
            return None
        where = "is gone" if live is None else f"was replaced (live id {live[:12]})"
        role = "domain" if name_key == "SYS_CONTAINER" else "system"
        return (
            f"container '{name}' {where}; the descriptor expects id "
            f"{recorded[:12]} — this {role} is stale"
        )
    return None


def _guard_container_fresh(meta: dict) -> None:
    """Refuse a mutating command if the descriptor's container is stale."""
    reason = _container_stale(meta)
    if reason:
        raise DatagenError(
            f"refusing to run: {reason} (destroy with -f to remove the descriptor)"
        )


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
    mode.add_argument(
        "--sys-manifest",
        dest="sys_manifest",
        metavar="FILE|-",
        help="batch: create every system in a `tdx-system` manifest (a list of "
        "entries whose keys mirror these flags; - reads stdin)",
    )
    p.add_argument(
        "--sys",
        metavar="NAME",
        help="single: system name (a file in the system store dir)",
    )
    _add_sys_dir(p)
    _add_domain_dir(p)
    _add_secret_env(p)
    _add_latch(p)
    p.add_argument(
        "-f", "--force", action="store_true", help="recreate if it already exists"
    )
    p.add_argument(
        "--sync",
        action="store_true",
        help="with --sys-manifest: reconcile the store to the manifest — create "
        "systems it lists but the store lacks, destroy systems the store has but "
        "it omits, and leave systems in both untouched (never updated)",
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
    if args.sync and not args.sys_manifest:
        raise DatagenError("--sync is only valid with --sys-manifest")
    if args.sys_manifest:
        if args.sys:
            raise DatagenError(
                "--sys is not used with --sys-manifest (names come from it)"
            )
        if args.sync:
            return _sys_sync_from_manifest(args)
        return _sys_create_from_manifest(args)
    if args.auto is not None:
        if args.sys:
            raise DatagenError(
                "--sys is not used with --auto (each system is named after its type)"
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


def _sys_common_argv(args) -> list[str]:
    """The store/secret/force flags to forward from a batch system-create to each
    per-entry single create."""
    common: list[str] = []
    if args.sys_dir:
        common += ["--sys-dir", args.sys_dir]
    if args.secret_env and args.secret_env != crypto.ENV_VAR:
        common += ["--secret-env", args.secret_env]
    if args.force:
        common.append("-f")
    return common


def _entry_sys_name(entry: dict) -> str:
    """A system manifest entry's identity: its `name` (the system file name),
    accepting the legacy `sys` key too; empty when neither is set."""
    return str(entry.get("name") or entry.get("sys") or "")


def _sys_create_entry(entry: dict, common: list[str]) -> tuple[str, str, bool, str]:
    """Create one system from a manifest entry via the single-create path (full
    validation), returning a (name, type, ok, detail) result on success. A
    failure RAISES (batch create fails fast — `_batch_create` turns the raise
    into a FAIL result and stops launching more); the reconciling `--sync` path
    catches it to keep going."""
    e = dict(entry)
    typ = str(e.pop("type", "?"))
    # identity is `name` (the system file name); accept legacy `sys` too. It
    # maps to the `--sys` create flag; the rest of the keys are settings.
    name = str(e.pop("name", None) or e.pop("sys", None) or "?")
    argv = [*common, "--type", typ, "--sys", name, *manifest.entry_to_argv(e)]
    _sys_create(argv)
    return (name, typ, True, "")


def _sys_create_from_manifest(args) -> int:
    """`--sys-manifest FILE|-`: create every system entry in the manifest, reusing
    the single-create path per entry (full validation), run concurrently (each in
    its own forked process). Fails fast: the first failed entry stops the batch
    (in-flight entries finish) and the command exits non-zero."""
    common = _sys_common_argv(args)
    entries = list(manifest.read(args.sys_manifest))
    _prefetch_images([
        t for t in (str(e.get("type", "")) for e in entries) if t in systems.all_slugs()
    ])
    results = _batch_create(entries, lambda e: _sys_create_entry(e, common), "system")
    return _report_created_batch("system", results, quiet=True, total=len(entries))


def _sys_sync_from_manifest(args) -> int:
    """`--sys-manifest FILE|- --sync`: reconcile the store to the manifest — create
    every system the manifest lists but the store lacks, destroy every system the
    store has but the manifest omits, and leave systems present in both untouched
    (`--sync` never updates an existing system). A system still referenced by a
    domain is not destroyed. Continue-on-error: each failed create/destroy is
    reported and the rest still run."""
    common = _sys_common_argv(args)
    store_names = {
        name for name, _ in _store_entries(store.system_dir(args.sys_dir), "sys")
    }
    manifest_names: set[str] = set()
    created: list[tuple[str, str, bool, str]] = []
    unchanged: list[str] = []
    for entry in manifest.read(args.sys_manifest):
        name = _entry_sys_name(entry)
        if not name:
            print(">>> skipping manifest entry with no `name`", file=sys.stderr)
            continue
        manifest_names.add(name)
        if name in store_names:
            unchanged.append(name)
        else:
            created.append(
                _create_entry_tolerant(
                    lambda e: _sys_create_entry(e, common),
                    entry,
                    name,
                    str(entry.get("type") or "?"),
                )
            )
    destroyed: list[tuple[str, bool, str]] = []
    for name in sorted(store_names - manifest_names):
        try:
            users = _domains_using_system(name, args.domain_dir)
            if users:
                raise DatagenError(f"in use by domain(s): {', '.join(users)}")
            destroyed.append((
                name,
                True,
                _sys_destroy_one(name, args.sys_dir, args.domain_dir),
            ))
        except Exception as exc:  # noqa: BLE001 - one bad target must not stop the rest
            destroyed.append((name, False, str(exc)))
    return _report_synced("system", created, destroyed, unchanged)


def _sys_dump(rest: list[str]) -> int:
    """`tdx-system dump --sys-manifest FILE|-`: write a manifest of every system in
    the store, so it can be replayed (`tdx-system create --sys-manifest`), used to
    destroy them (`tdx-system destroy --sys-manifest`), or used to create domains
    (`tdx-domain create --sys-manifest`). Each entry mirrors the system's
    `create` flags; secret settings stay `{env: VAR}` references, never resolved."""
    p = _ArgumentParser(
        prog="tdx-system dump",
        add_help=True,
        description="Write a YAML manifest of every system in the store dir.",
    )
    p.add_argument(
        "--sys-manifest",
        dest="sys_manifest",
        required=True,
        metavar="FILE|-",
        help="output manifest path (- for stdout)",
    )
    _add_sys_dir(p)
    _add_secret_env(p)
    args = p.parse_args(rest)
    _apply_secret(args)
    entries = [
        manifest.system_entry_from_descriptor(doc)
        for _, doc in _store_entries(store.system_dir(args.sys_dir), "sys")
    ]
    manifest.write(args.sys_manifest, entries)
    return 0


def _sys_create_single(args, slug: str) -> int:
    """Provision one system from the parsed create namespace. Its FULL
    descriptor is planned and written BEFORE provisioning, so a failed provision
    is torn down from it and retried (no category branch)."""
    sysobj = systems.get(slug)
    out = _sys_target(args)
    _prepare_descriptor_path(out, args.force)
    spec = CreateSystemSpec(
        name=args.sys,
        out=out,
        force=args.force,
        settings=sysobj.resolve_create_settings(args),
        host_port=getattr(args, "port", None),
        ready_timeout=getattr(args, "ready_timeout", 300),
    )
    settings = sysobj.plan_system(spec)
    started = descriptor.now_stamp()
    descriptor.write(sysobj, "sys", out, settings)
    descriptor.stamp(out, name=args.sys, started=started)
    ctx = _system_ctx_from_descriptor(slug, out)
    _prefetch_images([slug])
    _provision_with_retry(
        "system",
        sysobj,
        args.sys,
        out,
        ctx,
        lambda: sysobj.provision_system(spec, settings),
    )
    _record_provisioned_meta(sysobj, out, settings)
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
        # platform first, then the image (which already carries repo:tag); a
        # compose stack lists each of its images.
        imgs = sysobj.docker_images()
        if imgs:
            line += "  " + ", ".join(
                (f"{plat}  {img}" if plat else img) for img, plat in imgs
            )
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
        for image, plat in sysobj.docker_images():
            images.setdefault((image, plat), []).append(slug)
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
    concurrently (each in its own forked process) but the batch fails fast: on
    the first failure no more systems are launched (those already in flight
    finish), and the command exits non-zero."""
    slugs = _expand_types(args.auto)
    _prefetch_images(slugs)
    _batch_banner("creating", len(slugs), "system")
    results: list[tuple[str, str, bool, str]] = []

    def on_done(done: int, total: int, slug: str, outcome: dispatch.Outcome) -> None:
        _emit_captured(outcome)
        detail = "" if outcome.ok else outcome.error
        results.append((slug, slug, outcome.ok, detail))
        _progress(
            done,
            total,
            outcome.ok,
            "ok  " if outcome.ok else "FAIL",
            slug,
            outcome.seconds,
            detail,
        )

    dispatch.run(
        slugs,
        lambda slug: _provision_system_auto(slug, slug, args.sys_dir, args.force),
        on_done=on_done,
        stop_on_error=True,
    )
    return _report_created_batch("system", results, quiet=True, total=len(slugs))


def _provision_system_auto(
    slug: str, name: str, sys_dir: str | None, force: bool
) -> None:
    """Create the system `name` of type `slug` with settings resolved purely
    from `$<PREFIX>__<OPT>` env vars (the default-prefix fallback): plan and
    persist its full descriptor, then provision with retry (see
    `_provision_with_retry`)."""
    sysobj = systems.get(slug)
    out = str(store.system_path(name, sys_dir))
    spec = CreateSystemSpec(
        name=name,
        out=out,
        force=force,
        settings=sysobj.resolve_create_settings(
            argparse.Namespace(env_prefix=sysobj.default_env_prefix)
        ),
    )
    _prepare_descriptor_path(out, force)
    settings = sysobj.plan_system(spec)
    started = descriptor.now_stamp()
    descriptor.write(sysobj, "sys", out, settings)
    descriptor.stamp(out, name=name, started=started)
    ctx = _system_ctx_from_descriptor(slug, out)
    _provision_with_retry(
        "system",
        sysobj,
        name,
        out,
        ctx,
        lambda: sysobj.provision_system(spec, settings),
    )
    _record_provisioned_meta(sysobj, out, settings)


def _domains_using_system(sys_name: str, domain_dir: str | None) -> list[str]:
    """Names of domains in the store owned by system `sys_name` (matched on each
    domain descriptor's `sys_name`). Used by `tdx-system destroy --sys-manifest` to
    refuse dropping a system a domain still depends on."""
    return [
        name
        for name, doc in _store_entries(store.domain_dir(domain_dir), "env")
        if doc.get("sys_name") == sys_name
    ]


def _sys_names_from_manifest(manifest_path: str, sys_dir: str | None) -> list[str]:
    """System names in a `tdx-system` manifest that exist in the store; an entry
    whose system is missing from the store is warned to stderr and skipped."""
    names: list[str] = []
    for entry in manifest.read(manifest_path):
        name = str(entry.get("name") or entry.get("sys") or "")
        if not name:
            print(">>> skipping manifest entry with no `name`", file=sys.stderr)
            continue
        if not store.system_path(name, sys_dir).exists():
            print(f">>> skipping system '{name}': not in the store", file=sys.stderr)
            continue
        names.append(name)
    return names


def _sys_batch_ns(
    prog: str, rest: list[str], *, with_force: bool = False, with_latch: bool = False
) -> argparse.Namespace:
    """Parse a batch-capable system verb (destroy / status): a single `--sys NAME`,
    `--auto ALL|N1,N2,...` for a batch (every system in the store, or a comma list
    of names), or `--sys-manifest FILE|-` for every system in a `tdx-system`
    manifest — mirroring the domain side. Exactly one is required. `with_force`
    adds `-f/--force` and `with_latch` adds `--latch` (destroy only). Applies the
    secret before returning."""
    p = _ArgumentParser(
        prog=prog, add_help=True, description=f"Reads {_ENCRYPTED_DESC}."
    )
    grp = p.add_mutually_exclusive_group(required=True)
    grp.add_argument(
        "--sys",
        metavar="NAME",
        help="a single system (a file in the system store dir)",
    )
    grp.add_argument(
        "--auto",
        metavar="ALL|N1,N2,...",
        help="batch: ALL systems in the store, or a comma list of names",
    )
    grp.add_argument(
        "--sys-manifest",
        dest="sys_manifest",
        metavar="FILE|-",
        help="batch: every system in a `tdx-system` manifest (- reads stdin); a "
        "system missing from the store is warned and skipped",
    )
    if with_force:
        p.add_argument(
            "-f",
            "--force",
            action="store_true",
            help="remove the descriptor even if the backend teardown fails "
            "(warns instead of failing)",
        )
    if with_latch:
        _add_latch(p)
    _add_sys_dir(p)
    _add_domain_dir(p)
    _add_secret_env(p)
    args = p.parse_args(rest)
    _apply_secret(args)
    return args


def _sys_destroy_from_manifest(args) -> int:
    """`--sys-manifest FILE|-`: destroy every system named in a `tdx-system`
    manifest. A system missing from the store is warned to stderr and skipped; one
    still referenced by a domain fails unless -f (which cascades — destroys the
    dependent domains too), and the rest still run."""
    names = _sys_names_from_manifest(args.sys_manifest, args.sys_dir)
    return _multi_apply(
        names,
        "system",
        lambda name: _sys_destroy_one(
            name, args.sys_dir, args.domain_dir, force=args.force
        ),
        past="destroyed",
    )


def _require_single_system(name: str) -> None:
    """`--sys` selects a single system on destroy/status; `ALL` / a comma list go
    through `--auto`. Hint anyone still packing them into `--sys`, then validate."""
    if _is_multi(name):
        raise DatagenError(
            "--sys takes a single system here; use --auto ALL|n1,n2,... for a batch"
        )
    descriptor.validate_name(name)


def _sys_destroy(rest: list[str]) -> int:
    """`tdx-system destroy`: drop a single system (`--sys NAME`), a batch
    (`--auto ALL|N1,N2,...`), or every system in a `tdx-system` manifest
    (`--sys-manifest`). A system still used by domains fails unless -f, which
    cascades — destroying the dependent domains (ignoring failures) first."""
    args = _sys_batch_ns("tdx-system destroy", rest, with_force=True, with_latch=True)
    if args.sys_manifest:
        return _sys_destroy_from_manifest(args)
    if args.auto is not None:
        names = _expand_names(
            args.auto, store.system_dir(args.sys_dir), "sys", "--auto"
        )
        return _multi_apply(
            names,
            "system",
            lambda n: _sys_destroy_one(
                n, args.sys_dir, args.domain_dir, force=args.force
            ),
            past="destroyed",
        )
    _require_single_system(args.sys)
    detail = _sys_destroy_one(args.sys, args.sys_dir, args.domain_dir, force=args.force)
    console.out()
    console.good(f">>> destroyed system '{args.sys}' {detail}")
    console.out()
    return 0


def _sys_status(rest: list[str]) -> int:
    """`tdx-system status`: readiness of a single system (`--sys NAME`), a batch
    (`--auto ALL|N1,N2,...`), or every system in a `tdx-system` manifest
    (`--sys-manifest`)."""
    args = _sys_batch_ns("tdx-system status", rest)
    if args.sys_manifest:
        names = _sys_names_from_manifest(args.sys_manifest, args.sys_dir)
        return _multi_status(
            names, "system", lambda n: _sys_status_one(n, args.sys_dir)
        )
    if args.auto is not None:
        names = _expand_names(
            args.auto, store.system_dir(args.sys_dir), "sys", "--auto"
        )
        return _multi_status(
            names, "system", lambda n: _sys_status_one(n, args.sys_dir)
        )
    _require_single_system(args.sys)
    ok, detail = _sys_status_one(args.sys, args.sys_dir)
    return _report_status(ok, detail)


def _sys_restart_one(name: str, sys_dir: str | None) -> str:
    """Restart one container system from its descriptor (docker restart)."""
    doc = descriptor.load(store.system_path(name, sys_dir))
    _require_role(doc, "sys", "tdx-system", "tdx-domain")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    slug = doc["system"]
    state = systems.get(slug).restart(SystemContext(slug, doc["name"], meta))
    return f"({slug}); {state}"


def _restartable_systems(
    sys_dir: str | None,
) -> tuple[list[str], list[tuple[str, str]]]:
    """Partition the store's systems into (restartable names, [(name, reason)]
    skipped). Only container-backed systems (`supports_pause`) are restartable;
    an ssh-tunnel handle (remote container) or a stale descriptor (container gone
    / replaced) is skipped with a reason. Systems with no container (cloud / saas
    / service / fs) are simply not applicable and omitted."""
    targets: list[str] = []
    skipped: list[tuple[str, str]] = []
    for name, doc in _store_entries(store.system_dir(sys_dir), "sys"):
        if not systems.get(doc["system"]).supports_pause:
            continue
        meta = descriptor.resolve(doc)
        if meta.get("SYSTEM_LOCATION") == "ssh-tunnel" or meta.get("SSH_TUNNEL_PID"):
            skipped.append((name, "ssh-tunnel (its container is remote)"))
            continue
        stale = _container_stale(meta)
        if stale:
            skipped.append((name, stale))
            continue
        targets.append(name)
    return targets, skipped


def _sys_restart(rest: list[str]) -> int:
    """`tdx-system restart`: restart the docker container(s) of every container
    system in the store — the one command to run after the host machine or Docker
    was restarted. Systems with no container (cloud / SaaS / service / fs) are not
    applicable and skipped; an ssh-tunnel handle or a stale descriptor is reported
    and skipped. Restarts run concurrently."""
    p = _ArgumentParser(
        prog="tdx-system restart",
        add_help=True,
        description="Restart the docker container(s) of every container system in "
        "the store (useful after the host machine or Docker was restarted).",
    )
    _add_sys_dir(p)
    _add_secret_env(p)
    args = p.parse_args(rest)
    _apply_secret(args)
    targets, skipped = _restartable_systems(args.sys_dir)
    for name, why in skipped:
        console.warn(f">>> skipping '{name}': {why}")
    return _multi_apply(
        targets,
        "system",
        lambda n: _sys_restart_one(n, args.sys_dir),
        past="restarted",
    )


def _sys_info(rest: list[str]) -> int:
    args = _locator_ns("tdx-system info", "sys", rest, want_format=True)
    doc = descriptor.load(_sys_target(args))
    _require_role(doc, "sys", "tdx-system", "tdx-domain")
    print(descriptor.info(doc, args.format, prefix=args.prefix))
    # Flag (on stderr, so json/export stdout stays clean) when the recorded
    # container no longer matches the live one.
    reason = _container_stale(descriptor.resolve(doc))
    if reason:
        print(f">>> STALE: {reason}", file=sys.stderr)
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
    _guard_container_fresh(meta)
    sysobj = systems.get(slug)
    ctx = SystemContext(slug, doc["name"], meta)
    try:
        state = sysobj.pause(ctx) if verb == "pause" else sysobj.resume(ctx)
    except Unsupported:
        raise DatagenError(
            f"{verb} only applies to container systems "
            f"({doc['name']} is a {doc['category']} system)"
        )
    console.out()
    console.good(f">>> {verb}d {doc['name']} ({slug}): {state}")
    console.out()
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
        "  source <(tdx-system secrets --set 1p:dev_xp --out export)\n"
        "  source <(tdx-system secrets --set 1p:dev_xp --out unset)\n"
        "The names follow <ITEM>__<FIELD>, matching the "
        "$<PREFIX>__<OPT> env vars `tdx-system create` reads.\n"
        f"Also sets ${secret.SYSTEMS_ENV} to the distinct <ITEM> prefixes "
        "that have secrets (the systems `tdx-system create --auto` can build).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--set",
        dest="ref",
        required=True,
        metavar="PROVIDER:REF",
        help="secret source as <PROVIDER>:<REF>. Supported providers: "
        "1p (1Password) — e.g. --set 1p:dev_xp reads vault 'dev_xp'",
    )
    p.add_argument(
        "--out",
        choices=secret._OUT_FORMATS,
        default="export",
        help="output format (default: export). json is the portable "
        "path for Windows / tooling",
    )
    args = p.parse_args(rest)
    secrets = secret.with_systems_var(secret.fetch(args.ref))
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
    """`tdx-domain create`: create one domain (`--sys` + optional `--domain`) or a
    batch (`--auto` / `--domain-manifest`). The domain is named by `--domain`
    (default: the system name); its external entity (schema / dataset / directory)
    is named `<PREFIX><domain>`, the PREFIX defaulting to the run's
    `<user>_<build>_` (see `--domain-prefix`), or named verbatim by
    `--name-in-system` (single-domain create only)."""
    p = _ArgumentParser(
        prog="tdx-domain create",
        add_help=True,
        description="Create domain(s) and write their descriptors (encrypted "
        "files, AES-256-GCM in PEM armor, keyed by $TD_BACKENDS_CRYPTO_KEY). "
        "Give --sys (with optional --domain) for a single domain, or --auto / "
        "--domain-manifest for a batch.",
    )
    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--auto",
        metavar="ALL|S1,S2,...",
        help="batch: create one domain per system (ALL or a comma list of system "
        "names), each named after its system, seeding --model/--data where the "
        "system supports it (a no-op elsewhere)",
    )
    mode.add_argument(
        "--domain-manifest",
        dest="domain_manifest",
        metavar="FILE|-",
        help="batch: create every domain in a `tdx-domain` manifest (a list of "
        "entries whose keys mirror these flags; - reads stdin)",
    )
    p.add_argument(
        "--sys", metavar="NAME", help="single: the system to create the domain in"
    )
    p.add_argument(
        "--domain",
        default=None,
        metavar="NAME",
        help="single: the domain name (default: the system name)",
    )
    _add_sys_dir(p)
    _add_domain_dir(p)
    _add_secret_env(p)
    _add_latch(p)
    p.add_argument(
        "--model",
        default=None,
        help=f"install this model's schema into the domain ({_all_models()}); "
        "--auto skips systems that don't support it",
    )
    p.add_argument(
        "--data",
        default=None,
        choices=["xs", "s", "m", "l", "xl"],
        help="load model data at this size (requires --model)",
    )
    p.add_argument(
        "--domain-prefix",
        dest="domain_prefix",
        default=None,
        metavar="PREFIX",
        help="prefix for the entity container's name in the external system "
        "(schema/dataset/folder/db), so runs don't collide; default: "
        "$TD_BUILD_USER_$TD_BUILD_NUMBER_, else <os-user>_0_ (not stored in a "
        "manifest, so a manifest replays under each user's own prefix)",
    )
    p.add_argument(
        "--name-in-system",
        dest="name_in_system",
        default=None,
        metavar="NAME",
        help="use NAME verbatim as the entity container (schema/dataset/folder/db) "
        "created in the target system, instead of the default "
        "<domain-prefix><domain>; overrides --domain-prefix. Single-domain create "
        "only (in a manifest, use the per-entry `name-in-system` key)",
    )
    p.add_argument(
        "-f", "--force", action="store_true", help="recreate if it already exists"
    )
    p.add_argument(
        "--sync",
        action="store_true",
        help="with --domain-manifest: reconcile the store to the manifest — create "
        "domains it lists but the store lacks, destroy domains the store has but "
        "it omits, and leave domains in both untouched (never updated)",
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
    if args.sync and not args.domain_manifest:
        raise DatagenError("--sync is only valid with --domain-manifest")
    if args.name_in_system is not None and (args.domain_manifest or args.auto):
        raise DatagenError(
            "--name-in-system names a single entity container, so it is not used "
            "with --auto / --domain-manifest (a manifest carries a per-entry "
            "`name-in-system` key)"
        )
    if args.domain_manifest:
        if args.sys or args.domain:
            raise DatagenError(
                "--sys / --domain are not used with --domain-manifest "
                "(names come from it)"
            )
        if args.sync:
            return _domain_sync_from_manifest(args)
        return _domain_create_from_manifest(args)
    if args.auto is not None:
        if args.sys or args.domain:
            raise DatagenError(
                "--sys / --domain are not used with --auto (the systems come "
                "from --auto)"
            )
        return _domain_create_auto(args)
    # single mode
    if not args.sys:
        raise DatagenError(
            "give --sys (with optional --domain), or --auto / --domain-manifest "
            "for a batch"
        )
    if _is_multi(args.sys):
        raise DatagenError(
            "--sys takes a single system here; use `--auto ALL|s1,s2,...` to "
            "create a domain for many systems at once"
        )
    args.domain = args.domain or args.sys
    _reject_reserved_name(args.domain, "domain")
    descriptor.validate_name(args.domain)
    return _domain_create_single(args, peeked_sys)


def _system_can_seed(sys_name: str, model: str, sys_dir: str | None) -> bool:
    """Whether system `sys_name` can be seeded with `model`: it must accept
    models/data (`supports_models`), its kind must be one the model targets, and —
    for SQL systems — the model must ship a `<slug>.ddl` (a model with no DDL for
    that engine, e.g. no `clickhouse.ddl`, leaves an empty domain instead of
    failing). Used by `--auto` to make --model/--data a no-op elsewhere."""
    doc = descriptor.load(store.system_path(sys_name, sys_dir))
    sysobj = systems.get(doc["system"])
    if not sysobj.supports_models:
        return False
    sys_kind = descriptor.resolve(doc).get("SYSTEM_KIND", "")
    if sys_kind and sys_kind not in data_gen.model_kinds(model):
        return False
    if getattr(sysobj, "supports_sql", False):
        if not (data_gen.model_dir(model) / f"{sysobj.slug}.ddl").is_file():
            return False
    return True


def _system_loads_data(sys_name: str, sys_dir: str | None) -> bool:
    """Whether system `sys_name` loads row data (`--data`). False for a system
    that accepts `--model` but seeds no rows (e.g. druid), so `--auto` gives it a
    model-only domain instead of failing on `--data`."""
    doc = descriptor.load(store.system_path(sys_name, sys_dir))
    return systems.get(doc["system"]).supports_data


def _domain_common_argv(args) -> list[str]:
    """The store/secret/prefix/force flags to forward from a batch domain-create to
    each per-item single create. `--domain-prefix` is forwarded only when given
    explicitly, so a manifest replay defaults to each run's own prefix."""
    common: list[str] = []
    if args.sys_dir:
        common += ["--sys-dir", args.sys_dir]
    if args.domain_dir:
        common += ["--domain-dir", args.domain_dir]
    if args.secret_env and args.secret_env != crypto.ENV_VAR:
        common += ["--secret-env", args.secret_env]
    if args.domain_prefix is not None:
        common += ["--domain-prefix", args.domain_prefix]
    if args.force:
        common.append("-f")
    return common


def _domain_create_auto(args) -> int:
    """`--auto ALL|S1,S2,...`: create one domain per system (ALL or a comma list),
    each named after its system, seeding --model/--data where the system supports
    it. A system that can't take the model/data gets an empty domain and a warning
    (to stderr) that it was left unseeded. Reuses the single-create path per
    system, run concurrently (each in its own forked process); the batch fails
    fast — on the first failure no more domains are launched (those already in
    flight finish), and the command exits non-zero."""
    if args.data and not args.model:
        raise DatagenError("--data requires --model")
    common = _domain_common_argv(args)

    def _create_one(sname: str) -> str:
        argv = [*common, "--sys", sname]
        seed = bool(args.model) and _system_can_seed(sname, args.model, args.sys_dir)
        if args.model and not seed:
            print(
                f">>> {sname}: model {args.model!r} is not seedable here; "
                f"creating an empty domain",
                file=sys.stderr,
            )
        # A seedable system that loads no rows (e.g. druid) takes the model but
        # not the data — creating a model-only domain rather than failing --data.
        loads_data = (
            bool(args.data) and seed and _system_loads_data(sname, args.sys_dir)
        )
        if args.data and seed and not loads_data:
            print(
                f">>> {sname}: loads no data; creating with model "
                f"{args.model!r} only (no --data)",
                file=sys.stderr,
            )
        if seed:
            argv += ["--model", args.model]
            if loads_data:
                argv += ["--data", args.data]
        _domain_create(argv)
        if not args.model:
            return ""
        if not seed:
            return f"empty (model {args.model!r} not seeded)"
        if args.data and not loads_data:
            return f"model {args.model!r} (no data loaded)"
        return ""

    names = _expand_names(args.auto, store.system_dir(args.sys_dir), "sys", "--auto")
    _batch_banner("creating", len(names), "domain")
    results: list[tuple[str, str, bool, str]] = []

    def on_done(done: int, total: int, sname: str, outcome: dispatch.Outcome) -> None:
        _emit_captured(outcome)
        detail = outcome.value if outcome.ok else outcome.error
        results.append((sname, sname, outcome.ok, detail))
        _progress(
            done,
            total,
            outcome.ok,
            "ok  " if outcome.ok else "FAIL",
            sname,
            outcome.seconds,
            detail,
        )

    dispatch.run(names, _create_one, on_done=on_done, stop_on_error=True)
    return _report_created_batch("domain", results, quiet=True, total=len(names))


def _domain_create_entry(entry: dict, common: list[str]) -> tuple[str, str, bool, str]:
    """Create one domain from a manifest entry via the single-create path,
    returning a (name, sys, ok, detail) result on success. A failure RAISES
    (batch create fails fast — `_batch_create` turns the raise into a FAIL result
    and stops launching more); the reconciling `--sync` path catches it to keep
    going. Each entry's `name` maps to `--domain` and `sys` to `--sys` (the rest
    mirror the create flags)."""
    e = dict(entry)
    name = str(e.pop("name", None) or "")
    sys_name = str(e.pop("sys", None) or "")
    if not name:
        raise DatagenError("manifest entry has no `name` (domain name)")
    argv = [
        *common,
        "--sys",
        sys_name,
        "--domain",
        name,
        *manifest.entry_to_argv(e),
    ]
    _domain_create(argv)
    return (name, sys_name, True, "")


def _domain_create_from_manifest(args) -> int:
    """`--domain-manifest FILE|-`: create every domain entry in a `tdx-domain`
    manifest, reusing the single-create path per entry, run concurrently (each in
    its own forked process). Fails fast: the first failed entry stops the batch
    (in-flight entries finish) and the command exits non-zero."""
    common = _domain_common_argv(args)
    entries = list(manifest.read(args.domain_manifest))
    results = _batch_create(
        entries, lambda e: _domain_create_entry(e, common), "domain"
    )
    return _report_created_batch("domain", results, quiet=True, total=len(entries))


def _domain_sync_from_manifest(args) -> int:
    """`--domain-manifest FILE|- --sync`: reconcile the store to the manifest —
    create every domain the manifest lists but the store lacks, destroy every domain
    the store has but the manifest omits, and leave domains present in both untouched
    (`--sync` never updates an existing domain). Continue-on-error: each failed
    create/destroy is reported and the rest still run."""
    common = _domain_common_argv(args)
    store_names = {
        name for name, _ in _store_entries(store.domain_dir(args.domain_dir), "env")
    }
    manifest_names: set[str] = set()
    created: list[tuple[str, str, bool, str]] = []
    unchanged: list[str] = []
    for entry in manifest.read(args.domain_manifest):
        name = str(entry.get("name") or "")
        if not name:
            print(">>> skipping manifest entry with no `name`", file=sys.stderr)
            continue
        manifest_names.add(name)
        if name in store_names:
            unchanged.append(name)
        else:
            created.append(
                _create_entry_tolerant(
                    lambda e: _domain_create_entry(e, common),
                    entry,
                    name,
                    str(entry.get("sys") or "?"),
                )
            )
    destroyed: list[tuple[str, bool, str]] = []
    for name in sorted(store_names - manifest_names):
        try:
            detail = _domain_destroy_one(name, args.domain_dir, args.sys_dir)
            destroyed.append((name, True, detail))
        except Exception as exc:  # noqa: BLE001 - one bad target must not stop the rest
            destroyed.append((name, False, str(exc)))
    return _report_synced("domain", created, destroyed, unchanged)


def _domain_dump(rest: list[str]) -> int:
    """`tdx-domain dump --domain-manifest FILE|-`: write a manifest of every domain
    in the store (name + system + model/data), replayable with `tdx-domain create
    --domain-manifest` and droppable with `tdx-domain destroy --domain-manifest`.
    The entity prefix is intentionally omitted, so a manifest replays under each
    user's own `<user>_<build>_`."""
    p = _ArgumentParser(
        prog="tdx-domain dump",
        add_help=True,
        description="Write a YAML manifest of every domain in the store dir.",
    )
    p.add_argument(
        "--domain-manifest",
        dest="domain_manifest",
        required=True,
        metavar="FILE|-",
        help="output manifest path (- for stdout)",
    )
    _add_domain_dir(p)
    _add_secret_env(p)
    args = p.parse_args(rest)
    _apply_secret(args)
    entries = [
        manifest.domain_entry_from_descriptor(doc)
        for _, doc in _store_entries(store.domain_dir(args.domain_dir), "env")
    ]
    manifest.write(args.domain_manifest, entries)
    return 0


def _install_model_and_data(
    domain_path: str, model: str | None, data: str | None, *, force: bool
) -> None:
    """Install a model's schema (and optionally load its data) into an existing
    domain — the shared tail of create / seed / recreate. `data create` installs
    the schema; `data load` fills it.

    `data load` runs with force=False: `data create` has just installed the schema
    into the (freshly created / recreated) domain, so re-running the DDL here would
    execute it TWICE. SQL backends whose force-load re-runs the DDL only survive
    that because their table-drop clears the first run's tables first — db2/oracle
    don't (their reflect can't see the domain schema), so the second CREATE TABLE
    collides. Loading without force skips the redundant re-install entirely."""
    if model:
        cli._run_data_create(
            argparse.Namespace(model=model, domain=domain_path, force=force)
        )
    if data:
        # Fixed-data models have no size (they'd reject one); sizeable models
        # take the requested bucket.
        size = data if data_gen.is_sizeable(model) else None
        # Kafka data-load writes per-entity schema sidecars to --schemas; they're
        # only needed to serialize the produce, so hold them in a temp dir that's
        # discarded here rather than littering the domain store next to the
        # descriptor. (Non-kafka loads ignore --schemas and never touch it.)
        with tempfile.TemporaryDirectory(prefix="tdx-schemas-") as schemas_dir:
            cli._run_data_load(
                argparse.Namespace(
                    model=model,
                    domain=domain_path,
                    size=size,
                    force=False,
                    format=None,
                    chunks=None,
                    ser="json",
                    schemas=schemas_dir,
                    use_schema_registry=False,
                )
            )


def _domain_create_single(args, peeked_sys) -> int:
    """Create one domain from the parsed create namespace (optionally seeding
    its --model/--data). `-f`/--force on an existing domain means *destroy it
    first* (dropping the entity over the OWNING SYSTEM's connection, via the
    domain's `sys_name` — see `_domain_meta_via_system`), then create fresh; a
    prior entity is torn down authoritatively, never clobbered in place."""
    if args.data and not args.model:
        raise DatagenError("--data requires --model")
    sys_path = _sys_target(args)
    domain_path = _domain_target(args)
    if args.force and Path(domain_path).exists():
        print(
            f">>> --force: destroying existing domain '{args.domain}' "
            f"(via its system) before recreating"
        )
        _domain_destroy_one(args.domain, args.domain_dir, args.sys_dir)
    _prepare_descriptor_path(domain_path, args.force)

    sysdoc = descriptor.load(sys_path)
    _require_role(sysdoc, "sys", "tdx-system", "tdx-domain")
    sysmeta = descriptor.resolve(sysdoc)
    _setenv(sysmeta)
    _guard_container_fresh(sysmeta)
    slug = sysdoc["system"]
    started = descriptor.now_stamp()

    sysobj = peeked_sys or systems.get(slug)
    if (args.model or args.data) and not sysobj.supports_models:
        raise DatagenError(f"{slug} does not support --model / --data")
    if args.data and not sysobj.supports_data:
        raise DatagenError(
            f"{slug} does not load data (--model records the model; --data is "
            f"not supported)"
        )
    # `--name-in-system` names the external entity container verbatim (overriding
    # the default <domain-prefix><domain>); it is stamped as external_name, so a
    # later recreate rebuilds the SAME container.
    external = getattr(args, "name_in_system", None) or _external_name(
        args.domain, args.domain_prefix
    )
    sysctx = SystemContext(slug, sysdoc["name"], sysmeta)
    spec = CreateDomainSpec(
        name=args.domain,
        out=domain_path,
        force=args.force,
        options=sysobj.resolve_domain_options(args),
        external_name=external,
    )
    # The FULL descriptor is planned and written BEFORE the domain resource is
    # provisioned, so a failed provision is torn down from it and retried.
    settings = sysobj.plan_domain(sysctx, spec)
    descriptor.write(sysobj, "env", domain_path, settings)
    descriptor.stamp(
        domain_path,
        name=args.domain,
        started=started,
        model=args.model,
        data=args.data,
        external_name=external,
        sys_name=args.sys,
        entity_term=sysobj.domain_flag,
    )
    ctx = _domain_ctx_from_descriptor(slug, domain_path, args.sys_dir)

    # Provision the domain resource AND seed its optional model/data as ONE
    # retried unit: if either fails, the resource is destroyed and the whole
    # thing re-attempted from the (unchanged) descriptor — the pinned settings
    # mean the re-provisioned domain is identical, so the seed replays cleanly.
    def _provision_and_seed() -> None:
        sysobj.provision_domain(sysctx, spec, settings)
        _install_model_and_data(domain_path, args.model, args.data, force=args.force)

    _provision_with_retry(
        "domain",
        sysobj,
        args.domain,
        domain_path,
        ctx,
        _provision_and_seed,
    )
    # Announced only once the domain (and any model/data) is fully in place.
    _print_created("domain", domain_path, _domain_hint(args))
    return 0


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
    _add_latch(p)
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
    if args.data and not sysobj.supports_data:
        raise DatagenError(f"{slug} does not load data (--data is not supported)")
    _guard_container_fresh(descriptor.resolve(doc))
    # Reuse the same pipeline `create --model` runs, with force=False so an
    # already-populated schema/tables make it fail rather than clobber.
    _install_model_and_data(path, args.model, args.data, force=False)
    # Record the model/data so `list` shows it and a second seed is refused.
    descriptor.stamp(
        path,
        name=doc["name"],
        started=doc.get("started", ""),
        model=args.model,
        data=args.data,
    )
    console.out()
    console.good(
        f">>> seeded domain '{doc['name']}' ({slug}) with "
        f"model={args.model} data={args.data or '-'}"
    )
    console.out()
    return 0


def _domain_recreate(rest: list[str]) -> int:
    """`tdx-domain recreate --domain NAME|ALL|N1,N2,...`: reset domain(s) to a
    clean initial state — re-provision the resource fresh and replay the
    model/data each recorded at create time (so a seeded domain comes back with
    its DDL + data, an empty one comes back empty). Works over `ALL` / a comma
    list; a per-domain failure is reported and the rest still run."""
    args = _locator_ns("tdx-domain recreate", "domain", rest, with_latch=True)
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
    console.out()
    console.good(f">>> recreated domain '{args.domain}' {detail}")
    console.out()
    return 0


def _domain_names_from_manifest(
    manifest_path: str, domain_dir: str | None
) -> list[str]:
    """Domain names (each entry's ``name``) in a `tdx-domain` manifest that exist in
    the store; an entry whose domain is missing from the store is warned to stderr
    and skipped."""
    names: list[str] = []
    for entry in manifest.read(manifest_path):
        name = str(entry.get("name") or "")
        if not name:
            print(">>> skipping manifest entry with no `name`", file=sys.stderr)
            continue
        if not store.domain_path(name, domain_dir).exists():
            print(f">>> skipping domain '{name}': not in the store", file=sys.stderr)
            continue
        names.append(name)
    return names


def _domain_destroy(rest: list[str]) -> int:
    args = _domain_batch_ns(
        "tdx-domain destroy", rest, with_force=True, with_latch=True
    )
    if args.domain_manifest:
        names = _domain_names_from_manifest(args.domain_manifest, args.domain_dir)
        return _multi_apply(
            names,
            "domain",
            lambda n: _domain_destroy_one(
                n, args.domain_dir, args.sys_dir, force=args.force
            ),
            past="destroyed",
        )
    if args.auto is not None:
        names = _expand_names(
            args.auto, store.domain_dir(args.domain_dir), "env", "--auto"
        )
        return _multi_apply(
            names,
            "domain",
            lambda n: _domain_destroy_one(
                n, args.domain_dir, args.sys_dir, force=args.force
            ),
            past="destroyed",
        )
    _require_single_domain(args.domain)
    detail = _domain_destroy_one(
        args.domain, args.domain_dir, args.sys_dir, force=args.force
    )
    console.out()
    console.good(f">>> destroyed domain '{args.domain}' {detail}")
    console.out()
    return 0


def _domain_status(rest: list[str]) -> int:
    args = _domain_batch_ns("tdx-domain status", rest)
    if args.domain_manifest:
        names = _domain_names_from_manifest(args.domain_manifest, args.domain_dir)
        return _multi_status(
            names, "domain", lambda n: _domain_status_one(n, args.domain_dir)
        )
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
    print(descriptor.info(doc, args.format, prefix=args.prefix))
    reason = _container_stale(descriptor.resolve(doc))
    if reason:
        print(f">>> STALE: {reason}", file=sys.stderr)
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
            "--domain takes a single domain here; use --auto ALL|n1,n2,... for a batch"
        )
    descriptor.validate_name(name)


def _domain_batch_ns(
    prog: str, rest: list[str], *, with_force: bool = False, with_latch: bool = False
) -> argparse.Namespace:
    """Parse a batch-capable domain verb (destroy / status): a single `--domain
    NAME`, `--auto ALL|N1,N2,...` for a batch (every domain in the store, or a
    comma list of names), or `--domain-manifest FILE|-` for every domain in a
    `tdx-domain` manifest — mirroring `create`'s modes. Exactly one is required.
    `with_force` adds `-f/--force` and `with_latch` adds `--latch` (destroy only).
    Applies the secret before returning."""
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
    grp.add_argument(
        "--domain-manifest",
        dest="domain_manifest",
        metavar="FILE|-",
        help="batch: every domain in a `tdx-domain` manifest (- reads stdin); a "
        "domain missing from the store is warned and skipped",
    )
    if with_force:
        p.add_argument(
            "-f",
            "--force",
            action="store_true",
            help="remove the descriptor even if the backend teardown fails "
            "(warns instead of failing)",
        )
    if with_latch:
        _add_latch(p)
    _add_domain_dir(p)
    _add_sys_dir(p)
    _add_secret_env(p)
    args = p.parse_args(rest)
    _apply_secret(args)
    return args


def _locator_ns(
    prog: str,
    kind: str,
    rest: list[str],
    *,
    want_format: bool = False,
    with_latch: bool = False,
) -> argparse.Namespace:
    """Parse a single-target verb's `--sys`/`--domain` name (+ its `--…-dir` and
    `--secret-env`), `--format` for info, and `--latch` for recreate. `kind` is
    "sys" or "domain". Applies the secret before returning, so callers can do I/O
    immediately."""
    p = _ArgumentParser(
        prog=prog, add_help=True, description=f"Reads {_ENCRYPTED_DESC}."
    )
    if kind == "sys":
        _add_sys_locator(p)
    else:
        _add_domain_locator(p)
    if with_latch:
        _add_latch(p)
    if want_format:
        p.add_argument(
            "--format",
            default="json",
            choices=list(descriptor.VALID_INFO_FORMATS),
            help="json (default) | export | unset",
        )
        p.add_argument(
            "--prefix",
            default=None,
            metavar="PREFIX",
            help="if given, every emitted key is <PREFIX>__<KEY> "
            "(json object keys, export/unset variable names)",
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
    "dump": _sys_dump,
    "list": _sys_list,
    "destroy": _sys_destroy,
    "status": _sys_status,
    "restart": _sys_restart,
    "info": _sys_info,
    "pause": _sys_pause,
    "resume": _sys_resume,
    "secrets": _sys_secrets,
    "connect": _sys_connect,
}
_DOMAIN_VERBS = {
    "create": _domain_create,
    "dump": _domain_dump,
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


# Verbs whose (chatty) detail is redirected to a timestamped log under the store
# dir while the console shows only brief, colored progress + counters (see the
# `console` module). The quick read-only / machine-output verbs (list / info /
# dump / types / secrets / fetch-images / connect / pause / resume) keep printing
# straight to the terminal.
_LOGGED_VERBS = {"create", "destroy", "status", "restart", "recreate", "seed"}

# Verbs that accept `--latch FILE` (see `_add_latch`): the long-running ones a
# caller may want to run in the background and wait on.
_LATCH_VERBS = {"create", "destroy", "seed", "recreate"}


def _latch_files(latch: Path) -> tuple[Path, Path]:
    """The (``<latch>.ok``, ``<latch>.err``) outcome files for a `--latch` path."""
    return Path(f"{latch}.ok"), Path(f"{latch}.err")


def _clear_latch(latch: Path) -> None:
    """Remove any marker files a previous run left at this path, so a waiter only
    ever observes markers produced by this invocation."""
    ok, err = _latch_files(latch)
    for p in (latch, ok, err):
        try:
            p.unlink(missing_ok=True)
        except OSError:
            pass


def _touch_latch(latch: Path, *, ok: bool) -> None:
    """Signal completion for a background caller: create the outcome file
    (``<latch>.ok`` on success, ``<latch>.err`` on failure) FIRST, then touch
    ``<latch>`` LAST — so a caller that waits for ``<latch>`` to appear is
    guaranteed the outcome file is already there to read. Best-effort: a
    filesystem error here is warned but must not mask the command's own result
    (nor turn a successful provision into a failure)."""
    ok_file, err_file = _latch_files(latch)
    try:
        latch.parent.mkdir(parents=True, exist_ok=True)
        (ok_file if ok else err_file).touch()
        latch.touch()
    except OSError as exc:
        print(f">>> warning: could not write latch {latch}: {exc}", file=sys.stderr)


def _run_dispatch(cmd: str, verbs: dict, argv: list[str], log_dir: Path) -> int:
    """Dispatch `argv`, routing a logged verb's detail into a fresh timestamped
    log under `log_dir` (the console then shows only brief, colored progress);
    every other verb prints straight to the terminal. A bare `-h`/`--help` is
    left un-redirected so no empty log is minted just to print help.

    A `--latch FILE` on a create/destroy/seed/recreate verb makes the run
    waitable: the outcome file `FILE.ok`/`FILE.err` is written first, then `FILE`
    is touched last, so a background caller can wait for `FILE` then read which
    outcome is present (see `_add_latch`)."""
    verb = argv[0] if argv else ""
    wants_help = any(a in ("-h", "--help") for a in argv[1:])
    latch = None
    if verb in _LATCH_VERBS and not wants_help:
        raw = _extract_opt(argv, "--latch")
        if raw:
            latch = Path(raw)
            _clear_latch(latch)
    logged = verb in _LOGGED_VERBS and verb in verbs and not wants_help
    try:
        if logged:
            with console.activate(log_dir):
                rc = _dispatch(cmd, verbs, argv)
        else:
            rc = _dispatch(cmd, verbs, argv)
    except BaseException:
        if latch is not None:
            _touch_latch(latch, ok=False)
        raise
    if latch is not None:
        _touch_latch(latch, ok=(rc == 0))
    return rc


def _main_system() -> int:
    import sys

    argv = sys.argv[1:]
    # Descriptor-free verbs (secrets) need no managed venv — keep them running
    # in the zero-dependency launcher.
    if not (argv and argv[0] in _NO_DESCRIPTOR_VERBS):
        _venv.bootstrap(module="tabsdatax.itest.tdksystems.tdx_system")
    return _run_dispatch(
        "tdx-system",
        _SYS_VERBS,
        argv,
        store.system_dir(_extract_opt(argv, "--sys-dir")),
    )


def _main_domain() -> int:
    _venv.bootstrap(module="tabsdatax.itest.tdksystems.tdx_domain")
    import sys

    argv = sys.argv[1:]
    return _run_dispatch(
        "tdx-domain",
        _DOMAIN_VERBS,
        argv,
        store.domain_dir(_extract_opt(argv, "--domain-dir")),
    )


def main_system() -> None:
    run(_main_system)


def main_domain() -> None:
    run(_main_domain)
