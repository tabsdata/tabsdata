"""`tdx-system` / `tdx-domain` — manage external systems and their domains.

    tdx-system create  --type TYPE --sys NAME [--sys-dir DIR] [type config]
    tdx-system list    [--sys-dir DIR]              # names + types in the store
    tdx-system destroy --sys NAME [--sys-dir DIR]   # tear down + remove descriptor
    tdx-system status  --sys NAME [--sys-dir DIR]
    tdx-system info    --sys NAME [--sys-dir DIR] --format json|export|unset
    tdx-system pause | resume --sys NAME [--sys-dir DIR]  # stop / start container
    tdx-system secrets --get 1p:VAULT --out json|export|unset   # pull creds
    tdx-system connect --remote-sys [user@]HOST:REMOTE_SYS --sys NAME [--sys-dir DIR]
                       --remote-secret-env VAR      # ssh-tunnel; container system;
                                                    # destroy stops the tunnel

    tdx-domain create  --sys NAME [--sys-dir DIR] --domain NAME [--domain-dir DIR] \\
                       [--model MODEL [--data xs|s|m|l|xl]]
    tdx-domain list    [--domain-dir DIR]           # names + types + model/data
    tdx-domain destroy --domain NAME [--domain-dir DIR]
    tdx-domain status  --domain NAME [--domain-dir DIR]
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
import os
import sys
from pathlib import Path

from . import _venv, cli, crypto, data_gen, descriptor, secret, ssh, store, systems
from .errors import DatagenError, run
from .shell import ArgumentParser as _ArgumentParser, find_free_port
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


# --- name-based descriptor locators ---------------------------------------
# Systems and domains are addressed by name; `store` maps a name (+ optional
# --sys-dir/--domain-dir) to its descriptor path. These add the flags, and
# resolve+validate the name to a path (see `store` for the precedence).

def _add_sys_dir(p: _ArgumentParser) -> None:
    p.add_argument("--sys-dir", dest="sys_dir", default=None, metavar="DIR",
                   help=f"system store dir (default: ${store.BACKENDS_DIR_ENV}"
                        f"/systems or {store.DEFAULT_BACKENDS_DIR}/systems)")


def _add_domain_dir(p: _ArgumentParser) -> None:
    p.add_argument("--domain-dir", dest="domain_dir", default=None, metavar="DIR",
                   help=f"domain store dir (default: ${store.BACKENDS_DIR_ENV}"
                        f"/domains or {store.DEFAULT_BACKENDS_DIR}/domains)")


def _add_secret_env(p: _ArgumentParser) -> None:
    """The `--secret-env` option carried by every descriptor verb: names the env
    var holding the personal secret, defaulting to $TD_BACKENDS_CRYPTO_KEY."""
    p.add_argument("--secret-env", dest="secret_env", default=crypto.ENV_VAR,
                   metavar="VAR",
                   help=f"env var holding the personal secret that "
                        f"encrypts/decrypts the descriptor "
                        f"(default: {crypto.ENV_VAR})")


def _apply_secret(ns) -> None:
    """Point crypto at the verb's `--secret-env` and require it: fails if that
    env var (default $TD_BACKENDS_CRYPTO_KEY) is unset or shorter than 12 chars.
    Every descriptor verb calls this right after parsing, before any I/O."""
    crypto.use_secret_env(ns.secret_env)
    crypto.require_secret()


def _add_sys_locator(p: _ArgumentParser) -> None:
    p.add_argument("--sys", required=True, metavar="NAME",
                   help="system name (a file in the system store dir)")
    _add_sys_dir(p)


def _add_domain_locator(p: _ArgumentParser) -> None:
    p.add_argument("--domain", required=True, metavar="NAME",
                   help="domain name (a file in the domain store dir)")
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
    return (f"--domain {ns.domain}"
            + (f" --domain-dir {ns.domain_dir}" if ns.domain_dir else ""))


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
    """List the `role` descriptors in `directory`: name + type, plus model/data
    for domains. Files that aren't a readable descriptor of that role (sidecars,
    a system file in the domain store, ones under a different secret) are
    skipped. Missing / empty dir prints a friendly note."""
    entries: list[tuple[str, dict]] = []
    if directory.is_dir():
        for path in sorted(directory.iterdir()):
            if not path.is_file():
                continue
            try:
                doc = descriptor.load(path)
            except DatagenError:
                continue   # not a descriptor / wrong secret / sidecar
            if doc.get("role") == role:
                entries.append((path.name, doc))
    print()
    if not entries:
        print(f"no {kind}s in {directory}")
        print()
        return
    print(f"{kind}s in {directory}:")
    width = max(len(name) for name, _ in entries)
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
        ctx = (SystemContext(sysobj.slug, name, meta) if kind == "system"
               else DomainContext(sysobj.slug, name, meta))
        if kind == "system":
            sysobj.destroy_system(ctx)
        else:
            sysobj.destroy_domain(ctx)
    except Exception as exc:
        print(f">>> cleanup: could not fully destroy {kind} '{name}': {exc}",
              file=sys.stderr)
    _safe_remove(path)


# =====================================================================
# tdx-system
# =====================================================================

def _sys_create(rest: list[str]) -> int:
    p = _ArgumentParser(
        prog="tdx-system create", add_help=True,
        description="Provision an external system and write its descriptor.\n"
                    "The descriptor is an encrypted file (AES-256-GCM in PEM\n"
                    "armor, keyed by $TD_BACKENDS_CRYPTO_KEY).")
    p.add_argument("--type", required=True, metavar="TYPE",
                   help=f"external system type ({_all_types()})")
    _add_sys_locator(p)
    _add_secret_env(p)
    p.add_argument("-f", "--force", action="store_true",
                   help="recreate if it already exists")
    # Pull --type out of the raw args (not via parse_known_args, which would
    # act on a `-h` before the type-specific options are added) so that
    # `tdx-system create --type s3 -h` lists s3's flags (--bucket / --region / …).
    the_type = _extract_opt(rest, "--type")
    if the_type is not None:
        if the_type not in systems.all_slugs():
            raise DatagenError(
                f"unknown --type {the_type!r} "
                f"(known: {', '.join(systems.all_slugs())})")
        return _sys_create_unified(p, rest, the_type)
    p.parse_args(rest)   # no --type: -h prints help, else errors (required)
    raise DatagenError("--type is required")   # unreachable (argparse exits)


def _sys_create_unified(p: _ArgumentParser, rest: list[str], slug: str) -> int:
    """`tdx-system create` for a system served by the unified registry — no
    category branch; the descriptor is persisted by descriptor.write."""
    sysobj = systems.get(slug)
    sysobj.add_create_options(p)
    args = p.parse_args(rest)
    _apply_secret(args)
    out = _sys_target(args)
    _prepare_descriptor_path(out, args.force)
    started = descriptor.now_stamp()
    baked = sysobj.create_system(CreateSystemSpec(
        name=args.sys, out=out, force=args.force,
        settings=sysobj.resolve_create_settings(args),
        host_port=getattr(args, "port", None),
        ready_timeout=getattr(args, "ready_timeout", 300)))
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


def _sys_destroy(rest: list[str]) -> int:
    args = _locator_ns("tdx-system destroy", "sys", rest)
    path = _sys_target(args)
    doc = descriptor.load(path)
    _require_role(doc, "sys", "tdx-system", "tdx-domain")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    slug = doc["system"]
    systems.get(slug).destroy_system(SystemContext(slug, doc["name"], meta))
    os.remove(path)
    print()
    print(f">>> destroyed system '{doc['name']}' ({slug}); removed {path}")
    print()
    return 0


def _sys_status(rest: list[str]) -> int:
    args = _locator_ns("tdx-system status", "sys", rest)
    doc = descriptor.load(_sys_target(args))
    _require_role(doc, "sys", "tdx-system", "tdx-domain")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    slug = doc["system"]
    h = systems.get(slug).system_status(SystemContext(slug, doc["name"], meta))
    return _report_status(h.ok, h.detail)


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
            f"({doc['name']} is a {doc['category']} system)")
    print()
    print(f">>> {verb}d {doc['name']} ({slug}): {state}")
    print()
    return 0


def _sys_secrets(rest: list[str]) -> int:
    """Fetch credentials from a pluggable secret provider and print them for
    the shell. Descriptor-free — no $TD_BACKENDS_CRYPTO_KEY, no managed venv; the
    provider (e.g. `1p`) just shells out to its own CLI (e.g. `op`)."""
    p = _ArgumentParser(
        prog="tdx-system secrets", add_help=True,
        description="Fetch secrets from a pluggable provider and print them to "
                    "stdout as a JSON dict, `export` lines, or `unset` lines.\n"
                    "Apply them to the current shell with `source`/`eval`:\n"
                    "  source <(tdx-system secrets --get 1p:dev_xp --out export)\n"
                    "  source <(tdx-system secrets --get 1p:dev_xp --out unset)\n"
                    "The names follow <ITEM>__<FIELD>, matching the "
                    "$<PREFIX>__<OPT> env vars `tdx-system create` reads.",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--get", required=True, metavar="PROVIDER:REF",
                   help="secret source as <PROVIDER>:<REF>. Supported providers: "
                        "1p (1Password) — e.g. --get 1p:dev_xp reads vault 'dev_xp'")
    p.add_argument("--out", choices=secret._OUT_FORMATS, default="export",
                   help="output format (default: export). json is the portable "
                        "path for Windows / tooling")
    args = p.parse_args(rest)
    text = secret.render(secret.fetch(args.get), args.out)
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
        prog="tdx-system connect", add_help=True,
        description="Open an SSH tunnel to a remote container system and write a "
                    "local system descriptor reaching it via localhost:<port>. "
                    "Container systems only: cloud and SaaS systems have no TCP "
                    "endpoint to tunnel and are rejected.\n"
                    "The server does nothing SSH-specific: the endpoint and the "
                    "path to its (encrypted) system descriptor are given here as "
                    "an scp-style [user@]HOST:PATH.",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--remote-sys", required=True,
                   metavar="[user@]HOST:REMOTE_SYS_PATH",
                   help="ssh endpoint + path to the remote (encrypted) system "
                        "descriptor")
    _add_sys_locator(p)   # local handle: --sys NAME [--sys-dir DIR]
    _add_secret_env(p)    # the local descriptor is written under this secret
    p.add_argument("--ssh-port", type=int, default=22,
                   help="ssh port (default: 22)")
    p.add_argument("--identity", default=None, metavar="KEYFILE",
                   help="ssh identity file (ssh -i)")
    p.add_argument("--remote-secret-env", required=True, metavar="VAR",
                   help="env var holding the secret that decrypts the REMOTE "
                        "descriptor (required); the local descriptor is written "
                        "under --secret-env")
    p.add_argument("-f", "--force", action="store_true",
                   help="overwrite the local descriptor if it exists")
    args = p.parse_args(rest)
    _apply_secret(args)   # local (client) secret
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
            f"(tdx-system connect is container-only)")
    slug = remote_doc["system"]
    sysobj = systems.get(slug)
    if getattr(sysobj, "uses_file_staging", False):
        raise DatagenError(
            f"{slug} is a file-staging system: its staging root is a server-side "
            f"filesystem path a TCP tunnel can't reach — connect is not supported")
    target_port = remote_meta.get("PORT", "")
    if not target_port.isdigit():
        raise DatagenError(
            f"remote descriptor has no numeric PORT to tunnel to (got {target_port!r})")
    name = args.sys   # the local handle's name is its file name (already validated)

    # 2. Allocate a local port and open the background tunnel.
    local_port = find_free_port()
    log_path = Path(local_path).with_suffix("").as_posix() + ".ssh.log"
    print()
    print(f">>> opening ssh tunnel to {ep.target}: localhost:{local_port} -> "
          f"localhost:{target_port} (container {remote_meta.get('CONTAINER', '?')})")
    pid = ssh.open_tunnel(ep, local_port, int(target_port), log_path)

    # 3. Write the local (connected) descriptor under the client's own secret,
    #    pointing HOST/PORT at the tunnel; tear the tunnel down if that fails.
    started = descriptor.now_stamp()
    try:
        descriptor.write_container_system(
            out=local_path, system_type=slug,
            system_kind=remote_meta.get("SYSTEM_KIND", ""), label=sysobj.label,
            container=remote_meta.get("CONTAINER", ""),
            host="localhost", port=str(local_port),
            user=remote_meta.get("USER", ""), password=remote_meta.get("PASSWORD", ""),
            domain_desc=remote_meta.get("DOMAIN_DESC", ""),
            image=remote_meta.get("IMAGE") or None,
            location="ssh-tunnel",
            extra_vars={
                "SSH_HOST": ep.host,
                "SSH_USER": ep.user or "",
                "SSH_PORT": str(ep.port),
                "SSH_TARGET_PORT": target_port,
                "SSH_TUNNEL_PID": str(pid),
            })
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
        prog="tdx-system list", add_help=True,
        description="List the systems in the system store dir (name + type).")
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
    p = _ArgumentParser(
        prog="tdx-domain create", add_help=True,
        description="Create a domain and write its descriptor. Reads and writes "
                    "encrypted files (AES-256-GCM in PEM armor, keyed by "
                    "$TD_BACKENDS_CRYPTO_KEY).")
    _add_sys_locator(p)      # the system to create the domain in
    _add_domain_locator(p)   # the domain to write
    _add_secret_env(p)
    p.add_argument("--model", default=None,
                   help=f"install this model's schema into the domain "
                        f"({_all_models()})")
    p.add_argument("--data", default=None, choices=["xs", "s", "m", "l", "xl"],
                   help="also load model data at this size (requires --model)")
    p.add_argument("-f", "--force", action="store_true",
                   help="recreate if it already exists")
    # Peek --sys / --sys-dir so the target system can contribute its own
    # create-domain options (e.g. bigquery --dataset / --domain-path) before we
    # parse, so `tdx-domain create --sys NAME -h` lists them (mirrors --type
    # peeking in _sys_create). The peek decrypts the system descriptor, so point
    # crypto at --secret-env first (best-effort: a missing secret is caught here
    # and re-raised cleanly by _apply_secret once we've parsed).
    crypto.use_secret_env(_extract_opt(rest, "--secret-env"))
    sys_peek = _extract_opt(rest, "--sys")
    peeked_sys = None
    if sys_peek:
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
    baked = sysobj.create_domain(
        SystemContext(slug, sysdoc["name"], sysmeta),
        CreateDomainSpec(name=args.domain, out=domain_path, force=args.force,
                         options=sysobj.resolve_domain_options(args)))
    # The domain resource (database / schema / prefix) exists now. If writing
    # its descriptor OR the optional schema-install / data-load fails, drop the
    # resource and remove the descriptor so nothing half-created is left behind.
    try:
        descriptor.write(sysobj, "env", domain_path, baked)
        descriptor.stamp(domain_path, name=args.domain, started=started,
                         model=args.model, data=args.data)

        # Optional model + data — reuse the data pipeline (reads the descriptor
        # we just wrote). `data create` installs the schema; `data load` fills it.
        if args.model:
            cli._run_data_create(argparse.Namespace(
                model=args.model, domain=domain_path, force=args.force))
        if args.data:
            # Fixed-data models have no size (they'd reject one); sizeable models
            # take the requested bucket.
            size = args.data if data_gen.is_sizeable(args.model) else None
            schemas_dir = Path(domain_path).with_suffix("").as_posix() + "-schemas"
            cli._run_data_load(argparse.Namespace(
                model=args.model, domain=domain_path, size=size,
                force=args.force, format=None, chunks=None,
                ser="json", schemas=schemas_dir, use_schema_registry=False))
    except BaseException:
        _cleanup_after_failed_create("domain", sysobj, args.domain, domain_path, baked)
        raise
    # Announced only once the domain (and any model/data) is fully in place.
    _print_created("domain", domain_path, _domain_hint(args))
    return 0


def _domain_destroy(rest: list[str]) -> int:
    args = _locator_ns("tdx-domain destroy", "domain", rest)
    path = _domain_target(args)
    doc = descriptor.load(path)
    _require_role(doc, "env", "tdx-domain", "tdx-system")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    slug = doc["system"]
    systems.get(slug).destroy_domain(DomainContext(slug, doc["name"], meta))
    os.remove(path)
    print()
    print(f">>> destroyed domain '{doc['name']}' ({slug}); removed {path}")
    print()
    return 0


def _domain_status(rest: list[str]) -> int:
    args = _locator_ns("tdx-domain status", "domain", rest)
    doc = descriptor.load(_domain_target(args))
    _require_role(doc, "env", "tdx-domain", "tdx-system")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    slug = doc["system"]
    h = systems.get(slug).domain_status(DomainContext(slug, doc["name"], meta))
    return _report_status(h.ok, h.detail)


def _domain_info(rest: list[str]) -> int:
    args = _locator_ns("tdx-domain info", "domain", rest, want_format=True)
    doc = descriptor.load(_domain_target(args))
    _require_role(doc, "env", "tdx-domain", "tdx-system")
    print(descriptor.info(doc, args.format))
    return 0


def _domain_list(rest: list[str]) -> int:
    """List the domains in the domain store dir (name + type + model/data)."""
    p = _ArgumentParser(
        prog="tdx-domain list", add_help=True,
        description="List the domains in the domain store dir "
                    "(name + type, plus the model/data each carries).")
    _add_domain_dir(p)
    _add_secret_env(p)
    args = p.parse_args(rest)
    _apply_secret(args)
    _print_listing("domain", store.domain_dir(args.domain_dir), role="env")
    return 0


# =====================================================================
# small parsers shared by destroy/status/info
# =====================================================================

_ENCRYPTED_DESC = ("the named system/domain's (encrypted) descriptor, keyed by "
                   "$TD_BACKENDS_CRYPTO_KEY")


def _locator_ns(prog: str, kind: str, rest: list[str], *,
                want_format: bool = False) -> argparse.Namespace:
    """Parse a read-verb's `--sys`/`--domain` name (+ its `--…-dir` and
    `--secret-env`), and `--format` for info. `kind` is "sys" or "domain".
    Applies the secret before returning, so callers can do I/O immediately."""
    p = _ArgumentParser(prog=prog, add_help=True,
                        description=f"Reads {_ENCRYPTED_DESC}.")
    if kind == "sys":
        _add_sys_locator(p)
    else:
        _add_domain_locator(p)
    if want_format:
        p.add_argument("--format", required=True,
                       choices=list(descriptor.VALID_INFO_FORMATS),
                       help="json | export | unset")
    _add_secret_env(p)
    args = p.parse_args(rest)
    _apply_secret(args)
    return args


# =====================================================================
# entry points
# =====================================================================

_SYS_VERBS = {"create": _sys_create, "list": _sys_list, "destroy": _sys_destroy,
              "status": _sys_status, "info": _sys_info,
              "pause": _sys_pause, "resume": _sys_resume,
              "secrets": _sys_secrets, "connect": _sys_connect}
_DOMAIN_VERBS = {"create": _domain_create, "list": _domain_list,
                 "destroy": _domain_destroy, "status": _domain_status,
                 "info": _domain_info}

# Verbs that touch no (encrypted) descriptor, so they don't need $TD_BACKENDS_CRYPTO_KEY
# and don't need the managed runtime venv (they only shell out to a provider
# CLI, e.g. `secrets` -> `op`).
_NO_DESCRIPTOR_VERBS = {"secrets"}


def _store_help(cmd: str) -> str:
    """The store-dir fallback blurb for the top-level `tdx-system`/`tdx-domain`
    help (the per-verb `-h` already documents the flags)."""
    sys_line = (f"--sys-dir, else ${store.BACKENDS_DIR_ENV}/systems, "
                f"else {store.DEFAULT_BACKENDS_DIR}/systems")
    dom_line = (f"--domain-dir, else ${store.BACKENDS_DIR_ENV}/domains, "
                f"else {store.DEFAULT_BACKENDS_DIR}/domains")
    if cmd == "tdx-domain":
        return ("Systems/domains are addressed by name in a store dir:\n"
                f"  system: {sys_line}\n"
                f"  domain: {dom_line}")
    return ("Systems are addressed by name in a store dir:\n"
            f"  {sys_line}")


def _dispatch(cmd: str, verbs: dict, argv: list[str]) -> int:
    if not argv or argv[0] in ("-h", "--help"):
        print(f"Usage: {cmd} {{{'|'.join(verbs)}}} [args...]  "
              f"(add -h to a verb for its options)")
        print()
        print(_store_help(cmd))
        return 0 if argv else 2
    verb, rest = argv[0], argv[1:]
    if verb not in verbs:
        raise DatagenError(f"unknown {cmd} subcommand: {verb!r} "
                           f"(expected {', '.join(verbs)})")
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
