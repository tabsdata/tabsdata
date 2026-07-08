"""`tdx-system` / `tdx-domain` — manage external systems and their domains.

    tdx-system create  --type TYPE --sys FILE --name NAME [type config]
    tdx-system destroy --sys FILE          # tear down + remove the descriptor
    tdx-system status  --sys FILE
    tdx-system info    --sys FILE --format json|export|unset
    tdx-system pause | resume --sys FILE   # stop / start a container (data persists)
    tdx-system secrets --get 1p:VAULT --out json|export|unset   # pull creds
    tdx-system connect --remote-sys [user@]HOST:REMOTE_SYS --sys FILE  # ssh-tunnel
                       [--remote-secret-env VAR]           # container system; destroy
                                                           # stops the tunnel

    tdx-domain create  --sys FILE --domain FILE --name NAME \\
                       [--model MODEL [--data xs|s|m|l|xl]]
    tdx-domain destroy --domain FILE
    tdx-domain status  --domain FILE
    tdx-domain info    --domain FILE --format json|export|unset

Each `create` writes a *descriptor* — a JSON document stored as an encrypted
file (AES-256-GCM in PEM armor, keyed by $TD_DEV_PERSONAL_SECRET; see
:mod:`tabsdatax.itest.tdksystems.descriptor` / :mod:`tabsdatax.itest.tdksystems.crypto`). The other
verbs read that file. `--type` names the external system — every backend in the
`container` / `cloud` / `saas` helper packages is a valid type, so adding a new
system is just a new helper module. `--name` is the logical name (an
identifier): it names the resource and prefixes the `info` keys.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from . import cli, crypto, data_gen, descriptor, secret, ssh, systems, _venv
from .errors import DatagenError, run
from .shell import ArgumentParser as _ArgumentParser, find_free_port
from .systems.base import (
    CreateDomainSpec, CreateSystemSpec, DomainContext, SystemContext, Unsupported,
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
    """The value of `--name X` / `--name=X` in argv (first hit), or None. Lets
    `_sys_create` peek at --type before adding the type-specific options."""
    for i, tok in enumerate(argv):
        if tok == name and i + 1 < len(argv):
            return argv[i + 1]
        if tok.startswith(name + "="):
            return tok.split("=", 1)[1]
    return None


def _require_role(doc: dict, role: str, this_cmd: str, other_cmd: str) -> None:
    if doc.get("role") != role:
        raise DatagenError(
            f"{doc.get('name', '?')} is a {doc.get('kind')} descriptor — "
            f"use `{other_cmd}`, not `{this_cmd}`"
        )


def _setenv(meta: dict[str, str]) -> None:
    os.environ.update({k: v for k, v in meta.items() if k != "KIND"})


def _print_created(kind: str, path: str) -> None:
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
    if kind == "domain":
        print(f"    info:  tdx-domain info --domain {path} --format json")
    else:
        print(f"    info:  tdx-system info --sys {path} --format json")
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
                    "armor, keyed by $TD_DEV_PERSONAL_SECRET).")
    p.add_argument("--type", required=True, metavar="TYPE",
                   help=f"external system type ({_all_types()})")
    p.add_argument("--sys", required=True, metavar="FILE",
                   help="path of the (encrypted) system descriptor file to write")
    p.add_argument("--name", required=True,
                   help="logical system name (an identifier); info-key prefix")
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
    descriptor.validate_name(args.name)
    _prepare_descriptor_path(args.sys, args.force)
    started = descriptor.now_stamp()
    baked = sysobj.create_system(CreateSystemSpec(
        name=args.name, out=args.sys, force=args.force,
        settings=sysobj.resolve_create_settings(args),
        host_port=getattr(args, "port", None),
        ready_timeout=getattr(args, "ready_timeout", 300)))
    # The system is provisioned now; if persisting its descriptor fails, tear
    # it back down rather than leaking a running container / created resource.
    try:
        descriptor.write(sysobj, "sys", args.sys, baked)
        descriptor.stamp(args.sys, name=args.name, started=started)
    except BaseException:
        _cleanup_after_failed_create("system", sysobj, args.name, args.sys, baked)
        raise
    _print_created("system", args.sys)
    return 0


def _sys_destroy(rest: list[str]) -> int:
    args = _one_desc_parser("tdx-system destroy", "--sys", rest)
    doc = descriptor.load(args.sys)
    _require_role(doc, "sys", "tdx-system", "tdx-domain")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    slug = doc["system"]
    systems.get(slug).destroy_system(SystemContext(slug, doc["name"], meta))
    os.remove(args.sys)
    print()
    print(f">>> destroyed system '{doc['name']}' ({slug}); removed {args.sys}")
    print()
    return 0


def _sys_status(rest: list[str]) -> int:
    args = _one_desc_parser("tdx-system status", "--sys", rest)
    doc = descriptor.load(args.sys)
    _require_role(doc, "sys", "tdx-system", "tdx-domain")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    slug = doc["system"]
    h = systems.get(slug).system_status(SystemContext(slug, doc["name"], meta))
    return _report_status(h.ok, h.detail)


def _sys_info(rest: list[str]) -> int:
    args = _info_parser("tdx-system info", "--sys", rest)
    doc = descriptor.load(args.sys)
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
    args = _one_desc_parser(f"tdx-system {verb}", "--sys", rest)
    doc = descriptor.load(args.sys)
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
    the shell. Descriptor-free — no $TD_DEV_PERSONAL_SECRET, no managed venv; the
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
    p.add_argument("--sys", required=True, metavar="FILE",
                   help="path of the local (encrypted) system descriptor to write")
    p.add_argument("--name", default=None,
                   help="logical name for the local handle (default: the remote's name)")
    p.add_argument("--ssh-port", type=int, default=22,
                   help="ssh port (default: 22)")
    p.add_argument("--identity", default=None, metavar="KEYFILE",
                   help="ssh identity file (ssh -i)")
    p.add_argument("--remote-secret-env", default=crypto.ENV_VAR, metavar="VAR",
                   help=f"env var holding the secret that decrypts the remote "
                        f"descriptor (default: {crypto.ENV_VAR}); the local "
                        f"descriptor is written under ${crypto.ENV_VAR}")
    p.add_argument("-f", "--force", action="store_true",
                   help="overwrite the local descriptor if it exists")
    args = p.parse_args(rest)
    _prepare_descriptor_path(args.sys, args.force)

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
    name = args.name or remote_doc.get("name") or slug
    descriptor.validate_name(name)

    # 2. Allocate a local port and open the background tunnel.
    local_port = find_free_port()
    log_path = Path(args.sys).with_suffix("").as_posix() + ".ssh.log"
    print()
    print(f">>> opening ssh tunnel to {ep.target}: localhost:{local_port} -> "
          f"localhost:{target_port} (container {remote_meta.get('CONTAINER', '?')})")
    pid = ssh.open_tunnel(ep, local_port, int(target_port), log_path)

    # 3. Write the local (connected) descriptor under the client's own secret,
    #    pointing HOST/PORT at the tunnel; tear the tunnel down if that fails.
    started = descriptor.now_stamp()
    try:
        descriptor.write_container_system(
            out=args.sys, system_type=slug,
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
        descriptor.stamp(args.sys, name=name, started=started)
    except BaseException:
        ssh.close_tunnel(pid)
        _safe_remove(args.sys)
        raise
    _print_created("system", args.sys)
    return 0


# =====================================================================
# tdx-domain
# =====================================================================

def _domain_create(rest: list[str]) -> int:
    p = _ArgumentParser(
        prog="tdx-domain create", add_help=True,
        description="Create a domain and write its descriptor. Reads and writes "
                    "encrypted files (AES-256-GCM in PEM armor, keyed by "
                    "$TD_DEV_PERSONAL_SECRET).")
    p.add_argument("--sys", required=True, metavar="FILE",
                   help="the (encrypted) system descriptor file to create the domain in")
    p.add_argument("--domain", required=True, metavar="FILE",
                   help="path of the (encrypted) domain descriptor file to write")
    p.add_argument("--name", required=True,
                   help="logical domain name (an identifier); info-key prefix")
    p.add_argument("--model", default=None,
                   help=f"install this model's schema into the domain "
                        f"({_all_models()})")
    p.add_argument("--data", default=None, choices=["xs", "s", "m", "l", "xl"],
                   help="also load model data at this size (requires --model)")
    p.add_argument("-f", "--force", action="store_true",
                   help="recreate if it already exists")
    args = p.parse_args(rest)
    if args.data and not args.model:
        raise DatagenError("--data requires --model")
    descriptor.validate_name(args.name)
    _prepare_descriptor_path(args.domain, args.force)

    sysdoc = descriptor.load(args.sys)
    _require_role(sysdoc, "sys", "tdx-system", "tdx-domain")
    sysmeta = descriptor.resolve(sysdoc)
    _setenv(sysmeta)
    slug = sysdoc["system"]
    started = descriptor.now_stamp()

    sysobj = systems.get(slug)
    baked = sysobj.create_domain(
        SystemContext(slug, sysdoc["name"], sysmeta),
        CreateDomainSpec(name=args.name, out=args.domain, force=args.force))
    # The domain resource (database / schema / prefix) exists now. If writing
    # its descriptor OR the optional schema-install / data-load fails, drop the
    # resource and remove the descriptor so nothing half-created is left behind.
    try:
        descriptor.write(sysobj, "env", args.domain, baked)
        descriptor.stamp(args.domain, name=args.name, started=started,
                         model=args.model, data=args.data)

        # Optional model + data — reuse the data pipeline (reads the descriptor
        # we just wrote). `data create` installs the schema; `data load` fills it.
        if args.model:
            cli._run_data_create(argparse.Namespace(
                model=args.model, domain=args.domain, force=args.force))
        if args.data:
            # Fixed-data models have no size (they'd reject one); sizeable models
            # take the requested bucket.
            size = args.data if data_gen.is_sizeable(args.model) else None
            schemas_dir = Path(args.domain).with_suffix("").as_posix() + "-schemas"
            cli._run_data_load(argparse.Namespace(
                model=args.model, domain=args.domain, size=size,
                force=args.force, format=None, chunks=None,
                ser="json", schemas=schemas_dir, use_schema_registry=False))
    except BaseException:
        _cleanup_after_failed_create("domain", sysobj, args.name, args.domain, baked)
        raise
    # Announced only once the domain (and any model/data) is fully in place.
    _print_created("domain", args.domain)
    return 0


def _domain_destroy(rest: list[str]) -> int:
    args = _one_desc_parser("tdx-domain destroy", "--domain", rest)
    doc = descriptor.load(args.domain)
    _require_role(doc, "env", "tdx-domain", "tdx-system")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    slug = doc["system"]
    systems.get(slug).destroy_domain(DomainContext(slug, doc["name"], meta))
    os.remove(args.domain)
    print()
    print(f">>> destroyed domain '{doc['name']}' ({slug}); removed {args.domain}")
    print()
    return 0


def _domain_status(rest: list[str]) -> int:
    args = _one_desc_parser("tdx-domain status", "--domain", rest)
    doc = descriptor.load(args.domain)
    _require_role(doc, "env", "tdx-domain", "tdx-system")
    meta = descriptor.resolve(doc)
    _setenv(meta)
    slug = doc["system"]
    h = systems.get(slug).domain_status(DomainContext(slug, doc["name"], meta))
    return _report_status(h.ok, h.detail)


def _domain_info(rest: list[str]) -> int:
    args = _info_parser("tdx-domain info", "--domain", rest)
    doc = descriptor.load(args.domain)
    _require_role(doc, "env", "tdx-domain", "tdx-system")
    print(descriptor.info(doc, args.format))
    return 0


# =====================================================================
# small parsers shared by destroy/status/info
# =====================================================================

_ENCRYPTED_DESC = "the (encrypted) descriptor file, keyed by $TD_DEV_PERSONAL_SECRET"


def _one_desc_parser(prog: str, flag: str, rest: list[str]) -> argparse.Namespace:
    p = _ArgumentParser(prog=prog, add_help=True,
                        description=f"Reads {_ENCRYPTED_DESC}.")
    p.add_argument(flag, dest="sys" if flag == "--sys" else "domain",
                   required=True, metavar="FILE", help=_ENCRYPTED_DESC)
    return p.parse_args(rest)


def _info_parser(prog: str, flag: str, rest: list[str]) -> argparse.Namespace:
    p = _ArgumentParser(prog=prog, add_help=True,
                        description=f"Reads {_ENCRYPTED_DESC}.")
    p.add_argument(flag, dest="sys" if flag == "--sys" else "domain",
                   required=True, metavar="FILE", help=_ENCRYPTED_DESC)
    p.add_argument("--format", required=True,
                   choices=list(descriptor.VALID_INFO_FORMATS),
                   help="json | export | unset")
    return p.parse_args(rest)


# =====================================================================
# entry points
# =====================================================================

_SYS_VERBS = {"create": _sys_create, "destroy": _sys_destroy,
              "status": _sys_status, "info": _sys_info,
              "pause": _sys_pause, "resume": _sys_resume,
              "secrets": _sys_secrets, "connect": _sys_connect}
_DOMAIN_VERBS = {"create": _domain_create, "destroy": _domain_destroy,
                 "status": _domain_status, "info": _domain_info}

# Verbs that touch no (encrypted) descriptor, so they don't need $TD_DEV_PERSONAL_SECRET
# and don't need the managed runtime venv (they only shell out to a provider
# CLI, e.g. `secrets` -> `op`).
_NO_DESCRIPTOR_VERBS = {"secrets"}


def _dispatch(cmd: str, verbs: dict, argv: list[str]) -> int:
    if not argv or argv[0] in ("-h", "--help"):
        print(f"Usage: {cmd} {{{'|'.join(verbs)}}} [args...]  "
              f"(add -h to a verb for its options)")
        return 0 if argv else 2
    verb, rest = argv[0], argv[1:]
    if verb not in verbs:
        raise DatagenError(f"unknown {cmd} subcommand: {verb!r} "
                           f"(expected {', '.join(verbs)})")
    # Descriptor verbs read or write an (encrypted) descriptor — gate on the
    # secret up front so the error is clear before any argument parsing. `-h`
    # / `--help` is exempt so the options are always discoverable.
    if verb not in _NO_DESCRIPTOR_VERBS and not any(
            a in ("-h", "--help") for a in rest):
        crypto.require_secret()
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
