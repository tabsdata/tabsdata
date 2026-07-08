"""JSON descriptors for tdx systems and domains.

`tdx-system create` and `tdx-domain create` write a *descriptor* describing one
external system or domain; `tdx-system` / `tdx-domain` read it and act on the
external system it names. The descriptor is a JSON document, but it is stored
**encrypted at rest** — an AES-256-GCM ciphertext in PEM armor, keyed by
``$TD_BACKENDS_CRYPTO_KEY`` (see :mod:`tabsdatax.itest.tdksystems.crypto`). Every
write encrypts and every read decrypts, so nothing runs without the secret.

The decrypted JSON is::

    {
      "generator": "td_gen",
      "kind":     "<category>-<role>",    # e.g. "container-system", "cloud-domain"
      "category": "container|cloud|saas", # which helper module family handles it
      "system":   "<slug>",               # external system (mysql, s3, snowflake…)
      "role":     "sys|env",              # system descriptor vs domain descriptor
      "name":     "<logical-name>",       # the descriptor's file name (an identifier)
      "started":  "<ISO-8601 UTC>",       # when the system/domain was created
      "model":    "<model>",              # domains only, if a model was installed
      "data":     "<size>",               # domains only, if data was loaded
      "meta": [                           # baked settings, in order
         {"name": "HOST", "value": "localhost"},  # a literal, OR
         {"name": "SYS_PATH", "env": "MY_ROOT"}   # read from $MY_ROOT at run time
      ],
      "exported": ["HOST", ...],          # connection keys surfaced by `info`
      "strip_slash": [["SYS_PATH", true]] # relative-prefix leading-slash strips
    }

`category` + `system` are enough for `tdx-system`/`tdx-domain` to pick the right
per-system helper module. `name`/`started`/`model`/`data` are attached by
:func:`stamp` right after the backend writes the connection settings. Env-backed
values (`{"env": VAR}`) are resolved at *use* time — a missing/empty one is a
hard error. Storage prefixes listed in `strip_slash` get a leading slash
stripped at resolve time (with a warning when the flag is true).
"""

from __future__ import annotations

import json
import os
import re
import shlex
import sys
from datetime import datetime, timezone
from pathlib import Path

from . import crypto
from .errors import DatagenError

# Marker at the top of every descriptor — lets `load` tell a tdx descriptor
# from an unrelated JSON file.
GENERATOR = "td_gen"

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_name(name: str) -> None:
    """A system/domain name must be a plain identifier — it is the descriptor's
    file name and its logical name (so it stays a portable resource identifier
    and can't reach outside its store directory)."""
    if not name or not _IDENT_RE.match(name):
        raise DatagenError(
            f"name must be an identifier — letters, digits and underscores, "
            f"not starting with a digit (got: {name!r})"
        )


def now_stamp() -> str:
    """Current time as an ISO-8601 UTC string, for the descriptor's `started`."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _meta_entries(
    meta: list[tuple[str, str | None, str | None]],
) -> list[dict[str, str]]:
    """Turn (name, literal, env_var) triples into JSON meta entries."""
    out: list[dict[str, str]] = []
    for name, lit, env in meta:
        if env is not None:
            out.append({"name": name, "env": env})
        else:
            out.append({"name": name, "value": lit if lit is not None else ""})
    return out


def _write(
    out: str,
    *,
    category: str,
    system: str,
    role: str,
    kind: str,
    meta: list[tuple[str, str | None, str | None]],
    exported: list[str],
    strip_slash: list[tuple[str, bool]],
) -> None:
    # `name` / `started` are filled in by stamp() right after this, once the CLI
    # knows the descriptor's name; the backend only bakes the connection
    # settings here.
    doc = {
        "generator": GENERATOR,
        "kind": kind,
        "category": category,
        "system": system,
        "role": role,
        "meta": _meta_entries(meta),
        "exported": list(exported),
        "strip_slash": [[n, bool(w)] for n, w in strip_slash],
    }
    _write_doc(out, doc)


def _write_doc(path: str | Path, doc: dict) -> None:
    """Serialise `doc` to JSON and write it encrypted (PEM) to `path`,
    creating parent directories as needed (e.g. `target/`)."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        crypto.encrypt(json.dumps(doc, indent=2).encode("utf-8")),
        encoding="utf-8",
    )


def _forwarded(
    forwarded: list[tuple[str, str | None, str | None]],
) -> list[tuple[str, str | None, str | None]]:
    """Expand each forwarded (name, literal, env_var) into its value entry plus
    a FORWARDED_<name>_ENV literal recording the --env: reference (so downstream
    scripts can preserve it)."""
    out: list[tuple[str, str | None, str | None]] = []
    for name, lit, env in forwarded:
        out.append((name, lit, env))
        out.append((f"FORWARDED_{name}_ENV", env or "", None))
    return out


# ---------------------------------------------------------------------------
# Writers (one per system/domain shape) — called by the backend base classes.
# ---------------------------------------------------------------------------


def write_container_system(
    *,
    out: str,
    system_type: str,
    system_kind: str,
    label: str,
    container: str,
    host: str,
    port: str,
    user: str,
    password: str,
    domain_desc: str,
    image: str | None = None,
    sys_path: str | None = None,
    sys_path_env: str | None = None,
    extra_vars: dict[str, str] | None = None,
    location: str = "container",
) -> None:
    """Descriptor for a container system (target/systems/mysql.json, …).

    `image` — the docker image:tag the container was started from; recorded in
    `meta` and the prefixed export set so `info` shows it.
    `sys_path` / `sys_path_env` — for file-staging systems, the staging-root
    path (literal, or the name of an env var read at run time), exported as
    `SYS_PATH` and forwarded so domain creation derives BASE_PATH.
    `extra_vars` — extra metadata surfaced in `meta` and the delegate env (e.g.
    redpanda's REGISTRY_PORT); not part of the prefixed export set.
    `location` — SYSTEM_LOCATION marker: "container" for a locally-run system,
    "ssh-tunnel" for a `tdx-system connect` handle (carries SSH_* extra_vars and
    HOST/PORT pointing at the local tunnel end)."""
    extra = extra_vars or {}
    staging = sys_path is not None or sys_path_env is not None
    meta: list[tuple[str, str | None, str | None]] = [
        ("SYSTEM_TYPE", system_type, None),
        ("SYSTEM_KIND", system_kind, None),
        ("SYSTEM_LOCATION", location, None),
        ("CONTAINER", container, None),
        ("HOST", host, None),
        ("PORT", port, None),
        ("USER", user, None),
        ("PASSWORD", password, None),
        ("DOMAIN_DESC", domain_desc, None),
    ]
    if image:
        meta.insert(4, ("IMAGE", image, None))  # after CONTAINER
    if staging:
        meta.append(("SYS_PATH", sys_path, sys_path_env))
        meta.append(("FORWARDED_SYS_PATH_ENV", sys_path_env or "", None))
    for k, v in extra.items():
        meta.append((k, v, None))
    exported = [
        "SYSTEM_TYPE",
        "SYSTEM_KIND",
        "SYSTEM_LOCATION",
        "CONTAINER",
        "HOST",
        "PORT",
        "USER",
        "PASSWORD",
    ]
    if image:
        exported.insert(4, "IMAGE")  # after CONTAINER
    if staging:
        exported.append("SYS_PATH")
    strip = [("SYS_PATH", True)] if staging else []
    _write(
        out,
        category="container",
        system=system_type,
        role="sys",
        kind="container-system",
        meta=meta,
        exported=exported,
        strip_slash=strip,
    )


def write_container_domain(
    *,
    out: str,
    system_type: str,
    system_kind: str,
    host: str,
    port: str,
    user: str,
    password: str,
    db: str,
    sqlalchemy_uri: str,
    domain_desc: str,
    supports_sql: bool,
    image: str | None = None,
    sys_path: str | None = None,
    sys_path_env: str | None = None,
    base_path: str | None = None,
    extra_vars: dict[str, str] | None = None,
) -> None:
    """Descriptor for a container domain (SQL or not).

    `image` — the docker image:tag of the system the domain lives on; recorded
    in `meta` and the prefixed export set so `info` shows it.
    `base_path` (+ `sys_path` / `sys_path_env`) — for file-staging systems, the
    domain's full staging prefix, exported as `BASE_PATH` alongside the root
    `SYS_PATH`. `extra_vars` — extra metadata surfaced in `meta` (e.g.
    redpanda's KAFKA_BROKERS / SCHEMA_REGISTRY_URL)."""
    extra = extra_vars or {}
    staging = base_path is not None
    meta: list[tuple[str, str | None, str | None]] = [
        ("SYSTEM_TYPE", system_type, None),
        ("SYSTEM_KIND", system_kind, None),
        ("HOST", host, None),
        ("PORT", port, None),
        ("USER", user, None),
        ("PASSWORD", password, None),
        ("DB", db, None),
        ("SQLALCHEMY_URI", sqlalchemy_uri, None),
        ("DOMAIN_DESC", domain_desc, None),
        ("SUPPORTS_SQL", "1" if supports_sql else "0", None),
    ]
    if image:
        meta.insert(2, ("IMAGE", image, None))  # after SYSTEM_KIND
    if staging:
        meta.append(("SYS_PATH", sys_path, sys_path_env))
        meta.append(("FORWARDED_SYS_PATH_ENV", sys_path_env or "", None))
        meta.append(("BASE_PATH", base_path, None))
    for k, v in extra.items():
        meta.append((k, v, None))
    exported = [
        "SYSTEM_TYPE",
        "SYSTEM_KIND",
        "HOST",
        "PORT",
        "USER",
        "PASSWORD",
        "DB",
        "SQLALCHEMY_URI",
    ]
    if image:
        exported.insert(2, "IMAGE")  # after SYSTEM_KIND
    if staging:
        exported += ["SYS_PATH", "BASE_PATH"]
    strip = [("SYS_PATH", True), ("BASE_PATH", False)] if staging else []
    _write(
        out,
        category="container",
        system=system_type,
        role="env",
        kind="container-domain",
        meta=meta,
        exported=exported,
        strip_slash=strip,
    )


def write_cloud_system(
    *,
    out: str,
    storage_type: str,
    system_kind: str,
    label: str,
    sys_path: str | None,
    sys_path_env: str | None,
    forwarded: list[tuple[str, str | None, str | None]],
    strip_sys_path_slash: bool = True,
) -> None:
    """Descriptor for a cloud-storage system (target/systems/s3.json, …).

    `strip_sys_path_slash` — when True (object stores) a leading slash on
    SYS_PATH is stripped at run time; fs passes False (absolute roots valid)."""
    meta: list[tuple[str, str | None, str | None]] = [
        ("STORAGE_TYPE", storage_type, None),
        ("SYSTEM_KIND", system_kind, None),
        ("SYS_PATH", sys_path, sys_path_env),
        ("FORWARDED_SYS_PATH_ENV", sys_path_env or "", None),
    ]
    meta += _forwarded(forwarded)
    forwarded_names = [t[0] for t in forwarded]
    exported = ["STORAGE_TYPE", "SYSTEM_KIND", "SYS_PATH"] + forwarded_names
    strip = [("SYS_PATH", True)] if strip_sys_path_slash else []
    _write(
        out,
        category="cloud",
        system=storage_type,
        role="sys",
        kind="cloud-system",
        meta=meta,
        exported=exported,
        strip_slash=strip,
    )


def write_cloud_domain(
    *,
    out: str,
    storage_type: str,
    system_kind: str,
    sys_path: str | None,
    sys_path_env: str | None,
    base_path: str,
    forwarded: list[tuple[str, str | None, str | None]],
    strip_sys_path_slash: bool = True,
) -> None:
    """Descriptor for a cloud domain. `strip_sys_path_slash` — see
    write_cloud_system; fs passes False."""
    meta: list[tuple[str, str | None, str | None]] = [
        ("STORAGE_TYPE", storage_type, None),
        ("SYSTEM_KIND", system_kind, None),
        ("SYS_PATH", sys_path, sys_path_env),
        ("FORWARDED_SYS_PATH_ENV", sys_path_env or "", None),
        ("BASE_PATH", base_path, None),
    ]
    meta += _forwarded(forwarded)
    forwarded_names = [t[0] for t in forwarded]
    exported = [
        "STORAGE_TYPE",
        "SYSTEM_KIND",
        "SYS_PATH",
        "BASE_PATH",
    ] + forwarded_names
    # BASE_PATH = SYS_PATH/<segment>, so it only leads with '/' when SYS_PATH
    # does — warn once (on SYS_PATH) and strip BASE_PATH silently.
    strip = [("SYS_PATH", True), ("BASE_PATH", False)] if strip_sys_path_slash else []
    _write(
        out,
        category="cloud",
        system=storage_type,
        role="env",
        kind="cloud-domain",
        meta=meta,
        exported=exported,
        strip_slash=strip,
    )


def write_saas_system(
    *,
    out: str,
    saas_type: str,
    system_kind: str,
    label: str,
    forwarded: list[tuple[str, str | None, str | None]],
) -> None:
    """Descriptor for a saas system (databricks/snowflake/salesforce). Same
    shape as the cloud system, minus SYS_PATH (saas systems aren't path-based —
    domains are schemas or no-op containers)."""
    meta: list[tuple[str, str | None, str | None]] = [
        ("SAAS_TYPE", saas_type, None),
        ("SYSTEM_KIND", system_kind, None),
    ]
    meta += _forwarded(forwarded)
    forwarded_names = [t[0] for t in forwarded]
    exported = ["SAAS_TYPE", "SYSTEM_KIND"] + forwarded_names
    _write(
        out,
        category="saas",
        system=saas_type,
        role="sys",
        kind="saas-system",
        meta=meta,
        exported=exported,
        strip_slash=[],
    )


def write_saas_domain(
    *,
    out: str,
    saas_type: str,
    system_kind: str,
    schema: str | None,
    forwarded: list[tuple[str, str | None, str | None]],
) -> None:
    """Descriptor for a saas domain. `schema` is None for systems whose domain
    is purely a credentials clone (salesforce); otherwise it's baked as SCHEMA."""
    meta: list[tuple[str, str | None, str | None]] = [
        ("SAAS_TYPE", saas_type, None),
        ("SYSTEM_KIND", system_kind, None),
    ]
    if schema is not None:
        meta.append(("SCHEMA", schema, None))
    meta += _forwarded(forwarded)
    forwarded_names = [t[0] for t in forwarded]
    exported = ["SAAS_TYPE", "SYSTEM_KIND"]
    if schema is not None:
        exported.append("SCHEMA")
    exported += forwarded_names
    _write(
        out,
        category="saas",
        system=saas_type,
        role="env",
        kind="saas-domain",
        meta=meta,
        exported=exported,
        strip_slash=[],
    )


# ---------------------------------------------------------------------------
# write: persist a System backend's returned settings (the one systems ->
# descriptor serialization step; the write_* above are its primitives).
# ---------------------------------------------------------------------------


def _settings_literals(settings: dict) -> dict[str, str]:
    return {name: (s.value or "") for name, s in settings.items()}


def _settings_triples(settings: dict, *, exclude: tuple[str, ...] = ()):
    """(name, literal|None, env_var|None) for the writers' `forwarded` param —
    env wins, so a forwarded reference is stored as a ref (secret never baked)."""
    return [
        (s.name, None if s.env_var else s.value, s.env_var)
        for s in settings.values()
        if s.name not in exclude
    ]


def write(sys_obj, role: str, out: str, settings: dict) -> None:
    """Persist a `System` backend's returned ``{name: Setting}`` as a
    descriptor, dispatching to the matching category writer. `role` is
    ``"sys"`` (system) or ``"env"`` (domain). Settings are duck-typed
    (``.name`` / ``.value`` / ``.env_var``); a Setting with ``env_var`` is
    stored as a forwarded reference, never baked."""
    cat = sys_obj.category
    if role == "sys":
        if cat == "container":
            v = _settings_literals(settings)
            sp = settings.get("SYS_PATH")
            write_container_system(
                out=out,
                system_type=sys_obj.slug,
                system_kind=sys_obj.kind,
                label=sys_obj.label,
                container=v["CONTAINER"],
                host=v["HOST"],
                port=v["PORT"],
                user=v["USER"],
                password=v["PASSWORD"],
                domain_desc=getattr(sys_obj, "domain_desc", ""),
                image=v.get("IMAGE") or None,
                sys_path=(sp.value if sp and sp.env_var is None else None),
                sys_path_env=(sp.env_var if sp else None),
            )
        elif cat == "cloud":
            sp = settings["SYS_PATH"]
            write_cloud_system(
                out=out,
                storage_type=sys_obj.slug,
                system_kind=sys_obj.kind,
                label=sys_obj.label,
                sys_path=(sp.value if sp.env_var is None else None),
                sys_path_env=sp.env_var,
                forwarded=_settings_triples(settings, exclude=("SYS_PATH",)),
                strip_sys_path_slash=getattr(sys_obj, "relative_sys_path", True),
            )
        else:
            write_saas_system(
                out=out,
                saas_type=sys_obj.slug,
                system_kind=sys_obj.kind,
                label=sys_obj.label,
                forwarded=_settings_triples(settings),
            )
        return
    # role == "env" (domain)
    if cat == "container":
        v = _settings_literals(settings)
        known = {"HOST", "PORT", "USER", "PASSWORD", "DB", "SQLALCHEMY_URI", "IMAGE"}
        extra = {k: val for k, val in v.items() if k not in known}  # SYS_* back-refs
        write_container_domain(
            out=out,
            system_type=sys_obj.slug,
            system_kind=sys_obj.kind,
            host=v["HOST"],
            port=v["PORT"],
            user=v["USER"],
            password=v["PASSWORD"],
            db=v["DB"],
            sqlalchemy_uri=v["SQLALCHEMY_URI"],
            domain_desc=getattr(sys_obj, "domain_desc", ""),
            supports_sql=sys_obj.supports_sql,
            image=v.get("IMAGE") or None,
            extra_vars=extra,
        )
    elif cat == "cloud":
        sp, bp = settings["SYS_PATH"], settings["BASE_PATH"]
        write_cloud_domain(
            out=out,
            storage_type=sys_obj.slug,
            system_kind=sys_obj.kind,
            sys_path=(sp.value if sp.env_var is None else None),
            sys_path_env=sp.env_var,
            base_path=bp.value or "",
            forwarded=_settings_triples(settings, exclude=("SYS_PATH", "BASE_PATH")),
            strip_sys_path_slash=getattr(sys_obj, "relative_sys_path", True),
        )
    else:
        schema = settings.get("SCHEMA")
        write_saas_domain(
            out=out,
            saas_type=sys_obj.slug,
            system_kind=sys_obj.kind,
            schema=(schema.value if schema else None),
            forwarded=_settings_triples(settings, exclude=("SCHEMA",)),
        )


# ---------------------------------------------------------------------------
# Reading / resolving
# ---------------------------------------------------------------------------


def _parse(path: str | Path):
    """Decrypt + parse a descriptor, or None if it can't be read as one
    (unreadable / not our PEM / wrong secret / bad JSON). Lenient — used by
    :func:`is_descriptor`."""
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError:
        return None
    if not crypto.is_pem(text):
        return None
    try:
        return json.loads(crypto.decrypt(text))
    except (DatagenError, ValueError):
        return None


def is_descriptor(path: str | Path) -> bool:
    """True if `path` is an encrypted tdx descriptor (decryptable with the
    current $TD_BACKENDS_CRYPTO_KEY and carrying the generator marker)."""
    doc = _parse(path)
    return isinstance(doc, dict) and doc.get("generator") == GENERATOR


def load(path: str | Path) -> dict:
    """Read, decrypt and validate a descriptor. Raises DatagenError with a
    specific message for: unreadable file, not an encrypted descriptor, wrong
    $TD_BACKENDS_CRYPTO_KEY, or not a tdx descriptor."""
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise DatagenError(f"cannot read descriptor {path}: {exc}")
    if not crypto.is_pem(text):
        raise DatagenError(
            f"not a tdx descriptor: {path} "
            f"(was it written by `tdx-system create` / `tdx-domain create`?)"
        )
    plaintext = crypto.decrypt(text)  # raises on wrong secret / corrupt blob
    try:
        doc = json.loads(plaintext)
    except ValueError:
        raise DatagenError(f"descriptor {path} did not decrypt to valid JSON")
    if not isinstance(doc, dict) or doc.get("generator") != GENERATOR:
        raise DatagenError(f"not a tdx descriptor: {path}")
    return doc


def loads(text: str) -> dict:
    """Decrypt + validate an in-memory descriptor string — the disk-free twin of
    :func:`load`, for content fetched over SSH by `tdx-system connect`. Uses the
    active personal secret (see :func:`crypto.using_secret_env`)."""
    if not crypto.is_pem(text):
        raise DatagenError(
            "not a tdx descriptor (no PEM armor) — was it written by "
            "`tdx-system create`?"
        )
    plaintext = crypto.decrypt(text)  # raises on wrong secret / corrupt blob
    try:
        doc = json.loads(plaintext)
    except ValueError:
        raise DatagenError("descriptor did not decrypt to valid JSON")
    if not isinstance(doc, dict) or doc.get("generator") != GENERATOR:
        raise DatagenError("not a tdx descriptor")
    return doc


def resolve(doc: dict, *, warn: bool = False) -> dict[str, str]:
    """Resolve a descriptor's `meta` to {name: value}. Env-backed values are
    read now (a missing/empty one is a hard error). Relative storage prefixes
    have a leading slash stripped; when `warn`, a flagged strip prints a notice
    to stderr."""
    meta: dict[str, str] = {}
    for entry in doc.get("meta", []):
        name = entry["name"]
        if "env" in entry:
            var = entry["env"]
            val = os.environ.get(var)
            if not val:
                raise DatagenError(f"{var} env var must be set (needed by {name})")
            meta[name] = val
        else:
            meta[name] = entry.get("value") or ""
    for name, flag in doc.get("strip_slash", []):
        v = meta.get(name, "")
        if v.startswith("/"):
            if warn and flag:
                print(
                    f"WARNING: {name} value {v!r} must not start with '/' "
                    f"(storage prefixes are relative) - removing the leading "
                    f"slash.",
                    file=sys.stderr,
                )
            meta[name] = v.lstrip("/")
    return meta


def read_meta(path: str | Path) -> dict[str, str]:
    """{"KIND": <kind>, <resolved meta vars>} — used by the data pipeline to
    learn a domain's connection details. Silent (no strip warnings)."""
    doc = load(path)
    return {"KIND": doc["kind"], **resolve(doc)}


# ---------------------------------------------------------------------------
# Identity: attach name/started/model/data after the backend writes settings
# ---------------------------------------------------------------------------


def stamp(
    path: str | Path,
    *,
    name: str,
    started: str,
    model: str | None = None,
    data: str | None = None,
    extra_meta: dict[str, str] | None = None,
) -> dict:
    """Attach the tdx identity to a freshly-written descriptor and rewrite it.

    Sets `name` / `started`; records `model` / `data` for domains; and appends
    `extra_meta` literals (e.g. a container domain's admin back-reference, so it
    can destroy itself)."""
    doc = load(path)
    doc["name"] = name
    doc["started"] = started
    if model is not None:
        doc["model"] = model
    if data is not None:
        doc["data"] = data
    if extra_meta:
        have = {e["name"] for e in doc["meta"]}
        for k, v in extra_meta.items():
            if k not in have:
                doc["meta"].append({"name": k, "value": v if v is not None else ""})
    _write_doc(path, doc)
    return doc


# ---------------------------------------------------------------------------
# info: name / type / started (+ model / data) + connection keys
# ---------------------------------------------------------------------------


def _info_pairs(doc: dict) -> list[tuple[str, str]]:
    """(key, value) pairs for `info`: the identity fields, then the connection
    settings (`exported`). Keys are unprefixed — the descriptor's file name
    already identifies the system/domain."""
    meta = resolve(doc)
    pairs = [
        ("NAME", doc.get("name", "")),
        ("TYPE", doc.get("system", "")),
        ("STARTED", doc.get("started", "")),
    ]
    if "model" in doc:
        pairs.append(("MODEL", doc.get("model") or ""))
    if "data" in doc:
        pairs.append(("DATA", doc.get("data") or ""))
    pairs += [(n, meta[n]) for n in doc.get("exported", [])]
    return pairs


def attributes(doc: dict) -> dict[str, str]:
    """The flat ``{KEY: value}`` view of a descriptor — the same unprefixed keys
    as ``info(doc, "json")`` (identity + connection settings), as data rather
    than rendered text. Consumers (e.g. the test fixtures) map their fields onto
    these keys."""
    return dict(_info_pairs(doc))


VALID_INFO_FORMATS = ("json", "export", "unset")


def info(doc: dict, fmt: str) -> str:
    """Render a descriptor's info in `fmt`:

    - ``json``   — a JSON object of `<KEY>: value` (identity + connection)
    - ``export`` — `export <KEY>=<value>` lines to source (unix)
    - ``unset``  — `unset <KEY>` lines to source (unix)
    """
    if fmt not in VALID_INFO_FORMATS:
        raise DatagenError(
            f"--format must be one of {'|'.join(VALID_INFO_FORMATS)} (got: {fmt!r})"
        )
    pairs = _info_pairs(doc)
    if fmt == "json":
        return json.dumps(dict(pairs), indent=2)
    if fmt == "export":
        return "\n".join(f"export {k}={shlex.quote(v)}" for k, v in pairs)
    return "\n".join(f"unset {k}" for k, _ in pairs)
