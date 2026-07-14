#
# Copyright 2026 Tabsdata Inc.
#

"""JSON descriptors for tdx systems and domains.

`tdx-system create` and `tdx-domain create` write a *descriptor* describing one
external system or domain; `tdx-system` / `tdx-domain` read it and act on the
external system it names. The descriptor is a JSON document, but it is stored
**encrypted at rest** — an AES-256-GCM ciphertext in PEM armor, keyed by
``$TD_BACKENDS_CRYPTO_KEY`` (see :mod:`tabsdatax.itest.tdksystems.crypto`). Every
write encrypts and every read decrypts, so nothing runs without the secret.

The decrypted JSON is::

    {
      "generator": "tdx",
      "kind":     "<category>-<role>",    # e.g. "container-system", "cloud-domain"
      "category": "container|cloud|native|saas|service",  # helper module family
      "system":   "<slug>",               # external system (mysql, s3, snowflake…)
      "role":     "sys|env",              # system descriptor vs domain descriptor
      "name":     "<logical-name>",       # the descriptor's file name (an identifier)
      "started":  "<ISO-8601 UTC>",       # when the system/domain was created
      "external_name": "<prefixed-name>", # domains only, the entity container's
                                          # name in the external system (schema /
                                          # dataset / folder / database)
      "sys_name": "<system-name>",        # domains only, the store name of the
                                          # system the domain was created in
      "model":    "<model>",              # domains only, if a model was installed
      "data":     "<size>",               # domains only, if data was loaded
      "entities": {"<table>": <rows>},    # domains only, per-entity record counts
                                          # (once data has been loaded)
      "meta": [                           # baked settings, in order
         {"name": "HOST", "value": "localhost"},        # a literal, OR
         {"name": "SYS_PATH", "value": "/data",         # a value captured from an
          "env": "MY_ROOT"}                             #   env var at create; `env`
                                                        #   records the source
      ],
      "exported": ["HOST", ...],          # connection keys surfaced by `info`
      "strip_slash": [["SYS_PATH", true]] # relative-prefix leading-slash strips
    }

`category` + `system` are enough for `tdx-system`/`tdx-domain` to pick the right
per-system helper module. `name`/`started`/`model`/`data` are attached by
:func:`stamp` right after the backend writes the connection settings. Env-backed
settings are captured from their env var at *create* time and baked as the
`value` — the descriptor is encrypted at rest, so it holds the resolved secret
and nothing is read from the environment at use time. The `env` name is kept
only so a manifest dump can re-emit the `{env: VAR}` reference (keeping that
plaintext YAML secret-free). Storage prefixes listed in `strip_slash` get a
leading slash stripped at resolve time (with a warning when the flag is true).
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
GENERATOR = "tdx"

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
    """Turn (name, value, env_var) triples into JSON meta entries. `value` is
    always baked (env-backed settings are resolved before they reach here); a
    non-empty `env_var` is recorded alongside as the value's source, so a
    manifest dump can re-emit the `{env: VAR}` reference."""
    out: list[dict[str, str]] = []
    for name, lit, env in meta:
        entry = {"name": name, "value": lit if lit is not None else ""}
        if env:
            entry["env"] = env
        out.append(entry)
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


def _uri_without_credentials(uri: str) -> str:
    """A connection URI with any ``user:password@`` userinfo stripped, so `info`
    can surface it without leaking credentials. Parsed by hand rather than with
    ``urlsplit`` because SQLAlchemy driver schemes carry characters it rejects as
    a scheme (e.g. the ``_`` in ``db2+ibm_db``), which would drop the authority.
    A URI with no ``scheme://`` prefix, or no ``@`` in its authority, is returned
    unchanged."""
    m = re.match(r"^([a-zA-Z][\w+.\-]*://)(.*)$", uri)
    if not m:
        return uri
    prefix, rest = m.group(1), m.group(2)
    end = min((i for i, ch in enumerate(rest) if ch in "/?#"), default=len(rest))
    authority, tail = rest[:end], rest[end:]
    if "@" in authority:
        authority = authority[authority.rfind("@") + 1 :]
    return prefix + authority + tail


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
    exported_extra: tuple[str, ...] = (),
) -> None:
    """Descriptor for a container domain (SQL or not).

    `image` — the docker image:tag of the system the domain lives on; recorded
    in `meta` and the prefixed export set so `info` shows it.
    `base_path` (+ `sys_path` / `sys_path_env`) — for file-staging systems, the
    domain's full staging prefix, exported as `BASE_PATH` alongside the root
    `SYS_PATH`. `extra_vars` — extra metadata surfaced in `meta` (e.g.
    redpanda's KAFKA_BROKERS / SCHEMA_REGISTRY_URL). `exported_extra` — the
    subset of `extra_vars` keys to also export (surface in `info` / fixtures)
    rather than keep meta-only (e.g. redpanda's GROUP_ID)."""
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
        ("URI", _uri_without_credentials(sqlalchemy_uri), None),
        ("DOMAIN_DESC", domain_desc, None),
        ("SUPPORTS_SQL", "1" if supports_sql else "0", None),
    ]
    if image:
        meta.insert(2, ("IMAGE", image, None))  # after SYSTEM_KIND
    if staging:
        meta.append(("SYS_PATH", sys_path, sys_path_env))
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
        "URI",
    ]
    if image:
        exported.insert(2, "IMAGE")  # after SYSTEM_KIND
    if staging:
        exported += ["SYS_PATH", "BASE_PATH"]
    exported += [k for k in exported_extra if k in extra]
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
    category: str = "cloud",
) -> None:
    """Descriptor for a cloud-storage system (target/systems/s3.json, …).

    `strip_sys_path_slash` — when True (object stores) a leading slash on
    SYS_PATH is stripped at run time; fs passes False (absolute roots valid).
    `category` is the user-facing family (object stores are "cloud"; the local
    filesystem passes "native"); the descriptor `kind` stays "cloud-*" either
    way, so the data pipeline routes both the same."""
    meta: list[tuple[str, str | None, str | None]] = [
        ("STORAGE_TYPE", storage_type, None),
        ("SYSTEM_KIND", system_kind, None),
        ("SYS_PATH", sys_path, sys_path_env),
    ]
    meta += forwarded
    forwarded_names = [t[0] for t in forwarded]
    exported = ["STORAGE_TYPE", "SYSTEM_KIND", "SYS_PATH"] + forwarded_names
    strip = [("SYS_PATH", True)] if strip_sys_path_slash else []
    _write(
        out,
        category=category,
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
    category: str = "cloud",
) -> None:
    """Descriptor for a cloud domain. `strip_sys_path_slash` — see
    write_cloud_system; fs passes False. `category` — see write_cloud_system
    (fs passes "native"); the descriptor `kind` stays "cloud-domain"."""
    meta: list[tuple[str, str | None, str | None]] = [
        ("STORAGE_TYPE", storage_type, None),
        ("SYSTEM_KIND", system_kind, None),
        ("SYS_PATH", sys_path, sys_path_env),
        ("BASE_PATH", base_path, None),
    ]
    meta += forwarded
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
        category=category,
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
    meta += forwarded
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
    meta += forwarded
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


def write_service_system(
    *,
    out: str,
    service_type: str,
    system_kind: str,
    label: str,
    forwarded: list[tuple[str, str | None, str | None]],
) -> None:
    """Descriptor for a credential-only service system (anthropic/openai): an
    externally-owned, keyed API account with nothing to provision — just the
    forwarded connection settings (the API key, resolved from its env)."""
    meta: list[tuple[str, str | None, str | None]] = [
        ("SERVICE_TYPE", service_type, None),
        ("SYSTEM_KIND", system_kind, None),
    ]
    meta += forwarded
    exported = ["SERVICE_TYPE", "SYSTEM_KIND"] + [t[0] for t in forwarded]
    _write(
        out,
        category="service",
        system=service_type,
        role="sys",
        kind="service-system",
        meta=meta,
        exported=exported,
        strip_slash=[],
    )


def write_service_domain(
    *,
    out: str,
    service_type: str,
    system_kind: str,
    forwarded: list[tuple[str, str | None, str | None]],
) -> None:
    """Descriptor for a service domain — a credentials clone (no entity
    container), so just the forwarded connection settings."""
    meta: list[tuple[str, str | None, str | None]] = [
        ("SERVICE_TYPE", service_type, None),
        ("SYSTEM_KIND", system_kind, None),
    ]
    meta += forwarded
    exported = ["SERVICE_TYPE", "SYSTEM_KIND"] + [t[0] for t in forwarded]
    _write(
        out,
        category="service",
        system=service_type,
        role="env",
        kind="service-domain",
        meta=meta,
        exported=exported,
        strip_slash=[],
    )


# ---------------------------------------------------------------------------
# write: persist a System backend's returned settings (the one systems ->
# descriptor serialization step; the write_* above are its primitives).
# ---------------------------------------------------------------------------


def _settings_literals(settings: dict) -> dict[str, str]:
    return {name: _resolve_env(s) for name, s in settings.items()}


def _resolve_env(setting) -> str:
    """The concrete value of a Setting to bake into the descriptor: its literal
    ``value`` if present, else the current value of its ``env_var`` — read now,
    at create time, with a missing/empty one a hard error. The descriptor is
    encrypted at rest, so the resolved value is stored and the env var is never
    read again at use time (the name is kept separately, as a source marker)."""
    if setting.value:
        return setting.value
    if setting.env_var:
        val = os.environ.get(setting.env_var, "")
        if not val:
            raise DatagenError(
                f"env var '{setting.env_var}' is unset / empty "
                f"(needed by {setting.name})"
            )
        return val
    return ""


def _settings_triples(settings: dict, *, exclude: tuple[str, ...] = ()):
    """(name, value, env_var) for the writers' `forwarded` param — the env-backed
    value is resolved and baked now, with its env var name riding along so a
    manifest dump can re-emit the ``{env: VAR}`` reference."""
    return [
        (s.name, _resolve_env(s), s.env_var)
        for s in settings.values()
        if s.name not in exclude
    ]


def write(sys_obj, role: str, out: str, settings: dict) -> None:
    """Persist a `System` backend's returned ``{name: Setting}`` as a
    descriptor, dispatching to the matching category writer. `role` is
    ``"sys"`` (system) or ``"env"`` (domain). Settings are duck-typed
    (``.name`` / ``.value`` / ``.env_var``); a Setting with ``env_var`` has its
    value resolved and baked now, keeping the env var name as a source marker."""
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
                sys_path=(_resolve_env(sp) if sp else None),
                sys_path_env=(sp.env_var if sp else None),
            )
        elif cat in ("cloud", "native"):  # object stores + local fs
            sp = settings["SYS_PATH"]
            write_cloud_system(
                out=out,
                storage_type=sys_obj.slug,
                system_kind=sys_obj.kind,
                label=sys_obj.label,
                sys_path=_resolve_env(sp),
                sys_path_env=sp.env_var,
                forwarded=_settings_triples(settings, exclude=("SYS_PATH",)),
                strip_sys_path_slash=getattr(sys_obj, "relative_sys_path", True),
                category=cat,
            )
        elif cat == "service":
            write_service_system(
                out=out,
                service_type=sys_obj.slug,
                system_kind=sys_obj.kind,
                label=sys_obj.label,
                forwarded=_settings_triples(settings),
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
        known = {
            "HOST",
            "PORT",
            "USER",
            "PASSWORD",
            "DB",
            "SQLALCHEMY_URI",
            "URI",
            "IMAGE",
        }
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
            exported_extra=getattr(sys_obj, "exported_domain_vars", ()),
        )
    elif cat in ("cloud", "native"):  # object stores + local fs
        sp, bp = settings["SYS_PATH"], settings["BASE_PATH"]
        write_cloud_domain(
            out=out,
            storage_type=sys_obj.slug,
            system_kind=sys_obj.kind,
            sys_path=_resolve_env(sp),
            sys_path_env=sp.env_var,
            base_path=bp.value or "",
            forwarded=_settings_triples(settings, exclude=("SYS_PATH", "BASE_PATH")),
            strip_sys_path_slash=getattr(sys_obj, "relative_sys_path", True),
            category=cat,
        )
    elif cat == "service":
        write_service_domain(
            out=out,
            service_type=sys_obj.slug,
            system_kind=sys_obj.kind,
            forwarded=_settings_triples(settings),
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
    except (OSError, UnicodeDecodeError) as exc:
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
    """Resolve a descriptor's `meta` to {name: value}. Values are baked into the
    descriptor at create time (env-backed settings were captured then), so this
    reads no environment variables — a system/domain works from its descriptor
    alone. Relative storage prefixes have a leading slash stripped; when `warn`,
    a flagged strip prints a notice to stderr."""
    meta: dict[str, str] = {}
    for entry in doc.get("meta", []):
        meta[entry["name"]] = entry.get("value") or ""
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
    external_name: str | None = None,
    sys_name: str | None = None,
    entity_term: str | None = None,
    extra_meta: dict[str, str] | None = None,
) -> dict:
    """Attach the tdx identity to a freshly-written descriptor and rewrite it.

    Sets `name` / `started`; records `model` / `data` for domains and, when a
    domain's entity container was created under a run prefix, its
    `external_name` (the schema/dataset/folder/database name in the external
    system); records `sys_name` (the store name of the system a domain was
    created in, so a manifest can name it); and appends `extra_meta` literals
    (e.g. a container domain's admin back-reference, so it can destroy itself).

    `entity_term` — the system's word for its entity container (postgres:
    `schema`, fs: `folder`, bigquery: `dataset`, …). When given, a redundant
    exported key of that (upper-cased) name is added carrying the entity
    container's name, unless the system already bakes it (bigquery's DATASET,
    saas SCHEMA)."""
    doc = load(path)
    doc["name"] = name
    doc["started"] = started
    if model is not None:
        doc["model"] = model
    if data is not None:
        doc["data"] = data
    if external_name is not None:
        doc["external_name"] = external_name
    if sys_name is not None:
        doc["sys_name"] = sys_name
    if extra_meta:
        have = {e["name"] for e in doc["meta"]}
        for k, v in extra_meta.items():
            if k not in have:
                doc["meta"].append({"name": k, "value": v if v is not None else ""})
    if entity_term:
        key = entity_term.upper()
        names = {e["name"] for e in doc["meta"]}
        if key not in names and key not in doc.get("exported", []):
            term_value = doc.get("external_name") or doc.get("name", "")
            doc["meta"].append({"name": key, "value": term_value})
            doc.setdefault("exported", []).append(key)
    _write_doc(path, doc)
    return doc


def record_entities(path: str | Path, entities: dict[str, int]) -> dict:
    """Record a domain's per-entity record counts (``{table: rows}``) into its
    descriptor, surfaced by `info` / `attributes` as ``ENTITIES``. Called by the
    data pipeline right after a model's data is loaded."""
    doc = load(path)
    doc["entities"] = {str(k): int(v) for k, v in entities.items()}
    _write_doc(path, doc)
    return doc


# ---------------------------------------------------------------------------
# info: name / type / started (+ model / data) + connection keys
# ---------------------------------------------------------------------------


def _info_pairs(doc: dict) -> list[tuple[str, object]]:
    """(key, value) pairs for `info`: the identity fields, then the connection
    settings (`exported`). Keys are unprefixed — the descriptor's file name
    already identifies the system/domain. Values are strings except ``ENTITIES``,
    which is the ``{table: rows}`` mapping verbatim."""
    meta = resolve(doc)
    pairs: list[tuple[str, object]] = [
        ("NAME", doc.get("name", "")),
        ("TYPE", doc.get("system", "")),
        ("STARTED", doc.get("started", "")),
    ]
    if doc.get("external_name"):
        pairs.append(("EXTERNAL_NAME", doc["external_name"]))
    if "model" in doc:
        pairs.append(("MODEL", doc.get("model") or ""))
    if "data" in doc:
        pairs.append(("DATA", doc.get("data") or ""))
    if doc.get("entities"):
        pairs.append(("ENTITIES", doc["entities"]))
    pairs += [(n, meta[n]) for n in doc.get("exported", [])]
    return pairs


def attributes(doc: dict) -> dict[str, object]:
    """The flat ``{KEY: value}`` view of a descriptor — the same unprefixed keys
    as ``info(doc, "json")`` (identity + connection settings), as data rather
    than rendered text. Consumers (e.g. the test fixtures) map their fields onto
    these keys. Values are strings except ``ENTITIES`` (the ``{table: rows}``
    mapping)."""
    return dict(_info_pairs(doc))


VALID_INFO_FORMATS = ("json", "export", "unset")


def info(doc: dict, fmt: str, prefix: str | None = None) -> str:
    """Render a descriptor's info in `fmt`:

    - ``json``   — a JSON object of `<KEY>: value` (identity + connection)
    - ``export`` — `export <KEY>=<value>` lines to source (unix)
    - ``unset``  — `unset <KEY>` lines to source (unix)

    Keys are unprefixed by default; if `prefix` is given, every key is emitted
    as ``<prefix>__<KEY>`` (json object keys, `export`/`unset` variable names).
    """
    if fmt not in VALID_INFO_FORMATS:
        raise DatagenError(
            f"--format must be one of {'|'.join(VALID_INFO_FORMATS)} (got: {fmt!r})"
        )
    pairs = _info_pairs(doc)
    if prefix:
        pairs = [(f"{prefix}__{k}", v) for k, v in pairs]
    if fmt == "json":
        return json.dumps(dict(pairs), indent=2)
    if fmt == "export":
        return "\n".join(f"export {k}={shlex.quote(_shell_value(v))}" for k, v in pairs)
    return "\n".join(f"unset {k}" for k, _ in pairs)


def _shell_value(v: object) -> str:
    """Render an info value for `export` lines: strings as-is; the ENTITIES
    mapping as compact JSON."""
    return v if isinstance(v, str) else json.dumps(v, separators=(",", ":"))
