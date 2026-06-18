"""Visitor helpers for the dict-representation of a `_JsonSerde` subclass,
plus the secret/var extract/dehydrate/hydrate pipeline built on top.

A serialized `_JsonSerde` subclass is a tree of *envelopes*:

    {"kind": ..., "apiVersion": ..., "type": ..., "spec": {...}}

`spec` may contain plain fields, lists, or further envelopes (nested
`_JsonSerde` payload). Each envelope is identified by its
`kind` discriminator. `find_by_kind` walks the tree and yields every
envelope whose `kind` matches; `replace_at` writes a new value at a
path returned by the walker.

The secret/var helpers (`extract_secrets`, `extract_vars`,
`dehydrate_secrets`, `hydrate_secrets`) target a fixed set of envelope
kinds: a serialized `tabsdatak.api.Secret` is an envelope of kind
``secretRef`` (opaque, value resolved later by a secret store) or
``secretLiteral`` (value carried inline). `Var` works the same way
with ``varRef`` / ``varLiteral``. Each such envelope lives at some
jsonpath inside the parent tree's ``spec``; the helpers use that
jsonpath as the stable identity of the secret -- the var-style key
derived from the path is what the resolver matches on, *not* the ref's
``_s`` name.
"""

from __future__ import annotations

import os
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, TypeAlias, Union

from tabsdatak.common._error import CommonErrorCode

type JsonPath = str
type Envelope = dict[str, Any]


JsonSerdeData = Union[dict, list]

# Valid var-style name (uppercase, no spaces).
VarNameSpec: TypeAlias = str


@dataclass
class KeyValue:
    key: VarNameSpec
    value: str | None = None


# ---------------------------------------------------------------------------
# Visitor helpers
# ---------------------------------------------------------------------------


def find_by_kind(
    json_serde_data: JsonSerdeData, kind: str
) -> list[tuple[JsonPath, Envelope]]:
    """Return every envelope under `node` whose ``kind`` equals `kind`.

    Each result is a ``(json_path, envelope)`` pair. The path uses the
    subset of JSONPath understood by `replace_at`:

    - ``$``          — the root
    - ``.<key>``     — dict key access (key must be a python identifier-ish
                       string; keys with ``.`` or ``[`` are not supported)
    - ``[<index>]``  — list element access

    The walk descends into matched envelopes too, so nested occurrences
    are also reported.
    """
    found: list[tuple[JsonPath, Envelope]] = []

    def walk(n: Any, path: JsonPath) -> None:
        if isinstance(n, dict):
            if n.get("kind") == kind:
                found.append((path, n))
            for k, v in n.items():
                walk(v, f"{path}.{k}")
        elif isinstance(n, list):
            for i, v in enumerate(n):
                walk(v, f"{path}[{i}]")

    walk(json_serde_data, "$")
    return found


def _parse_path(path: JsonPath) -> list[str | int]:
    if not path.startswith("$"):
        raise CommonErrorCode.COMMON_1.exception(path=path)
    tokens: list[str | int] = []
    i, n = 1, len(path)
    while i < n:
        ch = path[i]
        if ch == ".":
            j = i + 1
            while j < n and path[j] not in ".[":
                j += 1
            if j == i + 1:
                raise CommonErrorCode.COMMON_2.exception(rest=path[i:], path=path)
            tokens.append(path[i + 1 : j])
            i = j
        elif ch == "[":
            j = path.find("]", i + 1)
            if j == -1:
                raise CommonErrorCode.COMMON_3.exception(path=path)
            tokens.append(int(path[i + 1 : j]))
            i = j + 1
        else:
            raise CommonErrorCode.COMMON_4.exception(ch=ch, index=i, path=path)
    return tokens


def replace_at(
    json_serde_data: JsonSerdeData, path: JsonPath, value: JsonSerdeData
) -> None:
    """Replace the element at `path` inside `node` with `value`, in place.

    `path` must use the syntax produced by `find_by_kind`. Replacing the
    root (``$``) is not supported — the caller already holds that
    reference and can assign to it directly.
    """
    tokens = _parse_path(path)
    if not tokens:
        raise CommonErrorCode.COMMON_5.exception()
    target = json_serde_data
    for token in tokens[:-1]:
        target = target[token]
    target[tokens[-1]] = value


# ---------------------------------------------------------------------------
# Secret / var helpers
# ---------------------------------------------------------------------------

# On purpose, we don't use constants for the __kind__ attributes.
# Not to break backwards compatibility with serialized data
# out there unknowingly because of a change of constant that would not
# break things in our code because we are using the constant all over,
# but it would break things outside.
_SECRET_REF_KIND = "secretRef"
_SECRET_LITERAL_KIND = "secretLiteral"
_VAR_REF_KIND = "varRef"
_VAR_LITERAL_KIND = "varLiteral"
_CONNECTION_REF_KIND = "connectionRef"
_CONNECTION_NAME_FIELD = "name"

# Envelope ``type`` strings -- ``"<module>:<qualname>"`` of the concrete
# class behind the envelope. Dehydration/hydration flip ``kind`` between
# ref and literal; the ``type`` has to move with it, otherwise the
# envelope no longer round-trips through `tabsdatak.api.Secret.from_dict`
# (the validator checks ``type`` resolves to a class whose ``__kind__``
# matches the envelope ``kind``).
_SECRET_REF_TYPE = "tabsdatak.api:Secret"
_SECRET_LITERAL_TYPE = "tabsdatak.api:_LiteralSecret"
_VAR_REF_TYPE = "tabsdatak.api:Var"
_VAR_LITERAL_TYPE = "tabsdatak.api:_LiteralVar"

# Both `Secret` and `Var` store the ref placeholder / literal value
# under ``_s``. These match the field names on `tabsdatak.api.Secret`
# and `tabsdatak.api.Var`.
_SECRET_VALUE_FIELD = "_s"
_VAR_VALUE_FIELD = "_s"


def _to_key(path: JsonPath) -> str:
    """Convert a jsonpath into an env-var-style suffix.

    Strips the leading ``$.spec.`` (if present), maps ``.``, ``[``, ``]``
    to ``_``, and uppercases the result.
    """
    rest = path.removeprefix("$.spec.")
    for ch in ".[]":
        rest = rest.replace(ch, "_")
    return rest.upper()


def _harvest(
    data: JsonSerdeData,
    ref_kind: str,
    literal_kind: str,
    value_field: str,
) -> list[KeyValue]:
    out: list[KeyValue] = []
    for path, _env in find_by_kind(data, ref_kind):
        out.append(KeyValue(key=_to_key(path)))
    for path, env in find_by_kind(data, literal_kind):
        out.append(
            KeyValue(
                key=_to_key(path),
                value=env["spec"][value_field],
            )
        )
    return out


def extract_secrets(data: JsonSerdeData) -> list[KeyValue]:
    """Harvest every secret envelope under ``data``.

    Returns one `KeyValue` per ``secretRef`` / ``secretLiteral`` envelope.
    The key is the var form of the envelope's jsonpath (no prefix).
    The value is the literal value for ``secretLiteral`` envelopes and
    ``None`` for ``secretRef`` envelopes (the resolver fills it in).
    """
    return _harvest(
        data,
        _SECRET_REF_KIND,
        _SECRET_LITERAL_KIND,
        _SECRET_VALUE_FIELD,
    )


def extract_vars(data: JsonSerdeData) -> list[KeyValue]:
    """Same as `extract_secrets`, but for ``varRef`` / ``varLiteral``
    envelopes."""
    return _harvest(
        data,
        _VAR_REF_KIND,
        _VAR_LITERAL_KIND,
        _VAR_VALUE_FIELD,
    )


def list_refs(dehydrated_data: JsonSerdeData) -> tuple[list[str], list[str]]:
    """Return ``(secret_ref_keys, var_ref_keys)`` for a dehydrated JSON.

    The keys are the env-var-style names derived from each ref's jsonpath,
    matching the keys produced by `extract_secrets`/`extract_vars`. Unlike
    `extract_*`, this needs no literal values — it lists the ref names a
    dehydrated connection carries, for alignment checks against a separately
    supplied secrets/vars set.
    """
    secret_keys = [
        _to_key(path) for path, _ in find_by_kind(dehydrated_data, _SECRET_REF_KIND)
    ]
    var_keys = [
        _to_key(path) for path, _ in find_by_kind(dehydrated_data, _VAR_REF_KIND)
    ]
    return secret_keys, var_keys


def dehydrate_secrets(hydrated_data: JsonSerdeData) -> JsonSerdeData:
    """Return a deep copy of ``hydrated_data`` with every ``secretLiteral``
    envelope rewritten as a ``secretRef`` envelope.

    The literal value is dropped; ``_value_pattern`` is preserved so the
    resolved value is still gated downstream.
    """
    out = deepcopy(hydrated_data)
    for _path, env in find_by_kind(out, _SECRET_LITERAL_KIND):
        pattern = env["spec"].get("_value_pattern")
        env["kind"] = _SECRET_REF_KIND
        env["type"] = _SECRET_REF_TYPE
        env["spec"] = {_SECRET_VALUE_FIELD: "<secret>", "_value_pattern": pattern}
    return out


def hydrate_secrets(
    dehydrated_data: JsonSerdeData, secrets: list[KeyValue]
) -> JsonSerdeData:
    """Return a deep copy of ``dehydrated_data`` with every ``secretRef``
    envelope rewritten as a ``secretLiteral`` carrying the matching value.

    Intended as the inverse of `dehydrate_secrets`: callers `extract_secrets`
    on arrival (capturing literal values), `dehydrate_secrets` for storage,
    then later `hydrate_secrets` with the original literal values plus the
    resolved ref values. Every secret in ``dehydrated_data`` is therefore
    expected to be a ``secretRef``.

    Raises:
        CommonRuntimeException: if any entry in ``secrets`` has
            ``value is None``, or if the set of keys in ``secrets`` does
            not exactly match the keys derived from the ``secretRef``
            envelopes in ``dehydrated_data``.
    """
    missing_values = sorted(kv.key for kv in secrets if kv.value is None)
    if missing_values:
        raise CommonErrorCode.COMMON_6.exception(keys=missing_values)

    expected = {
        _to_key(path) for path, _ in find_by_kind(dehydrated_data, _SECRET_REF_KIND)
    }
    provided = {kv.key for kv in secrets}
    if expected != provided:
        raise CommonErrorCode.COMMON_7.exception(
            missing=sorted(expected - provided),
            extra=sorted(provided - expected),
        )

    values: dict[str, str] = {
        kv.key: kv.value for kv in secrets if kv.value is not None
    }

    out = deepcopy(dehydrated_data)
    for path, env in find_by_kind(out, _SECRET_REF_KIND):
        key = _to_key(path)
        pattern = env["spec"].get("_value_pattern")
        env["kind"] = _SECRET_LITERAL_KIND
        env["type"] = _SECRET_LITERAL_TYPE
        env["spec"] = {
            _SECRET_VALUE_FIELD: values[key],
            "_value_pattern": pattern,
        }
    return out


def hydrate_secrets_as_envs(
    dehydrated_data: JsonSerdeData, prefix: str
) -> JsonSerdeData:
    """Return a deep copy of ``dehydrated_data`` with every ``secretRef``
    envelope rewritten as a ``secretLiteral``, taking each value from the
    process environment under ``prefix + <inferred key>``.

    The inferred key is the var form of the envelope's jsonpath (the
    same key `extract_secrets` returns). ``prefix`` is prepended verbatim
    when reading ``os.environ``; pass ``""`` to read the bare inferred
    key.

    Raises:
        CommonRuntimeException: if any expected env var is unset (the
            usual `hydrate_secrets` missing-values error, reported with
            the inferred keys).
    """
    secrets = [
        KeyValue(key=_to_key(path), value=os.environ.get(prefix + _to_key(path)))
        for path, _ in find_by_kind(dehydrated_data, _SECRET_REF_KIND)
    ]
    return hydrate_secrets(dehydrated_data, secrets)


# ---------------------------------------------------------------------------
# Var helpers -- structurally identical to the Secret pipeline above,
# operating on the ``varRef`` / ``varLiteral`` envelope kinds. The
# `Var` class mirrors `Secret` in ``tabsdatak.api`` (no user-provided
# name; the framework infers it from the envelope's jsonpath).
# ---------------------------------------------------------------------------


def dehydrate_vars(hydrated_data: JsonSerdeData) -> JsonSerdeData:
    """Return a deep copy of ``hydrated_data`` with every ``varLiteral``
    envelope rewritten as a ``varRef`` envelope.

    The literal value is dropped; ``_value_pattern`` is preserved so
    the resolved value is still gated downstream.
    """
    out = deepcopy(hydrated_data)
    for _path, env in find_by_kind(out, _VAR_LITERAL_KIND):
        pattern = env["spec"].get("_value_pattern")
        env["kind"] = _VAR_REF_KIND
        env["type"] = _VAR_REF_TYPE
        env["spec"] = {_VAR_VALUE_FIELD: "<var>", "_value_pattern": pattern}
    return out


def hydrate_vars(
    dehydrated_data: JsonSerdeData, vars_: list[KeyValue]
) -> JsonSerdeData:
    """Return a deep copy of ``dehydrated_data`` with every ``varRef``
    envelope rewritten as a ``varLiteral`` carrying the matching value.

    Mirrors `hydrate_secrets`: callers `extract_vars` on arrival
    (capturing literal values), `dehydrate_vars` for storage, then
    later `hydrate_vars` with the original literal values plus the
    resolved ref values. Every var in ``dehydrated_data`` is therefore
    expected to be a ``varRef``.

    Raises:
        CommonRuntimeException: if any entry in ``vars_`` has
            ``value is None``, or if the set of keys in ``vars_`` does
            not exactly match the keys derived from the ``varRef``
            envelopes in ``dehydrated_data``.
    """
    missing_values = sorted(kv.key for kv in vars_ if kv.value is None)
    if missing_values:
        raise CommonErrorCode.COMMON_6.exception(keys=missing_values)

    expected = {
        _to_key(path) for path, _ in find_by_kind(dehydrated_data, _VAR_REF_KIND)
    }
    provided = {kv.key for kv in vars_}
    if expected != provided:
        raise CommonErrorCode.COMMON_7.exception(
            missing=sorted(expected - provided),
            extra=sorted(provided - expected),
        )

    values: dict[str, str] = {kv.key: kv.value for kv in vars_ if kv.value is not None}

    out = deepcopy(dehydrated_data)
    for path, env in find_by_kind(out, _VAR_REF_KIND):
        key = _to_key(path)
        pattern = env["spec"].get("_value_pattern")
        env["kind"] = _VAR_LITERAL_KIND
        env["type"] = _VAR_LITERAL_TYPE
        env["spec"] = {
            _VAR_VALUE_FIELD: values[key],
            "_value_pattern": pattern,
        }
    return out


def hydrate_vars_as_envs(dehydrated_data: JsonSerdeData, prefix: str) -> JsonSerdeData:
    """Return a deep copy of ``dehydrated_data`` with every ``varRef``
    envelope rewritten as a ``varLiteral``, taking each value from the
    process environment under ``prefix + <inferred key>``.

    The inferred key is the env-var form of the envelope's jsonpath
    (the same key `extract_vars` returns). ``prefix`` is prepended
    verbatim when reading ``os.environ``; pass ``""`` to read the bare
    inferred key.

    Raises:
        CommonRuntimeException: if any expected env var is unset.
    """
    vars_ = [
        KeyValue(
            key=_to_key(path),
            value=os.environ.get(prefix + _to_key(path)),
        )
        for path, _ in find_by_kind(dehydrated_data, _VAR_REF_KIND)
    ]
    return hydrate_vars(dehydrated_data, vars_)


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


def _resolve_connection_name(name_val: Any) -> str:
    """Resolve a ``connectionRef``'s ``name`` field to a plain string.

    ``name`` is either a literal string (``NamedConn(name="x")``) or a
    `Var` envelope (``NamedConn(name=Var._var_key("VAR"))``).
    ``varLiteral`` returns its inlined ``_s``; ``varRef`` reads
    ``os.environ[_s]``.
    """
    if isinstance(name_val, str):
        return name_val
    if isinstance(name_val, dict):
        kind = name_val.get("kind")
        if kind == _VAR_LITERAL_KIND:
            return name_val["spec"][_VAR_VALUE_FIELD]
        if kind == _VAR_REF_KIND:
            var = name_val["spec"][_VAR_VALUE_FIELD]
            val = os.environ.get(var)
            if val is None:
                raise CommonErrorCode.COMMON_9.exception(var=var)
            return val
    raise CommonErrorCode.COMMON_10.exception(name=name_val)


def extract_connection_names(data: JsonSerdeData) -> list[str]:
    """Return the de-duplicated, in-order list of connection names referenced
    by every ``connectionRef`` envelope under ``data``.

    A ``connectionRef``'s ``name`` field is either a literal string or an
    `Var` envelope; ``Var`` names are resolved against ``os.environ`` at
    call time (same semantics as `Var.value()`).
    """
    seen: dict[str, None] = {}
    for _path, env in find_by_kind(data, _CONNECTION_REF_KIND):
        seen.setdefault(
            _resolve_connection_name(env["spec"][_CONNECTION_NAME_FIELD]), None
        )
    return list(seen)


def hydrate_connections(
    data: JsonSerdeData,
    conn_by_name: dict[str, JsonSerdeData],
) -> JsonSerdeData:
    """Return a deep copy of ``data`` with every ``connectionRef`` envelope
    replaced by ``conn_by_name[<resolved name>]`` (itself deep-copied).

    The resolved name comes from the ``connectionRef``'s ``name`` field via
    `_resolve_connection_name`. Each ``connectionRef`` is replaced by an
    independent copy of the matching value in ``conn_by_name``, so callers
    can safely reuse the mapping across multiple hydrations.

    Raises:
        CommonRuntimeException: if any referenced name is missing from
            ``conn_by_name``, if the root envelope itself is a
            ``connectionRef`` (which `replace_at` cannot rewrite), or if a
            ``name`` field has an unsupported shape.
    """
    out = deepcopy(data)
    for path, env in find_by_kind(out, _CONNECTION_REF_KIND):
        if path == "$":
            raise CommonErrorCode.COMMON_5.exception()
        name = _resolve_connection_name(env["spec"][_CONNECTION_NAME_FIELD])
        if name not in conn_by_name:
            raise CommonErrorCode.COMMON_8.exception(name=name)
        replace_at(out, path, deepcopy(conn_by_name[name]))
    return out
