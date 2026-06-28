"""Visitor helpers for the dict-representation of a `_JsonSerde` subclass,
plus the secret/var dehydrate/hydrate pipeline built on top.

A serialized `_JsonSerde` subclass is a tree of *envelopes*:

    {"kind": ..., "apiVersion": ..., "type": ..., "spec": {...}}

`spec` may contain plain fields, lists, or further envelopes (nested
`_JsonSerde` payload). `find_by_kind` walks the tree and yields every
envelope whose `kind` matches; `replace_at` writes a new value at a path
returned by the walker. These power the *connection* helpers
(`extract_connection_names`, `hydrate_connections`) -- a `NamedConn`
serializes to an envelope of kind ``connectionRef``.

Secrets and vars are NOT envelopes. A `tabsdatak.api.Secret` serializes
to a single string ``"secret:<value>"`` (``"secret:"`` when unhydrated);
a `tabsdatak.api.Var` to ``"var:<value>"`` (``"var:"`` when unhydrated).
The secret/var helpers walk the tree for those string leaves and key
each one on the var-style form of its jsonpath:

* `dehydrate` strips every secret/var value out of the tree (replacing
  ``"secret:hunter2"`` with ``"secret:"``) and returns it alongside two
  `KeyValue` lists -- one for secrets, one for vars -- matching the
  ``Vec<KeyValue>`` the apiserver's connection upsert expects. Keys are
  the jsonpath form prefixed with ``SECRET_`` / ``VAR_``.
* `hydrate` is the inverse: given the dehydrated tree plus the two
  `KeyValue` lists (with values filled in), it writes each value back.
* `env_hydrate` fills unhydrated secret/var leaves from the process
  environment, reading the same ``SECRET_<key>`` / ``VAR_<key>`` names.
* `list_refs` lists the secret/var keys a tree carries (for the
  ``secret_names`` / ``var_names`` an apiserver connection read returns).
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
    json_serde_data: JsonSerdeData, path: JsonPath, value: Any
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

# Wire prefixes -- must match ``tabsdatak.api.Secret._prefix`` /
# ``tabsdatak.api.Var._prefix``. A secret/var is a plain string leaf
# ``"<prefix><value>"``; ``"<prefix>"`` (no value) is an unhydrated ref.
_SECRET_PREFIX = "secret:"
_VAR_PREFIX = "var:"

# Key prefixes for the harvested `KeyValue` keys (and the env-var names
# `env_hydrate` reads). Secrets get ``SECRET_``, vars ``VAR_``.
_SECRET_KEY_PREFIX = "SECRET_"
_VAR_KEY_PREFIX = "VAR_"


def _to_key(path: JsonPath) -> str:
    """Convert a jsonpath into an env-var-style suffix.

    Strips the leading ``$.spec.`` (if present), maps ``.``, ``[``, ``]``
    to ``_``, and uppercases the result.
    """
    rest = path.removeprefix("$.spec.")
    for ch in ".[]":
        rest = rest.replace(ch, "_")
    return rest.upper()


def _value_refs(
    data: JsonSerdeData, prefix: str
) -> list[tuple[JsonPath, str | None]]:
    """Return ``(json_path, value)`` for every string leaf under ``data``
    starting with ``prefix``.

    ``value`` is the text after the prefix, or ``None`` when the leaf is a
    bare ``"<prefix>"`` (an unhydrated ref). Paths use the same syntax as
    `find_by_kind`, so `replace_at` and `_to_key` accept them.
    """
    found: list[tuple[JsonPath, str | None]] = []

    def walk(n: Any, path: JsonPath) -> None:
        if isinstance(n, dict):
            for k, v in n.items():
                walk(v, f"{path}.{k}")
        elif isinstance(n, list):
            for i, v in enumerate(n):
                walk(v, f"{path}[{i}]")
        elif isinstance(n, str) and n.startswith(prefix):
            found.append((path, n[len(prefix) :] or None))

    walk(data, "$")
    return found


def _harvest(
    out: JsonSerdeData, prefix: str, key_prefix: str
) -> list[KeyValue]:
    """Collect a `KeyValue` per ``prefix`` leaf under ``out`` and blank
    each leaf back to the bare ``"<prefix>"`` placeholder, in place."""
    harvested: list[KeyValue] = []
    for path, value in _value_refs(out, prefix):
        harvested.append(KeyValue(key=key_prefix + _to_key(path), value=value))
        replace_at(out, path, prefix)
    return harvested


def dehydrate(
    data: JsonSerdeData,
) -> tuple[JsonSerdeData, list[KeyValue], list[KeyValue]]:
    """Strip every secret/var value out of ``data``.

    Returns ``(dehydrated, secrets, vars)``:

    * ``dehydrated`` -- a deep copy of ``data`` with every secret leaf
      reduced to ``"secret:"`` and every var leaf to ``"var:"``.
    * ``secrets`` / ``vars`` -- one `KeyValue` per leaf, keyed by the
      jsonpath form prefixed with ``SECRET_`` / ``VAR_``. ``value`` is
      the inline value the leaf carried, or ``None`` for a leaf that was
      already an unhydrated ref.

    The two lists serialize to the ``Vec<KeyValue>`` the apiserver's
    connection upsert expects (``[{"key": ..., "value": ...}]``).
    """
    out = deepcopy(data)
    secrets = _harvest(out, _SECRET_PREFIX, _SECRET_KEY_PREFIX)
    vars_ = _harvest(out, _VAR_PREFIX, _VAR_KEY_PREFIX)
    return out, secrets, vars_


def _restore(
    out: JsonSerdeData, prefix: str, key_prefix: str, kvs: list[KeyValue]
) -> None:
    missing_values = sorted(kv.key for kv in kvs if kv.value is None)
    if missing_values:
        raise CommonErrorCode.COMMON_6.exception(keys=missing_values)

    refs = _value_refs(out, prefix)
    expected = {key_prefix + _to_key(path) for path, _ in refs}
    provided = {kv.key for kv in kvs}
    if expected != provided:
        raise CommonErrorCode.COMMON_7.exception(
            missing=sorted(expected - provided),
            extra=sorted(provided - expected),
        )

    values = {kv.key: kv.value for kv in kvs}
    for path, _ in refs:
        replace_at(out, path, prefix + values[key_prefix + _to_key(path)])


def hydrate(
    data: JsonSerdeData,
    secrets: list[KeyValue],
    vars_: list[KeyValue],
) -> JsonSerdeData:
    """Write secret/var values back into a dehydrated tree.

    The inverse of `dehydrate`: given the dehydrated ``data`` plus the
    two `KeyValue` lists (with every ``value`` filled in), returns a deep
    copy with each ``"secret:"`` / ``"var:"`` leaf restored to
    ``"secret:<value>"`` / ``"var:<value>"``.

    Raises:
        CommonRuntimeException: if any entry in ``secrets`` / ``vars_``
            has ``value is None`` (COMMON_6), or if the set of keys does
            not exactly match the keys derived from the leaves in
            ``data`` (COMMON_7).
    """
    out = deepcopy(data)
    _restore(out, _SECRET_PREFIX, _SECRET_KEY_PREFIX, secrets)
    _restore(out, _VAR_PREFIX, _VAR_KEY_PREFIX, vars_)
    return out


def _env_restore(out: JsonSerdeData, prefix: str, key_prefix: str) -> None:
    plan: list[tuple[JsonPath, str]] = []
    missing: list[str] = []
    for path, value in _value_refs(out, prefix):
        # Inline literals are left untouched; only unhydrated refs
        # (bare ``"<prefix>"``) are resolved from the environment.
        if value is not None:
            continue
        key = key_prefix + _to_key(path)
        env_value = os.environ.get(key)
        if env_value is None:
            missing.append(key)
        else:
            plan.append((path, prefix + env_value))
    if missing:
        raise CommonErrorCode.COMMON_6.exception(keys=sorted(set(missing)))
    for path, new_value in plan:
        replace_at(out, path, new_value)


def env_hydrate(data: JsonSerdeData) -> JsonSerdeData:
    """Fill unhydrated secret/var leaves from the process environment.

    Returns a deep copy of ``data`` with every bare ``"secret:"`` leaf
    set from ``os.environ["SECRET_<key>"]`` and every bare ``"var:"``
    leaf from ``os.environ["VAR_<key>"]`` (``<key>`` is the jsonpath
    form, the same suffix `dehydrate` produces). Inline literals are left
    untouched.

    Raises:
        CommonRuntimeException: if any expected env var is unset
            (COMMON_6, reported with the missing ``SECRET_`` / ``VAR_``
            keys).
    """
    out = deepcopy(data)
    _env_restore(out, _SECRET_PREFIX, _SECRET_KEY_PREFIX)
    _env_restore(out, _VAR_PREFIX, _VAR_KEY_PREFIX)
    return out


def list_refs(data: JsonSerdeData) -> tuple[list[str], list[str]]:
    """Return ``(secret_keys, var_keys)`` for ``data``.

    The keys are the ``SECRET_`` / ``VAR_``-prefixed jsonpath forms of
    every secret/var leaf -- matching the `KeyValue` keys `dehydrate`
    produces. This backs the ``secret_names`` / ``var_names`` an
    apiserver connection read returns.
    """
    secret_keys = [
        _SECRET_KEY_PREFIX + _to_key(path)
        for path, _ in _value_refs(data, _SECRET_PREFIX)
    ]
    var_keys = [
        _VAR_KEY_PREFIX + _to_key(path) for path, _ in _value_refs(data, _VAR_PREFIX)
    ]
    return secret_keys, var_keys


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

# On purpose, we don't use constants for the __kind__ attributes.
# Not to break backwards compatibility with serialized data
# out there unknowingly because of a change of constant that would not
# break things in our code because we are using the constant all over,
# but it would break things outside.
_CONNECTION_REF_KIND = "connectionRef"
_CONNECTION_NAME_FIELD = "name"


def extract_connection_names(data: JsonSerdeData) -> list[str]:
    """Return the de-duplicated, in-order list of connection names
    referenced by every ``connectionRef`` envelope under ``data``.

    A ``connectionRef``'s ``name`` field is a plain string literal.
    """
    seen: dict[str, None] = {}
    for _path, env in find_by_kind(data, _CONNECTION_REF_KIND):
        seen.setdefault(env["spec"][_CONNECTION_NAME_FIELD], None)
    return list(seen)


def hydrate_connections(
    data: JsonSerdeData,
    conn_by_name: dict[str, JsonSerdeData],
) -> JsonSerdeData:
    """Return a deep copy of ``data`` with every ``connectionRef`` envelope
    replaced by ``conn_by_name[<name>]`` (itself deep-copied).

    The name comes from the ``connectionRef``'s ``name`` field (a plain
    string). Each ``connectionRef`` is replaced by an independent copy of
    the matching value in ``conn_by_name``, so callers can safely reuse
    the mapping across multiple hydrations.

    Raises:
        CommonRuntimeException: if any referenced name is missing from
            ``conn_by_name`` (COMMON_8), or if the root envelope itself is
            a ``connectionRef`` (COMMON_5, which `replace_at` cannot
            rewrite).
    """
    out = deepcopy(data)
    for path, env in find_by_kind(out, _CONNECTION_REF_KIND):
        if path == "$":
            raise CommonErrorCode.COMMON_5.exception()
        name = env["spec"][_CONNECTION_NAME_FIELD]
        if name not in conn_by_name:
            raise CommonErrorCode.COMMON_8.exception(name=name)
        replace_at(out, path, deepcopy(conn_by_name[name]))
    return out
