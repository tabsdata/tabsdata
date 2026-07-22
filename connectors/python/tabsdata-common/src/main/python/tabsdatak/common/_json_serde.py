#
# Copyright 2026 Tabsdata Inc.
#

"""Visitor helpers for the dict-representation of a `_JsonSerde` subclass,
plus the secret dehydrate/hydrate pipeline built on top.

A serialized `_JsonSerde` subclass is a tree of *envelopes*:

    {"kind": ..., "apiVersion": ..., "type": ..., "spec": {...}}

`spec` may contain plain fields, lists, or further envelopes (nested
`_JsonSerde` payload). `_secret_refs` walks the tree for secret string
leaves and yields a jsonpath for each; `replace_at` writes a new value at
such a path.

Secrets are NOT envelopes. A `tabsdatak.api.Secret` string leaf is one
of two wire forms: the dehydrated marker ``":secret:"`` (no value), or the
hydrated ``"secret:<value>"``. A third, create-time *input* form
``"$secret:<VAR>"`` sources the value from an environment variable; it is
rewritten to a wire form before storage and never persists. The secret
helpers walk the tree for those leaves and key each one on the
env-var-style form of its jsonpath:

* `resolve_env_secret_refs` runs first, at connection create/upsert time:
  it rewrites each ``"$secret:<VAR>"`` input leaf to ``"secret:<value>"``
  read from ``os.environ``, before the document is validated and
  dehydrated. ``"$secret:"`` never reaches the stored or hydrated form.
* `dehydrate` strips every secret value out of the tree (replacing
  ``"secret:hunter2"`` with ``":secret:"``) and returns it alongside a
  `KeyValue` list, matching the ``Vec<KeyValue>`` the apiserver's
  connection upsert expects. Keys are the jsonpath form prefixed with
  ``SECRET_``. A *type-aware* ``reference`` (a ``model.to_dict()``) may be
  passed so secret leaves are located by field type rather than by the
  ``"secret:"`` prefix -- harvesting even an un-prefixed value so it never
  lands in the stored connection in plain text.
* `hydrate` is the inverse: given the dehydrated tree plus the
  `KeyValue` list (with values filled in), it writes each value back as
  ``"secret:<value>"``.
* `env_hydrate` fills dehydrated secret leaves from the process
  environment, reading the same ``SECRET_<key>`` names.
* `list_refs` lists the secret keys a tree carries (for the
  ``secret_names`` an apiserver connection read returns).
"""

from __future__ import annotations

import os
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, TypeAlias, Union

from tabsdatak.common._error import CommonErrorCode

type JsonPath = str


JsonSerdeData = Union[dict, list]

# Valid env-var-style key name (uppercase, no spaces).
KeyNameSpec: TypeAlias = str


@dataclass
class KeyValue:
    key: KeyNameSpec
    value: str | None = None


# ---------------------------------------------------------------------------
# Visitor helpers
# ---------------------------------------------------------------------------


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


def replace_at(json_serde_data: JsonSerdeData, path: JsonPath, value: Any) -> None:
    """Replace the element at `path` inside `node` with `value`, in place.

    `path` must use the jsonpath syntax produced by `_secret_refs`.
    Replacing the root (``$``) is not supported — the caller already holds
    that reference and can assign to it directly.
    """
    tokens = _parse_path(path)
    if not tokens:
        raise CommonErrorCode.COMMON_5.exception()
    target = json_serde_data
    for token in tokens[:-1]:
        target = target[token]
    target[tokens[-1]] = value


_MISSING = object()


def _value_at(json_serde_data: JsonSerdeData, path: JsonPath) -> Any:
    """The value at `path` inside `node`, or :data:`_MISSING` if any
    segment of the path is absent. Uses the syntax produced by
    `_secret_refs`.
    """
    cur: Any = json_serde_data
    for token in _parse_path(path):
        try:
            cur = cur[token]
        except (KeyError, IndexError, TypeError):
            return _MISSING
    return cur


# ---------------------------------------------------------------------------
# Secret helpers
# ---------------------------------------------------------------------------

# Wire forms for a secret string leaf -- must match `tabsdatak.api.Secret`:
#   * ``":secret:"``       -- a dehydrated secret (no value).
#   * ``"secret:<value>"`` -- a hydrated secret carrying ``<value>``.
_SECRET_DEHYDRATED = ":secret:"
_SECRET_PREFIX = "secret:"

# Create-time input form: ``"$secret:<VAR>"`` names an environment variable
# to read the secret value from. `resolve_env_secret_refs` rewrites it to
# ``"secret:<value>"`` before validation / `dehydrate`; it never survives
# into a stored or hydrated document.
_SECRET_ENV_REF_PREFIX = "$secret:"

# Key prefix for the harvested `KeyValue` keys (and the env-var names
# `env_hydrate` reads). Secrets get ``SECRET_CONN__``.
_SECRET_KEY_PREFIX = "SECRET_CONN__"


def _to_key(path: JsonPath) -> str:
    """Convert a jsonpath into an env-var-style suffix.

    Strips the leading ``$.spec.`` (if present), maps ``.``, ``[``, ``]``
    to ``_``, and uppercases the result.
    """
    rest = path.removeprefix("$.spec.")
    for ch in ".[]":
        rest = rest.replace(ch, "_")
    return rest.upper()


def _parse_leaf(leaf: str) -> str | None:
    """The value a secret leaf carries: ``":secret:"`` -> ``None``,
    ``"secret:<value>"`` -> ``"<value>"``, and a bare ``"<value>"`` ->
    ``"<value>"`` (an un-prefixed value supplied for a ``Secret`` field).
    """
    if leaf == _SECRET_DEHYDRATED:
        return None
    if leaf.startswith(_SECRET_PREFIX):
        return leaf[len(_SECRET_PREFIX) :]
    return leaf


def _secret_refs(data: JsonSerdeData) -> list[tuple[JsonPath, str | None]]:
    """Return ``(json_path, value)`` for every secret string leaf under
    ``data``.

    ``value`` is the hydrated value (the text after the ``"secret:"``
    prefix), or ``None`` for a dehydrated ``":secret:"`` leaf. Paths use
    the ``$.key[i]`` jsonpath syntax that `replace_at` and `_to_key`
    accept.
    """
    found: list[tuple[JsonPath, str | None]] = []

    def walk(n: Any, path: JsonPath) -> None:
        if isinstance(n, dict):
            for k, v in n.items():
                walk(v, f"{path}.{k}")
        elif isinstance(n, list):
            for i, v in enumerate(n):
                walk(v, f"{path}[{i}]")
        elif isinstance(n, str):
            if n == _SECRET_DEHYDRATED:
                found.append((path, None))
            elif n.startswith(_SECRET_PREFIX):
                found.append((path, n[len(_SECRET_PREFIX) :]))

    walk(data, "$")
    return found


def dehydrate(
    data: JsonSerdeData,
    reference: JsonSerdeData | None = None,
) -> tuple[JsonSerdeData, list[KeyValue]]:
    """Strip every secret value out of ``data``.

    Returns ``(dehydrated, secrets)``:

    * ``dehydrated`` -- a deep copy of ``data`` with every secret leaf
      reduced to the dehydrated marker ``":secret:"``.
    * ``secrets`` -- one `KeyValue` per leaf, keyed by the jsonpath form
      prefixed with ``SECRET_``. ``value`` is the value the leaf carried
      (the text after a ``"secret:"`` prefix, or a bare un-prefixed
      ``"<value>"``), or ``None`` for a leaf that was already dehydrated.

    ``reference`` locates the secret leaves; it defaults to ``data``, so
    secrets are found by scanning ``data`` for the wire forms
    (``":secret:"`` / ``"secret:<value>"``). Pass a *type-aware* reference
    -- typically ``model.to_dict()`` of the validated document -- to
    locate secrets by field type instead: a ``Secret`` field serializes
    to ``":secret:"``, so its position is harvested regardless of how the
    value was written in ``data``. This is what stops a secret value
    supplied without the ``"secret:"`` prefix (e.g. a bare ``"hunter2"``)
    from leaking into the stored connection in plain text.

    The list serializes to the ``Vec<KeyValue>`` the apiserver's
    connection upsert expects (``[{"key": ..., "value": ...}]``).
    """
    if reference is None:
        reference = data
    out = deepcopy(data)
    secrets: list[KeyValue] = []
    for path, _ in _secret_refs(reference):
        leaf = _value_at(data, path)
        if leaf is _MISSING:
            # Present in the reference (e.g. a model default) but absent
            # from ``data`` -- nothing in the document to harvest or blank.
            continue
        value = _parse_leaf(leaf) if isinstance(leaf, str) else None
        secrets.append(KeyValue(key=_SECRET_KEY_PREFIX + _to_key(path), value=value))
        replace_at(out, path, _SECRET_DEHYDRATED)
    return out, secrets


def hydrate(
    data: JsonSerdeData,
    secrets: list[KeyValue],
) -> JsonSerdeData:
    """Write secret values back into a dehydrated tree.

    The inverse of `dehydrate`: given the dehydrated ``data`` plus the
    `KeyValue` list (with every ``value`` filled in), returns a deep copy
    with each ``":secret:"`` leaf restored to ``"secret:<value>"``.

    Raises:
        CommonRuntimeException: if any entry in ``secrets`` has
            ``value is None`` (COMMON_6), or if the set of keys does not
            exactly match the keys derived from the leaves in ``data``
            (COMMON_7).
    """
    out = deepcopy(data)
    missing_values = sorted(kv.key for kv in secrets if kv.value is None)
    if missing_values:
        raise CommonErrorCode.COMMON_6.exception(keys=missing_values)

    refs = _secret_refs(out)
    plaintext = sorted(path for path, value in refs if value is not None)
    if plaintext:
        raise CommonErrorCode.COMMON_13.exception(paths=plaintext)
    expected = {_SECRET_KEY_PREFIX + _to_key(path) for path, _ in refs}
    provided = {kv.key for kv in secrets}
    if expected != provided:
        raise CommonErrorCode.COMMON_7.exception(
            missing=sorted(expected - provided),
            extra=sorted(provided - expected),
        )

    values = {kv.key: kv.value for kv in secrets}
    for path, _ in refs:
        replace_at(
            out, path, _SECRET_PREFIX + values[_SECRET_KEY_PREFIX + _to_key(path)]
        )
    return out


def env_hydrate(data: JsonSerdeData) -> JsonSerdeData:
    """Fill dehydrated secret leaves from the process environment.

    Returns a deep copy of ``data`` with every ``":secret:"`` leaf set
    from ``os.environ["SECRET_<key>"]`` as ``"secret:<value>"`` (``<key>``
    is the jsonpath form, the same suffix `dehydrate` produces). Hydrated
    leaves are left untouched.

    Raises:
        CommonRuntimeException: if any expected env var is unset
            (COMMON_6, reported with the missing ``SECRET_`` keys).
    """
    out = deepcopy(data)
    plan: list[tuple[JsonPath, str]] = []
    missing: list[str] = []
    plaintext: list[JsonPath] = []
    for path, value in _secret_refs(out):
        # The input must be dehydrated: a leaf still carrying a value
        # (``"secret:<value>"``) is a plaintext secret and is rejected.
        if value is not None:
            plaintext.append(path)
            continue
        key = _SECRET_KEY_PREFIX + _to_key(path)
        env_value = os.environ.get(key)
        if env_value is None:
            missing.append(key)
        else:
            plan.append((path, _SECRET_PREFIX + env_value))
    if plaintext:
        raise CommonErrorCode.COMMON_13.exception(paths=sorted(plaintext))
    if missing:
        raise CommonErrorCode.COMMON_6.exception(keys=sorted(set(missing)))
    for path, new_value in plan:
        replace_at(out, path, new_value)
    return out


def _env_secret_refs(data: JsonSerdeData) -> list[tuple[JsonPath, str]]:
    """Return ``(json_path, var_name)`` for every ``"$secret:<VAR>"`` string
    leaf under ``data`` -- the create-time env-reference form. Paths use the
    same ``$.key[i]`` jsonpath syntax `replace_at` accepts.
    """
    found: list[tuple[JsonPath, str]] = []

    def walk(n: Any, path: JsonPath) -> None:
        if isinstance(n, dict):
            for k, v in n.items():
                walk(v, f"{path}.{k}")
        elif isinstance(n, list):
            for i, v in enumerate(n):
                walk(v, f"{path}[{i}]")
        elif isinstance(n, str) and n.startswith(_SECRET_ENV_REF_PREFIX):
            found.append((path, n[len(_SECRET_ENV_REF_PREFIX) :]))

    walk(data, "$")
    return found


def resolve_env_secret_refs(data: JsonSerdeData) -> JsonSerdeData:
    """Resolve ``"$secret:<VAR>"`` leaves from the process environment.

    Returns a deep copy of ``data`` with every ``"$secret:<VAR>"`` string
    leaf replaced by ``"secret:<value>"``, where ``<value>`` is
    ``os.environ["<VAR>"]`` (a var set to the empty string is a valid empty
    secret). This runs on connection create/upsert, before the document is
    validated and `dehydrate`d: it lets a secret value be supplied from an
    environment variable instead of being inlined. After it runs no
    ``"$secret:"`` leaf remains -- the value flows through the normal secret
    pipeline (validated at its field, harvested by `dehydrate`, stored as
    ``":secret:"``), so it is irrelevant to `hydrate` / `env_hydrate` /
    `list_refs`.

    Raises:
        CommonRuntimeException: if any referenced env var is unset
            (COMMON_12).
    """
    out = deepcopy(data)
    refs = _env_secret_refs(out)
    missing = sorted({var for _, var in refs if os.environ.get(var) is None})
    if missing:
        raise CommonErrorCode.COMMON_12.exception(vars=missing)
    for path, var in refs:
        replace_at(out, path, _SECRET_PREFIX + os.environ[var])
    return out


def env_secret_ref_vars(data: JsonSerdeData) -> list[str]:
    """Return the sorted, de-duplicated environment variable names referenced
    by ``"$secret:<VAR>"`` leaves in ``data`` (before resolution). Backs the
    "these env vars were read" notice the create/upsert CLI prints.
    """
    return sorted({var for _, var in _env_secret_refs(data)})


def list_refs(data: JsonSerdeData) -> list[str]:
    """Return ``secret_keys`` for ``data``.

    The keys are the ``SECRET_``-prefixed jsonpath forms of every secret
    leaf -- matching the `KeyValue` keys `dehydrate` produces. This backs
    the ``secret_names`` an apiserver connection read returns.
    """
    return [_SECRET_KEY_PREFIX + _to_key(path) for path, _ in _secret_refs(data)]
