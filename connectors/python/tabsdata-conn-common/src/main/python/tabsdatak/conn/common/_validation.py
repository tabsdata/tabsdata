#
# Copyright 2026 Tabsdata Inc.
#

"""Cross-connector pydantic validators shared by every
``tabsdatak.conn.*`` package: fully-qualified-name parsing,
identifier shape checks, conn_cfg / src_cfg / dest_cfg key-set
validators, etc. Connectors compose these into their own
``Annotated[...]`` field aliases rather than re-implementing
them."""

from __future__ import annotations

import re
from typing import Annotated, Any, TypeAlias

from pydantic import AfterValidator, model_validator

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.conn.common.error import ConnErrorCode
from tabsdatak.error import ConstraintError

# --- FQN syntax + helpers --------------------------------------------


# Field specs. Per-field non-emptiness rules ride the regular pydantic
# validation pipeline via ``Annotated[..., _api.ValueConstraints(...)]``
# instead of a hand-written ``__post_init__``. Cross-field rules
# (``escape``/``outer_only`` each requiring ``quote``) live in
# ``FqnDef._check_dependencies`` because pydantic field annotations only
# see one field at a time.
SEPARATOR_VALIDATION = _api.ValueConstraints(
    _api.Rule.length(ConnErrorCode.CONN_26, min_len=1)
)
QUOTE_VALIDATION = _api.ValueConstraints(
    _api.Rule.length(ConnErrorCode.CONN_27, min_len=1)
)

SeparatorSpec: TypeAlias = Annotated[str, SEPARATOR_VALIDATION]
QuoteSpec: TypeAlias = Annotated[str, QUOTE_VALIDATION]


@_api.dataclass(ConnErrorCode.CONN_9, kw_only=True)
class FqnDef:
    """Describes the syntax of a fully-qualified-name (FQN) for a
    particular system.

    Used by :func:`fqn` to assemble an FQN string from its parts and by
    :func:`unfqn` to split one back. Examples of the shapes this
    dataclass captures:

    - **Per-part quoting** (Databricks, ANSI SQL):
      ``FqnDef(separator='.', quote='`')`` quotes each part with backticks
      and joins them with dots (a, b, c -> backtick-a-backtick . ...).
    - **Outer-only quoting** (BigQuery):
      ``FqnDef(separator='.', quote='`', outer_only=True)`` wraps the whole
      dotted name in a single pair of backticks.
    - **Asymmetric quoting** (MSSQL / SQL Server):
      ``FqnDef(separator='.', quote='[', close_quote=']', escape=']')``
      -> ``[a].[b].[c]``
    - **No quoting** (purely-textual FQNs):
      ``FqnDef(separator='.')`` -> ``a.b.c``

    Attributes:
        separator: the (non-empty) string placed between parts.
        quote: optional quote character used to wrap parts that need
            escaping. When ``None`` no quoting is allowed; :func:`fqn`
            emits bare parts and :func:`unfqn` splits on ``separator``.
            When ``close_quote`` is set, ``quote`` is the opening
            delimiter only.
        close_quote: optional closing delimiter for asymmetric quoting
            (e.g. ``]`` paired with an opening ``[``). When ``None``
            (default), ``quote`` is used as both open and close
            (symmetric). Requires ``quote``.
        escape: optional escape sequence for the (close) quote character
            inside a quoted part. The two characters
            ``escape + close_quote_effective`` represent a literal
            close-quote character. The common case ``escape == quote``
            (or ``escape == close_quote``) encodes the SQL "doubled
            quote" / "doubled bracket" convention (``""`` -> ``"``;
            ``]]`` -> ``]``). When ``None``, the close-quote character
            is not allowed inside a part. Requires ``quote``.
        outer_only: when ``True`` the entire FQN is wrapped in one pair
            of ``quote`` / ``close_quote`` characters (BigQuery style)
            rather than each part being quoted individually. Requires
            ``quote``.
    """

    separator: SeparatorSpec
    quote: QuoteSpec | None = None
    close_quote: QuoteSpec | None = None
    escape: str | None = None
    outer_only: bool = False

    @model_validator(mode="after")
    def _check_dependencies(self) -> "FqnDef":
        if self.escape is not None and self.quote is None:
            raise ValueError("FqnDef.escape requires FqnDef.quote to be set")
        if self.outer_only and self.quote is None:
            raise ValueError("FqnDef.outer_only requires FqnDef.quote to be set")
        if self.close_quote is not None and self.quote is None:
            raise ValueError("FqnDef.close_quote requires FqnDef.quote to be set")
        return self


def fqn(parts: list[str], spec: FqnDef) -> str:
    """Assemble a fully-qualified-name from its parts.

    When ``spec.quote`` is set and ``spec.outer_only`` is false, every
    part is wrapped in ``spec.quote`` (predictable, round-trippable
    output). When ``spec.outer_only`` is true, the parts are joined
    bare with ``spec.separator`` and the whole result is wrapped once.
    When ``spec.quote`` is ``None``, parts must not contain
    ``spec.separator`` and are joined bare.

    A part that is already wrapped in ``spec.quote`` (per-part mode
    only) is passed through as-is. This makes it safe to feed quoted
    output from another system -- or the output of :func:`unfqn` after
    a manual edit -- back through :func:`fqn` without double-quoting.

    Args:
        parts: non-empty list of non-empty parts.
        spec: the FQN syntax.

    Raises:
        ValueError: ``parts`` is empty; any part is empty; a part
            contains the quote character with no ``spec.escape``
            defined; or a part contains the separator while
            ``spec.quote`` is ``None``.
    """
    if not parts:
        raise ValueError("fqn: parts must be non-empty")
    for i, p in enumerate(parts):
        if not p:
            raise ValueError(f"fqn: part at index {i} is empty")

    sep, q, esc = spec.separator, spec.quote, spec.escape
    # Effective close delimiter -- equal to ``q`` for symmetric
    # quoting, distinct for asymmetric (e.g. ``[`` / ``]``).
    c = spec.close_quote if spec.close_quote is not None else q

    if spec.outer_only:
        assert q is not None and c is not None  # enforced by FqnDef
        for i, p in enumerate(parts):
            if sep in p:
                raise ValueError(
                    f"fqn: part at index {i} contains separator {sep!r} "
                    f"under outer_only: {p!r}"
                )
        joined = sep.join(parts)
        if c in joined:
            if esc is None:
                raise ValueError(
                    f"fqn: FQN contains quote {c!r} with no escape defined: {joined!r}"
                )
            joined = joined.replace(c, esc + c)
        return f"{q}{joined}{c}"

    if q is None:
        for i, p in enumerate(parts):
            if sep in p:
                raise ValueError(
                    f"fqn: part at index {i} contains separator {sep!r} "
                    f"with no quoting available: {p!r}"
                )
        return sep.join(parts)

    assert c is not None  # follows from ``q is not None``
    quoted: list[str] = []
    for i, p in enumerate(parts):
        # No-op pass-through for parts that are already wrapped in
        # ``q``..``c`` (caller's assertion; we don't inspect the body).
        if len(p) >= len(q) + len(c) and p.startswith(q) and p.endswith(c):
            quoted.append(p)
            continue
        body = p
        if c in body:
            if esc is None:
                raise ValueError(
                    f"fqn: part at index {i} contains quote {c!r} with "
                    f"no escape defined: {p!r}"
                )
            body = body.replace(c, esc + c)
        quoted.append(f"{q}{body}{c}")
    return sep.join(quoted)


def complete_levels(
    parts: list[str | None],
    defaults: list[str | None],
) -> list[str | None]:
    """Fill the missing upper levels of a parsed hierarchical name from
    positional connection-level defaults.

    ``parts`` and ``defaults`` are positionally aligned to the same fixed
    levels, most-significant first -- e.g. ``[catalog, schema, table]``
    paired with ``[conn.catalog, conn.schema, None]``. ``parts`` is the
    output of :func:`unfqn` with ``total_parts=len(defaults)`` (or an
    equivalent split left-padded with ``None``): the specified levels are
    a non-``None`` suffix and any missing upper levels are a leading run
    of ``None``.

    The leading ``None`` levels are filled *contiguously* -- from the
    innermost missing level (just above the first specified part) outward
    toward the most-significant level -- stopping at the first level whose
    default is ``None``. Filling contiguously, rather than each level
    independently, guarantees a component is never shifted into the wrong
    slot: a level is defaulted only once every level below it (down to the
    specified part) is resolved. A level left unresolved stays ``None``;
    this function never raises on a missing default. Callers that require
    a fully-qualified result check the returned list for a leading
    ``None`` and raise their own typed error.

    Present (non-``None``) parts are never overwritten.

    Args:
        parts: the parsed levels; missing upper levels are a leading run
            of ``None``.
        defaults: per-level default values positionally aligned to
            ``parts``; ``None`` where no default applies.

    Returns:
        A new list the same length as ``parts`` with leading ``None``
        levels filled from ``defaults`` as far as the contiguous run of
        available defaults reaches.

    Raises:
        ValueError: ``parts`` and ``defaults`` differ in length, or a
            ``None`` appears in ``parts`` after a non-``None`` level
            (only leading levels may be missing).
    """
    if len(parts) != len(defaults):
        raise ValueError(
            f"complete_levels: parts has {len(parts)} entries, "
            f"defaults has {len(defaults)}"
        )

    out = list(parts)

    leading = 0
    for p in out:
        if p is None:
            leading += 1
        else:
            break

    for i in range(leading, len(out)):
        if out[i] is None:
            raise ValueError(
                f"complete_levels: None at position {i} after the leading "
                f"run; only leading levels may be missing: {parts!r}"
            )

    for i in range(leading - 1, -1, -1):
        if defaults[i] is None:
            break
        out[i] = defaults[i]

    return out


def unfqn(
    value: str,
    spec: FqnDef,
    total_parts: int | None = None,
) -> list[str | None]:
    """Split a fully-qualified-name back into its parts.

    When ``spec.outer_only`` is true the value must be wrapped in a
    single ``spec.quote`` pair; the body is then split bare on
    ``spec.separator`` and any ``spec.escape + spec.quote`` pairs are
    unescaped. Otherwise each part is detected independently: a part
    that begins with ``spec.quote`` is parsed as quoted (unescaping
    ``spec.escape + spec.quote`` pairs along the way), and any other
    part is read bare up to the next ``spec.separator``. Quoted and
    bare parts may be mixed freely.

    When ``total_parts`` is set, the returned list is left-padded with
    ``None`` so it always has exactly ``total_parts`` entries. This is
    useful for systems that allow partially-qualified names: e.g.
    ``unfqn("table", spec, total_parts=3)`` returns
    ``[None, None, "table"]``, leaving slots for ``catalog`` /
    ``schema`` to be filled in by the caller.

    Args:
        value: non-empty FQN string.
        spec: the FQN syntax.
        total_parts: when set, the exact length of the returned list.
            Must be a positive integer at least as large as the number
            of parts found in ``value``.

    Returns:
        The element list (with quote / escape characters stripped).
        When ``total_parts`` is set, the list is left-padded with
        ``None`` to that length; otherwise it has exactly as many
        entries as ``value`` contains.

    Raises:
        ValueError: ``value`` is empty; quoted identifier is
            unterminated; a bare part is empty; a stray quote appears
            inside a bare part; a trailing or leading separator is
            seen; ``total_parts`` is non-positive; or ``value`` has more
            parts than ``total_parts`` allows.
    """
    if not value:
        raise ValueError("unfqn: value must be non-empty")
    if total_parts is not None and total_parts < 1:
        raise ValueError(
            f"unfqn: total_parts must be a positive integer: {total_parts!r}"
        )

    sep, q, esc = spec.separator, spec.quote, spec.escape
    c = spec.close_quote if spec.close_quote is not None else q

    parts: list[str | None]
    if spec.outer_only:
        assert q is not None and c is not None  # enforced by FqnDef
        if (
            len(value) < len(q) + len(c)
            or not value.startswith(q)
            or not value.endswith(c)
        ):
            raise ValueError(
                f"unfqn: outer_only FQN must be wrapped in {q!r}..{c!r}: {value!r}"
            )
        inner = value[len(q) : -len(c)]
        if esc is not None:
            inner = inner.replace(esc + c, c)
        parts = list(inner.split(sep))
    elif q is None:
        parts = list(value.split(sep))
    else:
        assert c is not None
        parts = _split_quoted(value, sep, q, c, esc)

    if total_parts is not None:
        if len(parts) > total_parts:
            raise ValueError(
                f"unfqn: value has {len(parts)} parts, exceeds "
                f"total_parts={total_parts}: {value!r}"
            )
        parts = [None] * (total_parts - len(parts)) + parts
    return parts


def _split_quoted(
    value: str,
    sep: str,
    q: str,
    c: str,
    esc: str | None,
) -> list[str | None]:
    """Walk ``value`` char by char in per-part-quote mode.

    ``q`` is the opening delimiter and ``c`` the closing one. For
    symmetric quoting (the common case) ``q == c``; for asymmetric
    quoting (e.g. MSSQL ``[...]``) ``q != c``.
    """
    n = len(value)
    parts: list[str | None] = []
    i = 0

    while True:
        if value.startswith(q, i):
            # Quoted part: walk until matching closing delimiter,
            # honoring escape sequences (``esc + c`` -> literal ``c``).
            i += len(q)
            buf: list[str] = []
            closed = False
            while i < n:
                if esc is not None and value.startswith(esc + c, i):
                    buf.append(c)
                    i += len(esc) + len(c)
                    continue
                if value.startswith(c, i):
                    i += len(c)
                    closed = True
                    break
                buf.append(value[i])
                i += 1
            if not closed:
                raise ValueError(
                    f"unfqn: unterminated quoted identifier in FQN: {value!r}"
                )
            parts.append("".join(buf))
        else:
            # Bare part: read until next separator.
            j = i
            while j < n and not value.startswith(sep, j):
                if value.startswith(q, j):
                    raise ValueError(
                        f"unfqn: unexpected quote {q!r} at position {j} "
                        f"inside bare element: {value!r}"
                    )
                j += 1
            if j == i:
                raise ValueError(
                    f"unfqn: empty element at position {i} in FQN: {value!r}"
                )
            parts.append(value[i:j])
            i = j

        if i >= n:
            return parts
        if not value.startswith(sep, i):
            raise ValueError(
                f"unfqn: expected separator {sep!r} at position {i}: {value!r}"
            )
        i += len(sep)
        if i >= n:
            raise ValueError(f"unfqn: trailing separator in FQN: {value!r}")


def name_validate(
    fqn_def: FqnDef,
    max_parts: int,
    syntax: str = "SQL",
) -> _api.ValueConstraints:
    """Build the :class:`~tabsdatak._api.ValueConstraints` for an
    FQN-shaped string field.

    The rules run :func:`unfqn` against ``fqn_def`` to confirm the value
    parses as an FQN in the target syntax (``CONN_30``), then bound the
    number of parts by ``max_parts`` (``CONN_29``). Use directly in an
    :class:`Annotated` type alias::

        DatabricksTableNameSpec: TypeAlias = Annotated[
            str,
            _validation.name_validate(DBX_FQN_DEF, max_parts=3, syntax="Databricks"),
        ]

    Each call builds fresh rules; reuse the alias (or bind the result to a
    module-level constant) rather than rebuilding it on every field.

    Args:
        fqn_def: the FQN syntax (separator / quote / escape /
            ``outer_only``) the value must conform to.
        max_parts: maximum number of parts the value may contain
            (inclusive, must be a positive integer).
        syntax: the system name reported in the syntax-error message.

    Returns:
        A :class:`~tabsdatak._api.ValueConstraints` gating FQN syntax and
        part count, each rule carrying its own error code.

    Raises:
        ValueError: ``max_parts`` is non-positive.
    """
    if max_parts < 1:
        raise ValueError(
            f"name_validate: max_parts must be a positive integer: {max_parts!r}"
        )

    def _parts(value: str) -> list[str | None] | None:
        try:
            return unfqn(value, fqn_def)
        except ValueError:
            return None

    def _syntax_detail(value: str) -> dict[str, Any]:
        try:
            unfqn(value, fqn_def)
        except ValueError as e:
            # Strip `unfqn`'s own "unfqn: " prefix; the code supplies the
            # context, the cause supplies only the reason.
            return {
                "value": value,
                "syntax": syntax,
                "reason": str(e).removeprefix("unfqn: "),
            }
        return {"value": value, "syntax": syntax, "reason": ""}

    return _api.ValueConstraints(
        _api.Rule(
            ConnErrorCode.CONN_30,
            lambda value: _parts(value) is not None,
            _syntax_detail,
        ),
        _api.Rule(
            ConnErrorCode.CONN_29,
            lambda value: len(_parts(value) or ()) <= max_parts,
            lambda value: {
                "value": value,
                "parts": len(_parts(value) or ()),
                "max_parts": max_parts,
            },
        ),
    )


# --- FQN splitting helpers -------------------------------------------


def split_fqn(value: str, *, quote: str | None = None) -> list[str]:
    if quote is None:
        return value.split(".")
    parts: list[str] = []
    buf: list[str] = []
    in_quote = False
    i = 0
    n = len(value)
    while i < n:
        ch = value[i]
        if in_quote:
            buf.append(ch)
            if ch == quote:
                if i + 1 < n and value[i + 1] == quote:
                    buf.append(value[i + 1])
                    i += 2
                    continue
                in_quote = False
            i += 1
            continue
        if ch == quote:
            in_quote = True
            buf.append(ch)
            i += 1
            continue
        if ch == ".":
            parts.append("".join(buf))
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    if in_quote:
        raise ValueError(f"unterminated quoted identifier in FQN: {value!r}")
    parts.append("".join(buf))
    return parts


def split_fqn_brackets(value: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    in_brk = False
    i = 0
    n = len(value)
    while i < n:
        ch = value[i]
        if in_brk:
            buf.append(ch)
            if ch == "]":
                if i + 1 < n and value[i + 1] == "]":
                    buf.append(value[i + 1])
                    i += 2
                    continue
                in_brk = False
            i += 1
            continue
        if ch == "[":
            in_brk = True
            buf.append(ch)
            i += 1
            continue
        if ch == ".":
            parts.append("".join(buf))
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    if in_brk:
        raise ValueError(f"unterminated bracketed identifier in FQN: {value!r}")
    parts.append("".join(buf))
    return parts


# --- conn_cfg --------------------------------------------------------

_CONN_CFG_BASIC_TYPES = (str, int, float, bool, type(None))


def check_basic(value: Any, *, path: str) -> None:
    if isinstance(value, _CONN_CFG_BASIC_TYPES):
        return
    if isinstance(value, list):
        for i, item in enumerate(value):
            check_basic(item, path=f"{path}[{i}]")
        return
    if isinstance(value, dict):
        for k, v in value.items():
            if not isinstance(k, str):
                raise ConstraintError(
                    ConnErrorCode.CONN_31,
                    path=repr(path) if path else "<root>",
                    type=type(k).__name__,
                )
            check_basic(v, path=f"{path}.{k}" if path else k)
        return
    raise ConstraintError(
        ConnErrorCode.CONN_32,
        path=repr(path) if path else "<root>",
        type=type(value).__name__,
    )


def validate_conn_cfg(value: dict | None) -> dict | None:
    if value is None:
        return value
    check_basic(value, path="")
    return value


CONN_CFG_VALIDATION = AfterValidator(validate_conn_cfg)


# --- entity_whitelist ------------------------------------------------
#
# Per-source list of regex strings that the runtime applies to the
# external system's metadata names (tables / collections / objects /
# datasets / ...) to narrow the set of entities considered by the
# source. Each list entry must be a Python-compatible regex; the
# validator ``re.compile``s every entry at descriptor construction so
# pattern errors surface up-front rather than at read time.


def validate_entity_whitelist(value: list[str] | None) -> list[str] | None:
    if value is None:
        return value
    for i, pattern in enumerate(value):
        try:
            re.compile(pattern)
        except re.error as e:
            # A per-entry rule on a list field: ``ValueConstraints`` gates
            # text, so the coded error is raised directly here.
            raise ConstraintError(
                ConnErrorCode.CONN_28, index=i, value=pattern, reason=str(e)
            ) from e
    return value


ENTITY_WHITELIST_VALIDATION = AfterValidator(validate_entity_whitelist)


# --- Shared field-value rules ----------------------------------------
#
# The rules behind the cross-connector field specs in ``types.py``. Each
# pattern sits with the ``ValueConstraints`` that binds it to an error
# code; ``types.py`` annotates the matching spec with the constraints, so
# one rule gates a plain string and a ``Secret``'s hydrated value alike.


# The cloud connectors' `base_path` denotes a "folder" (object-key
# prefix) from the container / bucket root. It is spelled absolute-looking
# for a consistent UX -- starts with `/`, never ends with one (other than
# the bare root `/`), no empty segments (no `//`) -- but the leading `/`
# is trimmed when composing the actual object key (`cloud_full_path` /
# `normalize_dir_path` do the trimming). Backslash and NUL are forbidden.
# pydantic_core's regex engine has no lookaround, so the rule is spelled
# out structurally: a leading `/` followed by zero or more `/`-separated
# segments, each a run of one or more chars that are not `/`, `\` or NUL;
# the bare `/` root is allowed. Defaults to `/`.
_CLOUD_RELATIVE_PATH_RE_STR = r"^/(?:[^/\\\x00]+(?:/[^/\\\x00]+)*)?$"

CLOUD_RELATIVE_PATH_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(_CLOUD_RELATIVE_PATH_RE_STR, ConnErrorCode.CONN_17)
)

# The local-filesystem connectors' `base_path` is a real absolute path on
# the host, so it accepts two shapes: a POSIX absolute path (`/a/b`) or a
# Windows drive-absolute path (`C:/a/b` or `C:\a\b`, either separator).
# UNC paths (`\\host\share`) and relative paths are rejected. No empty
# segments and no NUL in either shape. The Windows branch mirrors the
# POSIX one: a drive letter, a separator, then zero or more `/`- or `\`-
# separated non-empty segments.
_LOCAL_ABSOLUTE_PATH_RE_STR = (
    r"^(?:/(?:[^/\\\x00]+(?:/[^/\\\x00]+)*)?"
    r"|[A-Za-z]:[/\\](?:[^/\\\x00]+(?:[/\\][^/\\\x00]+)*)?)$"
)

LOCAL_ABSOLUTE_PATH_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(_LOCAL_ABSOLUTE_PATH_RE_STR, ConnErrorCode.CONN_18)
)

# S3 bucket: 3-63 chars, lowercase letters / digits / hyphens, start
# and end alphanumeric. (Doesn't capture the imperative reserved-prefix
# / reserved-suffix / IPv4 checks the cloud provider enforces server-
# side anyway.)
_S3_BUCKET_RE_STR = r"^(?:[a-z0-9][a-z0-9-]{1,61}[a-z0-9])$"

S3_BUCKET_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(_S3_BUCKET_RE_STR, ConnErrorCode.CONN_19)
)

# AWS region: `us-east-1` / `us-gov-west-1` style.
_AWS_REGION_RE_STR = r"^(?:[a-z]{2}(?:-[a-z]+)+-\d+)$"

AWS_REGION_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(_AWS_REGION_RE_STR, ConnErrorCode.CONN_20)
)

# GCS bucket: same length / charset rules as S3 plus underscore.
# Doesn't capture the imperative `goog` prefix / Google-misspelling
# reserved-substring checks.
_GCS_BUCKET_RE_STR = r"^(?:[a-z0-9][a-z0-9_-]{1,61}[a-z0-9])$"

GCS_BUCKET_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(_GCS_BUCKET_RE_STR, ConnErrorCode.CONN_21)
)

# Azure container: lowercase alphanumeric / single hyphens (no
# leading, trailing, or consecutive hyphens), 3-63 chars. Encoded
# without lookahead (pydantic_core's regex engine doesn't support it):
# every char after the first is either alphanumeric, or a single hyphen
# *followed by* an alphanumeric -- so consecutive / trailing hyphens
# never match.
_AZ_CONTAINER_RE_STR = r"^(?:[a-z0-9](?:-?[a-z0-9]){2,62})$"

AZ_CONTAINER_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(_AZ_CONTAINER_RE_STR, ConnErrorCode.CONN_22)
)

# A DNS hostname or IPv4 literal: dot-separated labels of alphanumerics with
# internal (not leading / trailing) hyphens. Covers `localhost`,
# `db.example.com`, `10.0.0.1`. Read the value with `_convert(x, str)`.
_HOST_RE_STR = (
    r"^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?"
    r"(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)*$"
)

HOST_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(_HOST_RE_STR, ConnErrorCode.CONN_23)
)

# A TCP port, 1-65535. The value carries no type on the wire (it is a `str`
# or a secret), so the rule is the parse itself -- the same coercion
# `_convert(x, int)` performs at read time, bounded to the port range.
# `canonical` keeps the coercions pydantic would otherwise allow ("0080",
# " 80", "+80", "8.0") out.

PORT_VALIDATION = _api.ValueConstraints(
    _api.Rule.parses_as(int, ConnErrorCode.CONN_24, ge=1, le=65535, canonical=True)
)

# A boolean flag as text (the value is a `str` or a secret on the wire).
# The rule is the parse itself, so the accepted tokens are exactly the ones
# `_convert(x, bool)` understands at read time rather than a regex
# restating them.

BOOL_VALIDATION = _api.ValueConstraints(
    _api.Rule.parses_as(bool, ConnErrorCode.CONN_25)
)
