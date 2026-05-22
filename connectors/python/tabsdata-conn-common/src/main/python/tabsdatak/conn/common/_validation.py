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

# --- FQN syntax + helpers --------------------------------------------


def _validate_nonempty_separator(value: str) -> str:
    if not value:
        raise ValueError("FqnDef.separator must be non-empty")
    return value


def _validate_nonempty_quote(value: str) -> str:
    if not value:
        raise ValueError("FqnDef.quote, when set, must be non-empty")
    return value


# Field specs. Per-field non-emptiness rules are enforced via
# ``Annotated[..., AfterValidator(...)]`` so the regular pydantic
# validation pipeline carries them, instead of a hand-written
# ``__post_init__``. Cross-field rules (``escape``/``outer_only`` each
# requiring ``quote``) live in ``FqnDef._check_dependencies`` because
# pydantic field annotations only see one field at a time.
SeparatorSpec: TypeAlias = Annotated[str, AfterValidator(_validate_nonempty_separator)]
QuoteSpec: TypeAlias = Annotated[str, AfterValidator(_validate_nonempty_quote)]


@_api.dataclass(ConnErrorCode.CONN_9, kw_only=True)
class FqnDef:
    """Describes the syntax of a fully-qualified-name (FQN) for a
    particular system.

    Used by :func:`fqn` to assemble an FQN string from its parts and by
    :func:`unfqn` to split one back. Examples of the shapes this
    dataclass captures:

    - **Per-part quoting** (Databricks, ANSI SQL):
      ``FqnDef(separator='.', quote='`')`` -> ```a`.`b`.`c```
    - **Outer-only quoting** (BigQuery):
      ``FqnDef(separator='.', quote='`', outer_only=True)``
      -> ```a.b.c```
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


def fqn_complete(
    parts: list[str | None],
    total_parts: int,
    *defaults: str,
) -> list[str]:
    """Fill in the leading ``None`` slots of ``parts`` with the
    positional ``defaults`` values, returning a fully-populated list
    of length ``total_parts`` suitable for feeding to :func:`fqn`.

    ``parts`` is first normalized to length ``total_parts`` by
    left-padding with ``None``; then the leading run of ``None``
    entries is replaced with the ``defaults`` values in order. The
    typical use case is completing a partially-qualified name from
    :func:`unfqn` against connection defaults::

        # User wrote ``schema.users``; conn supplies the catalog.
        parts = unfqn("schema.users", spec, total_parts=3)
        full = fqn_complete(parts, 3, "main")          # ["main", "schema", "users"]
        fqn(full, spec)                                 # `main`.`schema`.`users`

    Args:
        parts: list whose leading entries may be ``None``. Length must
            not exceed ``total_parts``; ``None`` entries are only
            permitted in the leading run.
        total_parts: target length of the returned list.
        *defaults: replacement values, in order, for the leading
            ``None`` slots. The number of ``defaults`` must equal the
            number of leading ``None`` entries after normalization.

    Raises:
        ValueError: ``total_parts`` is non-positive; ``parts`` has
            more entries than ``total_parts``; ``parts`` has a
            ``None`` entry that is not in the leading run; the count
            of ``defaults`` does not match the leading-``None`` count;
            or any default is empty.
    """
    if total_parts < 1:
        raise ValueError(
            f"fqn_complete: total_parts must be a positive integer: {total_parts!r}"
        )
    if len(parts) > total_parts:
        raise ValueError(
            f"fqn_complete: parts has {len(parts)} entries, exceeds "
            f"total_parts={total_parts}"
        )

    padded: list[str | None] = [None] * (total_parts - len(parts)) + list(parts)

    leading_none_count = 0
    for v in padded:
        if v is None:
            leading_none_count += 1
        else:
            break

    for i in range(leading_none_count, total_parts):
        if padded[i] is None:
            raise ValueError(
                f"fqn_complete: parts has None at position {i} after the "
                f"leading run; only leading Nones are allowed: {parts!r}"
            )

    if len(defaults) != leading_none_count:
        raise ValueError(
            f"fqn_complete: expected {leading_none_count} default value(s) "
            f"to fill the leading None slots, got {len(defaults)}"
        )

    for i, d in enumerate(defaults):
        if not d:
            raise ValueError(f"fqn_complete: default at index {i} is empty")

    # After validation: leading slots filled from defaults, tail copied
    # from padded (all known to be non-None strings by the check above).
    tail = [str(v) for v in padded[leading_none_count:]]
    return list(defaults) + tail


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


def name_validate(fqn_def: FqnDef, max_parts: int) -> AfterValidator:
    """Build a pydantic :class:`AfterValidator` for FQN-shaped string
    fields.

    The returned validator runs :func:`unfqn` against ``fqn_def`` to
    confirm the value parses as a valid FQN in the target syntax, then
    bounds the number of parts by ``max_parts``. Use it directly in an
    :class:`Annotated` type alias::

        DatabricksTableNameSpec: TypeAlias = Annotated[
            str,
            _validation.name_validate(DBX_FQN_DEF, max_parts=3),
        ]

    Each call returns a fresh ``AfterValidator``; reuse the alias (or
    bind the validator to a module-level constant) rather than rebuilding
    it on every field.

    Args:
        fqn_def: the FQN syntax (separator / quote / escape /
            ``outer_only``) the value must conform to.
        max_parts: maximum number of parts the value may contain
            (inclusive, must be a positive integer).

    Returns:
        An :class:`AfterValidator` wrapping a closure that raises
        ``ValueError`` either on a syntax error (via :func:`unfqn`) or
        when the value exceeds ``max_parts`` parts.

    Raises:
        ValueError: ``max_parts`` is non-positive.
    """
    if max_parts < 1:
        raise ValueError(
            f"name_validate: max_parts must be a positive integer: {max_parts!r}"
        )

    def _validate(value: str) -> str:
        parts = unfqn(value, fqn_def)
        if len(parts) > max_parts:
            raise ValueError(
                f"name has {len(parts)} parts, exceeds max_parts={max_parts}: {value!r}"
            )
        return value

    return AfterValidator(_validate)


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
                raise ValueError(
                    f"conn_cfg has non-str key of type {type(k).__name__} at {path!r}"
                )
            check_basic(v, path=f"{path}.{k}" if path else k)
        return
    raise ValueError(
        f"conn_cfg has non-basic value of type "
        f"{type(value).__name__} at {path or '<root>'!r}"
    )


def validate_conn_cfg(value: dict | None) -> dict | None:
    if value is None:
        return value
    check_basic(value, path="")
    return value


CONN_CFG_VALIDATOR = AfterValidator(validate_conn_cfg)


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
            raise ValueError(
                f"entity_whitelist contains invalid regex at index {i}: "
                f"{pattern!r}: {e}"
            ) from e
    return value


ENTITY_WHITELIST_VALIDATOR = AfterValidator(validate_entity_whitelist)
