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
            raise ValueError(
                "FqnDef.close_quote requires FqnDef.quote to be set"
            )
        return self


def fqn(parts: list[str], spec: FqnDef) -> str:
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
                    f"fqn: FQN contains quote {c!r} with no escape defined: "
                    f"{joined!r}"
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
        if (
            len(p) >= len(q) + len(c)
            and p.startswith(q)
            and p.endswith(c)
        ):
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
    if total_parts < 1:
        raise ValueError(
            f"fqn_complete: total_parts must be a positive integer: "
            f"{total_parts!r}"
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
                f"unfqn: outer_only FQN must be wrapped in {q!r}..{c!r}: "
                f"{value!r}"
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
                f"unfqn: expected separator {sep!r} at position {i}: "
                f"{value!r}"
            )
        i += len(sep)
        if i >= n:
            raise ValueError(f"unfqn: trailing separator in FQN: {value!r}")


def name_validate(fqn_def: FqnDef, max_parts: int) -> AfterValidator:
    if max_parts < 1:
        raise ValueError(
            f"name_validate: max_parts must be a positive integer: "
            f"{max_parts!r}"
        )

    def _validate(value: str) -> str:
        parts = unfqn(value, fqn_def)
        if len(parts) > max_parts:
            raise ValueError(
                f"name has {len(parts)} parts, exceeds max_parts="
                f"{max_parts}: {value!r}"
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


# # --- Cloud-location shape regexes ------------------------------------
#
# # base_path: matches "/" alone OR a non-empty string that doesn't start
# # with "/", contains no backslash, and ends with "/".
# _BASE_PATH_RE_STR = r"/|[^/\\][^\\]*/"
# # _BASE_PATH_RE = re.compile(r"/|[^/\\][^\\]*/")
# #
# # def _validate_base_path(value: str) -> str:
# #     if not _BASE_PATH_RE.fullmatch(value):
# #         raise ValueError(
# #             f"not a valid base path. If root use '/',"
# #             f" otherwise doesn't start with '/'."
# #             f" It must end with '/' and it cannot have any `\\`: {value!r}"
# #         )
# #     return value
# #
# # def BASE_PATH_CONSTRAINTS() -> AfterValidator:
# #
#
# # S3 bucket: 3-63 chars, lowercase letters / digits / hyphens, start
# # and end alphanumeric. (Doesn't capture the imperative reserved-prefix
# # / reserved-suffix / IPv4 checks the cloud provider enforces server-
# # side anyway.)
# S3_BUCKET_RE_STR = r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]"
# #S3_BUCKET_RE = re.compile(S3_BUCKET_RE_STR)
#
# # GCS bucket: same length / charset rules as S3 plus underscore.
# # Doesn't capture the imperative ``goog`` prefix / Google-misspelling
# # reserved-substring checks.
# GCS_BUCKET_RE_STR = r"[a-z0-9][a-z0-9_-]{1,61}[a-z0-9]"
# GCS_BUCKET_RE = re.compile(GCS_BUCKET_RE_STR)
#
# # Azure container: lowercase alphanumeric / single hyphens (no
# # leading, trailing, or consecutive hyphens), 3-63 chars. The lookahead
# # enforces "no double hyphen, no trailing hyphen".
# AZ_CONTAINER_RE_STR = r"[a-z0-9](?:[a-z0-9]|-(?=[a-z0-9])){1,61}[a-z0-9]"
# AZ_CONTAINER_RE = re.compile(AZ_CONTAINER_RE_STR)
#
# # AWS region: ``us-east-1`` / ``us-gov-west-1`` style.
# AWS_REGION_RE_STR = r"[a-z]{2}(?:-[a-z]+)+-\d+"
# AWS_REGION_RE = re.compile(AWS_REGION_RE_STR)
#
#
#
#
# def _validate_s3_bucket(value: str) -> str:
#     if not S3_BUCKET_RE.fullmatch(value):
#         raise ValueError(
#             f"not a valid S3 bucket name (3-63 chars, lowercase letters / "
#             f"digits / hyphens, must start and end with an alphanumeric): "
#             f"{value!r}"
#         )
#     return value
#
#
# def _validate_gcs_bucket(value: str) -> str:
#     if not GCS_BUCKET_RE.fullmatch(value):
#         raise ValueError(
#             f"not a valid GCS bucket name (3-63 chars, lowercase letters / "
#             f"digits / hyphens / underscores, must start and end with an "
#             f"alphanumeric): {value!r}"
#         )
#     return value
#
#
# def _validate_az_container(value: str) -> str:
#     if not AZ_CONTAINER_RE.fullmatch(value):
#         raise ValueError(
#             f"not a valid Azure container name (3-63 chars, lowercase "
#             f"alphanumeric with single hyphens, no leading / trailing / "
#             f"consecutive hyphens): {value!r}"
#         )
#     return value
#
#
# def _validate_aws_region(value: str) -> str:
#     if not AWS_REGION_RE.fullmatch(value):
#         raise ValueError(
#             f"not a valid AWS region (e.g. 'us-east-1' or 'us-gov-west-1'): "
#             f"{value!r}"
#         )
#     return value
#

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
