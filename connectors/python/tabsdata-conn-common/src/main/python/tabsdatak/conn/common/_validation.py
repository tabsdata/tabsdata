from __future__ import annotations

import re
from typing import Any

from pydantic import AfterValidator

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
