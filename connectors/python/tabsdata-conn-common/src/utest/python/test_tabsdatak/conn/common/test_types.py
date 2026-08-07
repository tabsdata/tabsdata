#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.error import ConnErrorCode, ConnInitException
from tabsdatak.error import ConstraintError
from pydantic import TypeAdapter, ValidationError

from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AwsRegionSpec,
    AzContainerSpec,
    AzureAccountKey,
    BoolSpec,
    CloudRelativePathSpec,
    Credentials,
    GCPJson,
    GCSBucketSpec,
    HostSpec,
    LocalAbsolutePathSpec,
    PortSpec,
    S3BucketSpec,
    UserPassword,
)
from tabsdatak.conn.common.types import cloud_full_path, normalize_dir_path

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert


# ---------------------------------------------------------------------------
# Credentials -- round trips
# ---------------------------------------------------------------------------


def test_user_password_round_trip_drops_inline_values():
    # Serializing always dehydrates, so inline values do not survive a
    # serialize/deserialize round-trip -- the secrets come back dehydrated.
    c = UserPassword(user=Secret("etl"), password=Secret("pw"))
    back = UserPassword.from_json(c.to_json())
    assert back == UserPassword(user=Secret(), password=Secret())


def test_user_password_round_trip_with_unhydrated_refs():
    c = UserPassword(user=Secret(), password=Secret())
    back = UserPassword.from_json(c.to_json())
    assert isinstance(back.user, Secret)
    assert isinstance(back.password, Secret)


def test_user_password_credentials_reject_non_secret():
    # Credentials are Secret-only; a non-string, non-Secret value is rejected.
    with pytest.raises(ConnInitException):
        UserPassword(user=123, password=Secret())  # type: ignore[arg-type]


def test_aws_access_secret_key_round_trips():
    c = AwsAccessSecretKey(
        access_key_id=Secret(),
        secret_access_key=Secret(),
    )
    back = AwsAccessSecretKey.from_json(c.to_json())
    assert back == c


def test_gcp_json_round_trips():
    c = GCPJson(json=Secret())
    back = GCPJson.from_json(c.to_json())
    assert back == c


def test_azure_account_key_round_trips():
    c = AzureAccountKey(
        account_name=Secret("myacct"),
        account_key=Secret(),
    )
    back = AzureAccountKey.from_json(c.to_json())
    # Values never serialize, so both fields come back dehydrated.
    assert back == AzureAccountKey(account_name=Secret(), account_key=Secret())


def test_polymorphic_load_via_credentials_base():
    c = UserPassword(user=Secret("u"), password=Secret("p"))
    back = Credentials.from_json(c.to_json())
    assert isinstance(back, UserPassword)
    # Credential fields are typed `Secret`; values never serialize, so the
    # round-tripped secrets come back dehydrated.
    assert isinstance(back.user, Secret)
    assert back.user == Secret()


# ---------------------------------------------------------------------------
# Credentials -- init failure routing (each ApiError tag wraps a
# pydantic ValidationError as a ConnInitException)
# ---------------------------------------------------------------------------


def test_user_password_missing_field_raises_conn_1():
    with pytest.raises(ConnInitException) as ei:
        UserPassword(user=Secret("etl"))  # type: ignore[call-arg]
    assert ei.value.api_error is ConnErrorCode.CONN_1


def test_gcp_json_missing_field_raises_conn_2():
    with pytest.raises(ConnInitException) as ei:
        GCPJson()  # type: ignore[call-arg]
    assert ei.value.api_error is ConnErrorCode.CONN_2


def test_aws_access_secret_key_missing_field_raises_conn_3():
    with pytest.raises(ConnInitException) as ei:
        AwsAccessSecretKey(access_key_id=Secret())  # type: ignore[call-arg]
    assert ei.value.api_error is ConnErrorCode.CONN_3


def test_azure_account_key_missing_field_raises_conn_4():
    with pytest.raises(ConnInitException) as ei:
        AzureAccountKey(account_name=Secret("a"))  # type: ignore[call-arg]
    assert ei.value.api_error is ConnErrorCode.CONN_4


# ---------------------------------------------------------------------------
# full_path helper
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("base_path", "path", "expected"),
    [
        # base_path = "" (root)
        ("", "foo.csv", "foo.csv"),
        ("", "/foo.csv", "foo.csv"),
        ("", "/foo/bar.csv", "foo/bar.csv"),
        ("", "foo/bar.csv/", "foo/bar.csv"),
        ("", "", ""),
        ("", "/", ""),
        # base_path = "/"
        ("/", "foo.csv", "foo.csv"),
        ("/", "/foo.csv", "foo.csv"),
        # base_path with leading/trailing slash variants
        ("/data/", "foo.csv", "data/foo.csv"),
        ("data/", "foo.csv", "data/foo.csv"),
        ("/data", "foo.csv", "data/foo.csv"),
        ("data", "foo.csv", "data/foo.csv"),
        # absolute-looking path is treated as relative
        ("/data/", "/foo.csv", "data/foo.csv"),
        ("data", "/sub/foo.csv", "data/sub/foo.csv"),
        # multi-segment paths
        ("/a/b/", "c/d/", "a/b/c/d"),
        ("a/b", "/c/d/", "a/b/c/d"),
        # empty path leaves base_path as-is (stripped)
        ("/data/", "", "data"),
        ("/data/", "/", "data"),
    ],
)
def testfull_path_normalizes_str_base_path(base_path, path, expected):
    assert cloud_full_path(base_path, path) == expected




def testfull_path_resolves_secret_base_path():
    assert cloud_full_path(Secret("/data/secret/"), "/foo.csv") == "data/secret/foo.csv"


def testfull_path_result_never_starts_or_ends_with_slash():
    for bp in ["", "/", "/a/", "a/", "/a", "a", "/a/b/", "a/b"]:
        for p in ["", "/", "x", "/x", "x/", "/x/", "/x/y/"]:
            out = cloud_full_path(bp, p)
            assert not out.startswith("/"), (bp, p, out)
            assert not out.endswith("/"), (bp, p, out)


# ---------------------------------------------------------------------------
# CloudRelativePathSpec validation
# ---------------------------------------------------------------------------

_full_path = TypeAdapter(CloudRelativePathSpec)


def _accepts_json(value) -> bool:
    # Wire form: markers are decoded (``str:`` -> plain, ``secret:`` -> Secret).
    try:
        _full_path.validate_json(json.dumps(value))
        return True
    except Exception:
        return False


def _accepts_python(value) -> bool:
    # Python construction: the value is literal (no marker interpretation).
    try:
        _full_path.validate_python(value)
        return True
    except Exception:
        return False


@pytest.mark.parametrize("path", ["/", "/a", "/a/b", "/data/sub"])
def test_full_path_wire_accepts_absolute(path):
    # On the wire a plain value may be ``str:``-marked or bare, a secret
    # ``secret:``-marked; the full-path regex gates the plain value and the
    # secret's hydrated value alike.
    assert _accepts_json(f"str:{path}")
    assert _accepts_json(path)
    assert _accepts_json(f"secret:{path}")


def test_full_path_wire_accepts_dehydrated_secret():
    # A dehydrated secret carries no value, so it is left unchecked.
    assert _accepts_json(":secret:")


@pytest.mark.parametrize(
    "path",
    ["", "a", "a/b", "staging", "/a/", "//", "/a//b", "/a/b/", "/a\\b", "/a\x00"],
)
def test_full_path_wire_rejects_bad_shape(path):
    # The regex rejects these whether the value is plain (str:/bare) or secret.
    assert not _accepts_json(f"str:{path}")
    assert not _accepts_json(path)
    assert not _accepts_json(f"secret:{path}")


@pytest.mark.parametrize("path", ["/", "/a", "/a/b", "/data/sub"])
def test_full_path_python_takes_literal_value(path):
    # In Python the value is literal: a valid absolute path passes the regex,
    # while a ``str:``-prefixed value is taken verbatim and fails it.
    assert _accepts_python(path)
    assert not _accepts_python(f"str:{path}")


# ---------------------------------------------------------------------------
# normalize_dir_path helper
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "lead", "back", "expected"),
    [
        ("staging", False, True, "staging/"),
        ("/staging/", False, True, "staging/"),
        ("staging", True, False, "/staging"),
        ("staging", True, True, "/staging/"),
        ("staging", False, False, "staging"),
        ("a/b", False, True, "a/b/"),
        ("", False, True, ""),
        ("", True, True, ""),
        ("/", True, True, ""),
    ],
)
def test_normalize_dir_path(path, lead, back, expected):
    assert normalize_dir_path(path, lead, back) == expected


# ---------------------------------------------------------------------------
# _convert helper
# ---------------------------------------------------------------------------


def test_convert_passes_through_str():
    assert _convert("hello", str) == "hello"


def test_convert_passes_through_int():
    assert _convert(42, int) == 42


def test_convert_passes_through_float():
    assert _convert(3.14, float) == 3.14


def test_convert_passes_through_bool():
    assert _convert(True, bool) is True
    assert _convert(False, bool) is False


def test_convert_passes_through_bytes():
    assert _convert(b"abc", bytes) == b"abc"


def test_convert_passes_through_datetime():
    dt = datetime(2026, 5, 21, 12, 0, tzinfo=timezone.utc)
    assert _convert(dt, datetime) is dt


def test_convert_resolves_secret_literal_to_str():
    assert _convert(Secret("hello"), str) == "hello"




def test_convert_coerces_secret_to_int():
    assert _convert(Secret("42"), int) == 42




def test_convert_coerces_secret_to_float():
    assert _convert(Secret("3.14"), float) == 3.14


def test_convert_coerces_secret_to_bool_true():
    assert _convert(Secret("true"), bool) is True


def test_convert_coerces_secret_to_bool_false():
    assert _convert(Secret("false"), bool) is False


def test_convert_coerces_secret_to_bytes():
    assert _convert(Secret("abc"), bytes) == b"abc"


def test_convert_coerces_secret_to_datetime():
    out = _convert(Secret("2026-05-21T12:00:00+00:00"), datetime)
    assert out == datetime(2026, 5, 21, 12, 0, tzinfo=timezone.utc)


def test_convert_raises_on_uncoercible_secret_to_int():
    with pytest.raises(Exception):
        _convert(Secret("not-a-number"), int)

# ---------------------------------------------------------------------------
# Shared field-spec rules
#
# Every spec in `types.py` is `Annotated[StrOrSecretSpec,
# _validation.<X>_VALIDATION]`, so one rule gates both arms of the union.
# These tests pin the two properties that follow from that and would
# otherwise go unchecked:
#
#   * **arm symmetry** -- a plain string and a `Secret` carrying the same
#     text get the same verdict *and* the same error code, and a dehydrated
#     `Secret` (no text) is skipped;
#   * **code identity** -- a rejection reports the spec's own code, so
#     CONN-19 means "bad S3 bucket" rather than "something failed".
#
# The specs are exercised through their aliases (not the `*_VALIDATION`
# objects) so the union decode runs too: on the wire a plain value is
# `str:<v>` or bare and a secret is `secret:<v>`, while in Python the value
# is literal.
# ---------------------------------------------------------------------------


# (spec, its error code, accepted values, rejected values)
_SPEC_RULES = [
    pytest.param(
        S3BucketSpec,
        ConnErrorCode.CONN_19,
        ["ok-bucket", "abc", "a-b-c", "a" * 63],
        ["", "AB", "BAD!", "ab", "-abc", "abc-", "a" * 64, "a_b"],
        id="S3BucketSpec",
    ),
    pytest.param(
        AwsRegionSpec,
        ConnErrorCode.CONN_20,
        ["us-east-1", "us-gov-west-1", "eu-central-1"],
        ["", "US-EAST-1", "us-east", "useast1", "us-east-"],
        id="AwsRegionSpec",
    ),
    pytest.param(
        GCSBucketSpec,
        ConnErrorCode.CONN_21,
        ["ok-bucket", "ok_bucket", "a-b_c"],
        ["", "AB", "BAD!", "ab", "-abc", "abc-"],
        id="GCSBucketSpec",
    ),
    pytest.param(
        AzContainerSpec,
        ConnErrorCode.CONN_22,
        ["abc", "a-b-c", "a" * 63],
        # single hyphens only: no leading, trailing or consecutive ones.
        ["", "ab", "-abc", "abc-", "a--b", "ABC", "a_b"],
        id="AzContainerSpec",
    ),
    pytest.param(
        HostSpec,
        ConnErrorCode.CONN_23,
        ["localhost", "db.example.com", "10.0.0.1", "a-b.c"],
        ["", "bad_host", "-host", "host-", "a..b"],
        id="HostSpec",
    ),
    pytest.param(
        PortSpec,
        ConnErrorCode.CONN_24,
        ["1", "80", "9030", "65535"],
        # out of range, and the coercions `parses_as(canonical=True)` rejects.
        ["", "0", "65536", "0080", " 80", "+80", "8.0", "abc", "-1"],
        id="PortSpec",
    ),
    pytest.param(
        BoolSpec,
        ConnErrorCode.CONN_25,
        ["true", "False", "t", "f", "yes", "NO", "y", "n", "on", "Off", "1", "0"],
        ["", "2", "maybe", "tru"],
        id="BoolSpec",
    ),
    pytest.param(
        LocalAbsolutePathSpec,
        ConnErrorCode.CONN_18,
        ["/", "/a", "/a/b", "C:/a", "C:\\a\\b"],
        # UNC and relative paths, empty segments, trailing separator, NUL.
        ["", "a", "a/b", "/a/", "//", "/a//b", "\\\\host\\share", "/a\x00"],
        id="LocalAbsolutePathSpec",
    ),
    pytest.param(
        CloudRelativePathSpec,
        ConnErrorCode.CONN_17,
        ["/", "/a", "/a/b", "/data/sub"],
        ["", "a", "a/b", "/a/", "//", "/a//b", "/a\\b", "/a\x00"],
        id="CloudRelativePathSpec",
    ),
]


def _rule_code(exc: Exception) -> object:
    """The `ConstraintError` code behind a pydantic `ValidationError`."""
    for err in exc.errors():
        original = err.get("ctx", {}).get("error")
        if isinstance(original, ConstraintError):
            return original.code
    return None


def _reject_code(adapter: TypeAdapter, value: object) -> object:
    """Validate `value`, requiring rejection, and return the rule's code."""
    with pytest.raises(ValidationError) as ei:
        adapter.validate_python(value)
    return _rule_code(ei.value)


@pytest.mark.parametrize(("spec", "code", "valid", "invalid"), _SPEC_RULES)
def test_spec_accepts_valid_on_both_arms(spec, code, valid, invalid):
    adapter = TypeAdapter(spec)
    for value in valid:
        # Python: the value is literal, plain or wrapped in a Secret.
        assert adapter.validate_python(value) == value
        assert adapter.validate_python(Secret(value)) == Secret(value)
        # Wire: bare, `str:`-marked and `secret:`-marked all decode and pass.
        assert adapter.validate_json(json.dumps(value)) == value
        assert adapter.validate_json(json.dumps(f"str:{value}")) == value
        assert adapter.validate_json(json.dumps(f"secret:{value}")) == Secret(value)


@pytest.mark.parametrize(("spec", "code", "valid", "invalid"), _SPEC_RULES)
def test_spec_rejects_invalid_with_its_own_code(spec, code, valid, invalid):
    adapter = TypeAdapter(spec)
    for value in invalid:
        assert _reject_code(adapter, value) is code, value


@pytest.mark.parametrize(("spec", "code", "valid", "invalid"), _SPEC_RULES)
def test_spec_arms_agree(spec, code, valid, invalid):
    # The point of one rule per spec: the plain and secret arms cannot
    # disagree, on the verdict or on the code.
    adapter = TypeAdapter(spec)
    for value in invalid:
        assert _reject_code(adapter, value) is _reject_code(adapter, Secret(value)), value


@pytest.mark.parametrize(("spec", "code", "valid", "invalid"), _SPEC_RULES)
def test_spec_skips_dehydrated_secret(spec, code, valid, invalid):
    # A dehydrated secret carries no text, so no rule can run on it -- the
    # value is checked again when the runtime rehydrates it.
    adapter = TypeAdapter(spec)
    assert adapter.validate_python(Secret()) == Secret()
    assert adapter.validate_json(json.dumps(":secret:")) == Secret()
