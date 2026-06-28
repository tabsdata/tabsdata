from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tabsdatak.api import Var, Secret
from tabsdatak.conn.common.error import ConnErrorCode, ConnInitException
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AzureAccountKey,
    Credentials,
    GCPJson,
    UserPassword,
)
from tabsdatak.conn.common.types import full_path

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert


# ---------------------------------------------------------------------------
# Credentials -- round trips
# ---------------------------------------------------------------------------


def test_user_password_round_trip_with_inline_strings():
    c = UserPassword(user=Secret("etl"), password=Secret("pw"))
    back = UserPassword.from_json(c.to_json())
    assert back == c


def test_user_password_round_trip_with_unhydrated_refs():
    c = UserPassword(user=Secret(), password=Secret())
    back = UserPassword.from_json(c.to_json())
    assert isinstance(back.user, Secret)
    assert isinstance(back.password, Secret)


def test_user_password_credentials_reject_var():
    # Credentials are Secret-only; a Var ref is rejected.
    with pytest.raises(ConnInitException):
        UserPassword(user=Var(), password=Secret())  # type: ignore[arg-type]


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
    assert back == c


def test_polymorphic_load_via_credentials_base():
    c = UserPassword(user=Secret("u"), password=Secret("p"))
    back = Credentials.from_json(c.to_json())
    assert isinstance(back, UserPassword)
    # Credential fields are typed `Secret`; bare strings coerce to a
    # `Secret` literal. The literal payload is reachable via `value`.
    assert back.user.value == "u"


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
    assert full_path(base_path, path) == expected




def testfull_path_resolves_secret_base_path():
    assert full_path(Secret("/data/secret/"), "/foo.csv") == "data/secret/foo.csv"


def testfull_path_result_never_starts_or_ends_with_slash():
    for bp in ["", "/", "/a/", "a/", "/a", "a", "/a/b/", "a/b"]:
        for p in ["", "/", "x", "/x", "x/", "/x/", "/x/y/"]:
            out = full_path(bp, p)
            assert not out.startswith("/"), (bp, p, out)
            assert not out.endswith("/"), (bp, p, out)


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


def test_convert_resolves_env_literal_to_str():
    assert _convert(Var("hello"), str) == "hello"




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


