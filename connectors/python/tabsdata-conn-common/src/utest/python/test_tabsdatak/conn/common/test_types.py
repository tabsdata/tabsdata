from __future__ import annotations

import pytest

from tabsdatak.api import Env, Secret
from tabsdatak.conn.common.error import ConnErrorCode, ConnInitException
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AzureAccountKey,
    Credentials,
    GCPJson,
    UserPassword,
)
from tabsdatak.conn.common.types import full_path


# ---------------------------------------------------------------------------
# Credentials -- round trips
# ---------------------------------------------------------------------------


def test_user_password_round_trip_with_inline_strings():
    c = UserPassword(user=Secret.lit("etl"), password=Secret.lit("pw"))
    back = UserPassword.from_json(c.to_json())
    assert back == c


def test_user_password_round_trip_with_env_refs():
    c = UserPassword(user=Env("DB_USER"), password=Secret("db-creds#PW"))
    back = UserPassword.from_json(c.to_json())
    assert isinstance(back.user, Env)
    assert isinstance(back.password, Secret)


def test_aws_access_secret_key_round_trips():
    c = AwsAccessSecretKey(
        access_key_id=Env("AWS_ACCESS_KEY_ID"),
        secret_access_key=Secret("aws-creds#SECRET"),
    )
    back = AwsAccessSecretKey.from_json(c.to_json())
    assert back == c


def test_gcp_json_round_trips():
    c = GCPJson(json=Secret("gcp-sa#KEY"))
    back = GCPJson.from_json(c.to_json())
    assert back == c


def test_azure_account_key_round_trips():
    c = AzureAccountKey(
        account_name=Secret.lit("myacct"),
        account_key=Secret("azure-acct#KEY"),
    )
    back = AzureAccountKey.from_json(c.to_json())
    assert back == c


def test_polymorphic_load_via_credentials_base():
    c = UserPassword(user=Secret.lit("u"), password=Secret.lit("p"))
    back = Credentials.from_json(c.to_json())
    assert isinstance(back, UserPassword)
    # Credential fields are typed `SecretOrEnvSpec`; bare strings coerce
    # to a `LiteralSecret`. The literal payload is reachable via `value()`.
    assert back.user.value() == "u"


# ---------------------------------------------------------------------------
# Credentials -- init failure routing (each ApiError tag wraps a
# pydantic ValidationError as a ConnInitException)
# ---------------------------------------------------------------------------


def test_user_password_missing_field_raises_conn_1():
    with pytest.raises(ConnInitException) as ei:
        UserPassword(user=Secret.lit("etl"))  # type: ignore[call-arg]
    assert ei.value.api_error is ConnErrorCode.CONN_1


def test_gcp_json_missing_field_raises_conn_2():
    with pytest.raises(ConnInitException) as ei:
        GCPJson()  # type: ignore[call-arg]
    assert ei.value.api_error is ConnErrorCode.CONN_2


def test_aws_access_secret_key_missing_field_raises_conn_3():
    with pytest.raises(ConnInitException) as ei:
        AwsAccessSecretKey(access_key_id=Env("AWS_ACCESS_KEY_ID"))  # type: ignore[call-arg]
    assert ei.value.api_error is ConnErrorCode.CONN_3


def test_azure_account_key_missing_field_raises_conn_4():
    with pytest.raises(ConnInitException) as ei:
        AzureAccountKey(account_name=Secret.lit("a"))  # type: ignore[call-arg]
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


def testfull_path_resolves_env_base_path(monkeypatch):
    monkeypatch.setenv("BP", "/data/prod/")
    assert full_path(Env(s="BP"), "foo.csv") == "data/prod/foo.csv"


def testfull_path_resolves_secret_base_path():
    assert full_path(Secret.lit("/data/secret/"), "/foo.csv") == "data/secret/foo.csv"


def testfull_path_result_never_starts_or_ends_with_slash():
    for bp in ["", "/", "/a/", "a/", "/a", "a", "/a/b/", "a/b"]:
        for p in ["", "/", "x", "/x", "x/", "/x/", "/x/y/"]:
            out = full_path(bp, p)
            assert not out.startswith("/"), (bp, p, out)
            assert not out.endswith("/"), (bp, p, out)


