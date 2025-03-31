#
# Copyright 2024 Tabs Data Inc.
#

import pytest

from tabsdata import (
    AzureAccountKeyCredentials,
    HashiCorpSecret,
    S3AccessKeyCredentials,
    UserPasswordCredentials,
)
from tabsdata.credentials import build_credentials
from tabsdata.exceptions import CredentialsConfigurationError, ErrorCode
from tabsdata.secret import DirectSecret


def test_account_key_credentials_initialization():
    credentials = AzureAccountKeyCredentials(
        account_name="test_user",
        account_key="test_password",
    )
    assert credentials.account_name == DirectSecret("test_user")
    assert credentials.account_key == DirectSecret("test_password")
    assert credentials.to_dict() == {
        AzureAccountKeyCredentials.IDENTIFIER: {
            AzureAccountKeyCredentials.ACCOUNT_NAME_KEY: (
                DirectSecret("test_user").to_dict()
            ),
            AzureAccountKeyCredentials.ACCOUNT_KEY_KEY: (
                DirectSecret("test_password").to_dict()
            ),
        }
    }
    assert build_credentials(credentials) == credentials
    assert build_credentials(credentials.to_dict()) == credentials
    assert credentials.__repr__()


def test_account_key_credentials_update():
    credentials = AzureAccountKeyCredentials(
        account_name="test_user",
        account_key="test_password",
    )
    assert credentials.account_name == DirectSecret("test_user")
    assert credentials.account_key == DirectSecret("test_password")
    credentials.account_name = "new_test_user"
    credentials.account_key = "new_test_password"
    assert credentials.account_name == DirectSecret("new_test_user")
    assert credentials.account_key == DirectSecret("new_test_password")
    credentials.account_name = HashiCorpSecret("secret_path", "new_test_user")
    credentials.account_key = HashiCorpSecret("secret_path", "new_test_password")
    assert credentials.account_name == HashiCorpSecret("secret_path", "new_test_user")
    assert credentials.account_key == HashiCorpSecret(
        "secret_path", "new_test_password"
    )


def test_account_key_credentials_from_dictionary():
    credentials = {
        AzureAccountKeyCredentials.IDENTIFIER: {
            AzureAccountKeyCredentials.ACCOUNT_NAME_KEY: "test_user",
            AzureAccountKeyCredentials.ACCOUNT_KEY_KEY: "test_password",
        }
    }
    built_credentials = build_credentials(credentials)
    assert isinstance(built_credentials, AzureAccountKeyCredentials)
    assert built_credentials.account_name == DirectSecret("test_user")
    assert built_credentials.account_key == DirectSecret("test_password")


def test_account_key_credentials_object_and_dict_not_equal():
    credentials = AzureAccountKeyCredentials(
        account_name="test_user",
        account_key="test_password",
    )
    credentials_dict = {
        AzureAccountKeyCredentials.IDENTIFIER: {
            AzureAccountKeyCredentials.ACCOUNT_NAME_KEY: "test_user",
            AzureAccountKeyCredentials.ACCOUNT_KEY_KEY: "test_password",
        }
    }
    assert credentials != credentials_dict


def test_s3_access_key_credentials_initialization():
    credentials = S3AccessKeyCredentials(
        aws_access_key_id="test_access_key_id",
        aws_secret_access_key="test_secret_access_key",
    )
    assert credentials.aws_access_key_id == DirectSecret("test_access_key_id")
    assert credentials.aws_secret_access_key == DirectSecret("test_secret_access_key")
    assert credentials.to_dict() == {
        S3AccessKeyCredentials.IDENTIFIER: {
            S3AccessKeyCredentials.AWS_ACCESS_KEY_ID_KEY: (
                DirectSecret("test_access_key_id").to_dict()
            ),
            S3AccessKeyCredentials.AWS_SECRET_ACCESS_KEY_KEY: (
                DirectSecret("test_secret_access_key").to_dict()
            ),
        }
    }
    assert build_credentials(credentials) == credentials
    assert build_credentials(credentials.to_dict()) == credentials
    assert credentials.__repr__()


def test_s3_access_key_credentials_update():
    credentials = S3AccessKeyCredentials(
        aws_access_key_id="test_access_key_id",
        aws_secret_access_key="test_secret_access_key",
    )
    assert credentials.aws_access_key_id == DirectSecret("test_access_key_id")
    assert credentials.aws_secret_access_key == DirectSecret("test_secret_access_key")
    credentials.aws_access_key_id = "new_test_access_key_id"
    credentials.aws_secret_access_key = "new_test_secret_access_key"
    assert credentials.aws_access_key_id == DirectSecret("new_test_access_key_id")
    assert credentials.aws_secret_access_key == DirectSecret(
        "new_test_secret_access_key"
    )
    credentials.aws_access_key_id = HashiCorpSecret(
        "secret_path", "new_test_access_key_id"
    )
    credentials.aws_secret_access_key = HashiCorpSecret(
        "secret_path", "new_test_secret_access_key"
    )
    assert credentials.aws_access_key_id == HashiCorpSecret(
        "secret_path", "new_test_access_key_id"
    )
    assert credentials.aws_secret_access_key == HashiCorpSecret(
        "secret_path", "new_test_secret_access_key"
    )


def test_build_s3_credentials_from_dictionary():
    credentials = {
        S3AccessKeyCredentials.IDENTIFIER: {
            S3AccessKeyCredentials.AWS_ACCESS_KEY_ID_KEY: "test_access_key_id",
            S3AccessKeyCredentials.AWS_SECRET_ACCESS_KEY_KEY: "test_secret_access_key",
        }
    }
    built_credentials = build_credentials(credentials)
    assert isinstance(built_credentials, S3AccessKeyCredentials)
    assert built_credentials.aws_access_key_id == DirectSecret("test_access_key_id")
    assert built_credentials.aws_secret_access_key == DirectSecret(
        "test_secret_access_key"
    )


def test_user_password_credentials_initialization():
    credentials = UserPasswordCredentials(
        user="test_user",
        password="test_password",
    )
    assert credentials.user == DirectSecret("test_user")
    assert credentials.password == DirectSecret("test_password")
    assert credentials.to_dict() == {
        UserPasswordCredentials.IDENTIFIER: {
            UserPasswordCredentials.USER_KEY: DirectSecret("test_user").to_dict(),
            UserPasswordCredentials.PASSWORD_KEY: (
                DirectSecret("test_password").to_dict()
            ),
        }
    }
    assert build_credentials(credentials) == credentials
    assert build_credentials(credentials.to_dict()) == credentials
    assert credentials.__repr__()


def test_user_password_credentials_update():
    credentials = UserPasswordCredentials(
        user="test_user",
        password="test_password",
    )
    assert credentials.user == DirectSecret("test_user")
    assert credentials.password == DirectSecret("test_password")
    credentials.user = "new_test_user"
    credentials.password = "new_test_password"
    assert credentials.user == DirectSecret("new_test_user")
    assert credentials.password == DirectSecret("new_test_password")
    credentials.user = HashiCorpSecret("secret_path", "new_test_user")
    credentials.password = HashiCorpSecret("secret_path", "new_test_password")
    assert credentials.user == HashiCorpSecret("secret_path", "new_test_user")
    assert credentials.password == HashiCorpSecret("secret_path", "new_test_password")


def test_user_password_credentials_from_dictionary():
    credentials = {
        UserPasswordCredentials.IDENTIFIER: {
            UserPasswordCredentials.USER_KEY: "test_user",
            UserPasswordCredentials.PASSWORD_KEY: "test_password",
        }
    }
    built_credentials = build_credentials(credentials)
    assert isinstance(built_credentials, UserPasswordCredentials)
    assert built_credentials.user == DirectSecret("test_user")
    assert built_credentials.password == DirectSecret("test_password")


def test_user_password_credentials_object_and_dict_not_equal():
    credentials = UserPasswordCredentials(
        user="test_user",
        password="test_password",
    )
    credentials_dict = {
        UserPasswordCredentials.IDENTIFIER: {
            UserPasswordCredentials.USER_KEY: "test_user",
            UserPasswordCredentials.PASSWORD_KEY: "test_password",
        }
    }
    assert credentials != credentials_dict


def test_build_credentials_wrong_type_raises_exception():
    credentials = "wrong_type"
    with pytest.raises(CredentialsConfigurationError) as e:
        build_credentials(credentials)
    assert e.value.error_code == ErrorCode.CCE3


def test_build_credentials_from_wrong_dictionary_id_raises_exception():
    credentials = {
        "wrong-id": {
            S3AccessKeyCredentials.AWS_ACCESS_KEY_ID_KEY: "test_access_key_id",
            S3AccessKeyCredentials.AWS_SECRET_ACCESS_KEY_KEY: "test_secret_access_key",
        }
    }
    with pytest.raises(CredentialsConfigurationError) as e:
        build_credentials(credentials)
    assert e.value.error_code == ErrorCode.CCE1


def test_build_credentials_from_multiple_dictionary_id_raises_exception():
    credentials = {
        S3AccessKeyCredentials.IDENTIFIER: {
            S3AccessKeyCredentials.AWS_ACCESS_KEY_ID_KEY: "test_access_key_id",
            S3AccessKeyCredentials.AWS_SECRET_ACCESS_KEY_KEY: "test_secret_access_key",
        },
        "wrong-id": {
            S3AccessKeyCredentials.AWS_ACCESS_KEY_ID_KEY: "test_access_key_id",
            S3AccessKeyCredentials.AWS_SECRET_ACCESS_KEY_KEY: "test_secret_access_key",
        },
    }
    with pytest.raises(CredentialsConfigurationError) as e:
        build_credentials(credentials)
    assert e.value.error_code == ErrorCode.CCE1


def test_build_credentials_from_wrong_dictionary_content_raises_exception():
    credentials = {S3AccessKeyCredentials.IDENTIFIER: 42}
    with pytest.raises(CredentialsConfigurationError) as e:
        build_credentials(credentials)
    assert e.value.error_code == ErrorCode.CCE2


def test_s3_credentials_object_and_dict_not_equal():
    credentials = S3AccessKeyCredentials(
        aws_access_key_id="test_access_key_id",
        aws_secret_access_key="test_secret_access_key",
    )
    credentials_dict = {
        S3AccessKeyCredentials.IDENTIFIER: {
            S3AccessKeyCredentials.AWS_ACCESS_KEY_ID_KEY: "test_access_key_id",
            S3AccessKeyCredentials.AWS_SECRET_ACCESS_KEY_KEY: "test_secret_access_key",
        }
    }
    assert credentials != credentials_dict
