#
# Copyright 2024 Tabs Data Inc.
#

import os

import pytest

from tabsdata import DirectSecret, EnvironmentSecret, HashiCorpSecret
from tabsdata.exceptions import ErrorCode, SecretConfigurationError
from tabsdata.secret import build_secret


def test_direct_secret_initialization():
    secret = DirectSecret(
        "secret_thing",
    )
    assert secret.secret_value == "secret_thing"
    assert secret.to_dict() == {
        DirectSecret.IDENTIFIER: {
            DirectSecret.SECRET_DIRECT_VALUE_KEY: "secret_thing",
        }
    }
    assert build_secret(secret) == secret
    assert build_secret(secret.to_dict()) == secret
    assert secret.__repr__()
    assert secret.secret_value == "secret_thing"


def test_update_direct_secret():
    secret = DirectSecret(
        "secret_thing",
    )
    assert secret.secret_value == "secret_thing"
    assert secret.secret_direct_value == "secret_thing"
    secret.secret_direct_value = "new_secret_thing"
    assert secret.secret_value == "new_secret_thing"
    assert secret.secret_direct_value == "new_secret_thing"


def test_build_secret_from_dictionary():
    secret = {
        DirectSecret.IDENTIFIER: {
            DirectSecret.SECRET_DIRECT_VALUE_KEY: "secret_thing",
        }
    }
    built_secret = build_secret(secret)
    assert isinstance(built_secret, DirectSecret)
    assert built_secret.secret_value == "secret_thing"
    assert built_secret.secret_value == "secret_thing"


def test_build_secret_with_string_returns_direct_secret():
    secret = "secret_thing"
    built_secret = build_secret(secret)
    assert isinstance(built_secret, DirectSecret)
    assert built_secret.secret_value == "secret_thing"


def test_build_secret_wrong_type_raises_exception():
    secret = 42
    with pytest.raises(SecretConfigurationError) as e:
        build_secret(secret)
    assert e.value.error_code == ErrorCode.SCE3


def test_build_secret_from_wrong_dictionary_id_raises_exception():
    secret = {
        "wrong-id": {
            DirectSecret.SECRET_DIRECT_VALUE_KEY: "secret_thing",
        }
    }
    with pytest.raises(SecretConfigurationError) as e:
        build_secret(secret)
    assert e.value.error_code == ErrorCode.SCE1


def test_build_secret_from_multiple_dictionary_id_raises_exception():
    secret = {
        DirectSecret.IDENTIFIER: {
            DirectSecret.SECRET_DIRECT_VALUE_KEY: "secret_thing",
        },
        HashiCorpSecret.IDENTIFIER: {
            DirectSecret.SECRET_DIRECT_VALUE_KEY: "secret_thing",
        },
    }
    with pytest.raises(SecretConfigurationError) as e:
        build_secret(secret)
    assert e.value.error_code == ErrorCode.SCE1


def test_build_secret_from_wrong_dictionary_content_raises_exception():
    secret = {DirectSecret.IDENTIFIER: 42}
    with pytest.raises(SecretConfigurationError) as e:
        build_secret(secret)
    assert e.value.error_code == ErrorCode.SCE2


def test_secret_object_and_dict_not_equal():
    secret = DirectSecret("secret_thing")
    secret_dict = secret.to_dict()
    assert secret != secret_dict


def test_build_hashicorp_secret():
    hashicorp_secret = HashiCorpSecret("secret_thing_name")
    assert hashicorp_secret.secret_name == "secret_thing_name"
    assert hashicorp_secret.to_dict() == {
        HashiCorpSecret.IDENTIFIER: {
            HashiCorpSecret.SECRET_NAME_KEY: "secret_thing_name",
        }
    }
    assert build_secret(hashicorp_secret) == hashicorp_secret
    assert build_secret(hashicorp_secret.to_dict()) == hashicorp_secret
    assert hashicorp_secret.__repr__()


def test_hashicorp_secret_value():
    hashicorp_secret = HashiCorpSecret("secret_thing_name")
    with pytest.raises(NotImplementedError):
        _ = hashicorp_secret.secret_value


def test_secret_repr():
    secret = DirectSecret("secret_thing")
    assert secret.__repr__()
    hashicorp_secret = HashiCorpSecret("secret_thing_name")
    assert hashicorp_secret.__repr__()
    environment_secret = EnvironmentSecret("secret_variable_name")
    assert environment_secret.__repr__()


def test_build_environment_secret():
    environment_secret = EnvironmentSecret("environment_variable_name")
    assert environment_secret.environment_variable_name == "environment_variable_name"
    assert environment_secret.to_dict() == {
        EnvironmentSecret.IDENTIFIER: {
            EnvironmentSecret.ENVIRONMENT_VARIABLE_NAME_KEY: (
                "environment_variable_name"
            ),
        }
    }
    assert build_secret(environment_secret) == environment_secret
    assert build_secret(environment_secret.to_dict()) == environment_secret
    assert environment_secret.__repr__()


def test_environment_secret_value():
    environment_secret = EnvironmentSecret("testing_environment_variable")
    os.environ["testing_environment_variable"] = "testing_value"
    assert environment_secret.secret_value == "testing_value"


def test_environment_secret_value_not_exist():
    environment_secret = EnvironmentSecret("does_not_exist")
    with pytest.raises(ValueError):
        _ = environment_secret.secret_value
