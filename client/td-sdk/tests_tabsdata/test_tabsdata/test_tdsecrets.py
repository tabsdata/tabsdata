#
# Copyright 2024 Tabs Data Inc.
#

import os

import pytest
from pytest import MonkeyPatch

from tabsdata import (
    EnvironmentSecret,
    HashiCorpSecret,
)
from tabsdata._secret import (
    DirectSecret,
    _recursively_evaluate_secret,
    _recursively_load_secret,
    build_secret,
)
from tabsdata.exceptions import ErrorCode, SecretConfigurationError
from tests_tabsdata.conftest import (
    HASHICORP_TESTING_SECRET_NAME,
    HASHICORP_TESTING_SECRET_PATH,
    HASHICORP_TESTING_SECRET_VALUE,
)


@pytest.mark.unit
def test_direct_secret_initialization():
    secret = DirectSecret(
        "secret_thing",
    )
    assert secret.secret_value == "secret_thing"
    assert secret._to_dict() == {
        DirectSecret.IDENTIFIER: {
            DirectSecret.SECRET_DIRECT_VALUE_KEY: "secret_thing",
        }
    }
    assert build_secret(secret) == secret
    assert build_secret(secret._to_dict()) == secret
    assert secret.__repr__()
    assert secret.secret_value == "secret_thing"


@pytest.mark.unit
def test_update_direct_secret():
    secret = DirectSecret(
        "secret_thing",
    )
    assert secret.secret_value == "secret_thing"
    assert secret.secret_direct_value == "secret_thing"
    secret.secret_direct_value = "new_secret_thing"
    assert secret.secret_value == "new_secret_thing"
    assert secret.secret_direct_value == "new_secret_thing"


@pytest.mark.unit
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


@pytest.mark.unit
def test_build_secret_with_string_returns_direct_secret():
    secret = "secret_thing"
    built_secret = build_secret(secret)
    assert isinstance(built_secret, DirectSecret)
    assert built_secret.secret_value == "secret_thing"


@pytest.mark.unit
def test_build_secret_wrong_type_raises_exception():
    secret = 42
    with pytest.raises(SecretConfigurationError) as e:
        build_secret(secret)
    assert e.value.error_code == ErrorCode.SCE3


@pytest.mark.unit
def test_build_secret_from_wrong_dictionary_id_raises_exception():
    secret = {
        "wrong-id": {
            DirectSecret.SECRET_DIRECT_VALUE_KEY: "secret_thing",
        }
    }
    with pytest.raises(SecretConfigurationError) as e:
        build_secret(secret)
    assert e.value.error_code == ErrorCode.SCE1


@pytest.mark.unit
def test_build_secret_from_multiple_dictionary_id_raises_exception():
    secret = {
        DirectSecret.IDENTIFIER: {
            DirectSecret.SECRET_DIRECT_VALUE_KEY: "secret_thing",
        },
        HashiCorpSecret.IDENTIFIER: {
            HashiCorpSecret.NAME_KEY: "another_secret_thing",
            HashiCorpSecret.PATH_KEY: "secret_thing",
        },
    }
    with pytest.raises(SecretConfigurationError) as e:
        build_secret(secret)
    assert e.value.error_code == ErrorCode.SCE1


@pytest.mark.unit
def test_build_secret_from_wrong_dictionary_content_raises_exception():
    secret = {DirectSecret.IDENTIFIER: 42}
    with pytest.raises(SecretConfigurationError) as e:
        build_secret(secret)
    assert e.value.error_code == ErrorCode.SCE2


@pytest.mark.unit
def test_secret_object_and_dict_not_equal():
    secret = DirectSecret("secret_thing")
    secret_dict = secret._to_dict()
    assert secret != secret_dict


@pytest.mark.unit
def test_build_hashicorp_secret():
    hashicorp_secret = HashiCorpSecret("secret_thing_path", "secret_thing_name")
    assert hashicorp_secret.name == "secret_thing_name"
    assert hashicorp_secret.path == "secret_thing_path"
    assert hashicorp_secret.vault == "HASHICORP"
    assert hashicorp_secret._to_dict() == {
        HashiCorpSecret.IDENTIFIER: {
            HashiCorpSecret.PATH_KEY: "secret_thing_path",
            HashiCorpSecret.NAME_KEY: "secret_thing_name",
            HashiCorpSecret.VAULT_KEY: "HASHICORP",
        }
    }
    assert build_secret(hashicorp_secret) == hashicorp_secret
    assert build_secret(hashicorp_secret._to_dict()) == hashicorp_secret
    assert hashicorp_secret.__repr__()


@pytest.mark.unit
def test_build_hashicorp_secret_with_vault_name():
    hashicorp_secret = HashiCorpSecret(
        "secret_thing_path", "secret_thing_name", vault="TESTING_VAULT_NAME"
    )
    assert hashicorp_secret.name == "secret_thing_name"
    assert hashicorp_secret.path == "secret_thing_path"
    assert hashicorp_secret.vault == "TESTING_VAULT_NAME"
    assert hashicorp_secret._to_dict() == {
        HashiCorpSecret.IDENTIFIER: {
            HashiCorpSecret.PATH_KEY: "secret_thing_path",
            HashiCorpSecret.NAME_KEY: "secret_thing_name",
            HashiCorpSecret.VAULT_KEY: "TESTING_VAULT_NAME",
        }
    }
    assert build_secret(hashicorp_secret) == hashicorp_secret
    assert build_secret(hashicorp_secret._to_dict()) == hashicorp_secret
    assert hashicorp_secret.__repr__()


@pytest.mark.hashicorp
def test_hashicorp_secret_value(testing_hashicorp_vault):
    hashicorp_secret = HashiCorpSecret(
        HASHICORP_TESTING_SECRET_PATH, HASHICORP_TESTING_SECRET_NAME
    )
    assert hashicorp_secret.secret_value == HASHICORP_TESTING_SECRET_VALUE


@pytest.mark.hashicorp
def test_hashicorp_secret_value_vault_name(testing_hashicorp_vault):
    hashicorp_secret = HashiCorpSecret(
        HASHICORP_TESTING_SECRET_PATH, HASHICORP_TESTING_SECRET_NAME, vault="H1"
    )
    assert hashicorp_secret.secret_value == HASHICORP_TESTING_SECRET_VALUE


@pytest.mark.hashicorp
def test_hashicorp_secret_value_vault_name_no_exists(testing_hashicorp_vault):
    hashicorp_secret = HashiCorpSecret(
        HASHICORP_TESTING_SECRET_PATH,
        HASHICORP_TESTING_SECRET_NAME,
        vault="NO_EXISTS",
    )
    with pytest.raises(ValueError):
        _ = hashicorp_secret.secret_value


@pytest.mark.hashicorp
def test_hashicorp_secret_name_not_exist(testing_hashicorp_vault):
    hashicorp_secret = HashiCorpSecret(HASHICORP_TESTING_SECRET_PATH, "does_not_exist")
    with pytest.raises(ValueError):
        _ = hashicorp_secret.secret_value


@pytest.mark.hashicorp
def test_hashicorp_secret_path_not_exist(testing_hashicorp_vault):
    hashicorp_secret = HashiCorpSecret("does_not_exist", HASHICORP_TESTING_SECRET_NAME)
    with pytest.raises(ValueError):
        _ = hashicorp_secret.secret_value


@pytest.mark.hashicorp
def test_hashicorp_secret_url_env_var_not_exist(testing_hashicorp_vault):
    with MonkeyPatch.context() as mp:
        mp.setenv(HashiCorpSecret.VAULT_URL_ENV_VAR, "WRONG_URL")
        hashicorp_secret = HashiCorpSecret(
            HASHICORP_TESTING_SECRET_PATH, HASHICORP_TESTING_SECRET_NAME
        )
        with pytest.raises(ValueError):
            _ = hashicorp_secret.secret_value


@pytest.mark.hashicorp
def test_hashicorp_secret_token_env_var_not_exist(testing_hashicorp_vault):
    with MonkeyPatch.context() as mp:
        mp.setenv(HashiCorpSecret.VAULT_TOKEN_ENV_VAR, "WRONG_TOKEN")
        hashicorp_secret = HashiCorpSecret(
            HASHICORP_TESTING_SECRET_PATH, HASHICORP_TESTING_SECRET_NAME
        )
        with pytest.raises(ValueError):
            _ = hashicorp_secret.secret_value


@pytest.mark.unit
def test_hashicorp_secret_vault_wrong_type():
    with pytest.raises(SecretConfigurationError) as e:
        HashiCorpSecret("secret_thing_path", "secret_thing_name", vault=42)
    assert e.value.error_code == ErrorCode.SCE4


@pytest.mark.unit
def test_hashicorp_secret_vault_wrong_value():
    with pytest.raises(SecretConfigurationError) as e:
        HashiCorpSecret("secret_thing_path", "secret_thing_name", vault="a")
    assert e.value.error_code == ErrorCode.SCE5
    with pytest.raises(SecretConfigurationError) as e:
        HashiCorpSecret("secret_thing_path", "secret_thing_name", vault="3")
    assert e.value.error_code == ErrorCode.SCE5
    with pytest.raises(SecretConfigurationError) as e:
        HashiCorpSecret("secret_thing_path", "secret_thing_name", vault="3A_")
    assert e.value.error_code == ErrorCode.SCE5
    with pytest.raises(SecretConfigurationError) as e:
        HashiCorpSecret("secret_thing_path", "secret_thing_name", vault="A3v")
    assert e.value.error_code == ErrorCode.SCE5
    with pytest.raises(SecretConfigurationError) as e:
        HashiCorpSecret("secret_thing_path", "secret_thing_name", vault="")
    assert e.value.error_code == ErrorCode.SCE5
    with pytest.raises(SecretConfigurationError) as e:
        HashiCorpSecret("secret_thing_path", "secret_thing_name", vault="A3+")
    assert e.value.error_code == ErrorCode.SCE5


@pytest.mark.unit
def test_secret_repr():
    secret = DirectSecret("secret_thing")
    assert secret.__repr__()
    hashicorp_secret = HashiCorpSecret("secret_thing_path", "secret_thing_name")
    assert hashicorp_secret.__repr__()
    environment_secret = EnvironmentSecret("secret_variable_name")
    assert environment_secret.__repr__()


@pytest.mark.unit
def test_build_environment_secret():
    environment_secret = EnvironmentSecret("environment_variable_name")
    assert environment_secret.environment_variable_name == "environment_variable_name"
    assert environment_secret._to_dict() == {
        EnvironmentSecret.IDENTIFIER: {
            EnvironmentSecret.ENVIRONMENT_VARIABLE_NAME_KEY: (
                "environment_variable_name"
            ),
        }
    }
    assert build_secret(environment_secret) == environment_secret
    assert build_secret(environment_secret._to_dict()) == environment_secret
    assert environment_secret.__repr__()


@pytest.mark.unit
def test_environment_secret_value():
    environment_secret = EnvironmentSecret("testing_environment_variable")
    os.environ["testing_environment_variable"] = "testing_value"
    assert environment_secret.secret_value == "testing_value"


@pytest.mark.unit
def test_environment_secret_value_not_exist():
    environment_secret = EnvironmentSecret("does_not_exist")
    with pytest.raises(ValueError):
        _ = environment_secret.secret_value


@pytest.mark.unit
def test_recursively_load_secret():
    secret_dict = EnvironmentSecret("does_not_exist")._to_dict()
    assert _recursively_load_secret(secret_dict) == EnvironmentSecret("does_not_exist")
    value = ("hello", secret_dict)
    assert _recursively_load_secret(value) == (
        "hello",
        EnvironmentSecret("does_not_exist"),
    )
    value = "hello"
    assert _recursively_load_secret(value) == "hello"
    value = ["hello", secret_dict]
    assert _recursively_load_secret(value) == [
        "hello",
        EnvironmentSecret("does_not_exist"),
    ]
    value = {"key": secret_dict}
    assert _recursively_load_secret(value) == {
        "key": EnvironmentSecret("does_not_exist")
    }
    value = {"key": "hello"}
    assert _recursively_load_secret(value) == {"key": "hello"}
    value = {"key": ["hello", secret_dict]}
    assert _recursively_load_secret(value) == {
        "key": ["hello", EnvironmentSecret("does_not_exist")]
    }
    value = {"key": {"subkey": secret_dict}}
    assert _recursively_load_secret(value) == {
        "key": {"subkey": EnvironmentSecret("does_not_exist")}
    }
    value = {"key": {"subkey": "hello"}}
    assert _recursively_load_secret(value) == {"key": {"subkey": "hello"}}
    value = {"key": {"subkey": ["hello", secret_dict]}}
    assert _recursively_load_secret(value) == {
        "key": {"subkey": ["hello", EnvironmentSecret("does_not_exist")]}
    }
    value = {"key": {"subkey": {"subsubkey": secret_dict}}}
    assert _recursively_load_secret(value) == {
        "key": {"subkey": {"subsubkey": EnvironmentSecret("does_not_exist")}}
    }


@pytest.mark.unit
def test_recursively_evaluate_secret():
    secret = DirectSecret("evaluated_secret_value")
    secret_dict = secret._to_dict()
    assert _recursively_evaluate_secret(secret_dict) == "evaluated_secret_value"
    value = ("hello", secret_dict)
    assert _recursively_evaluate_secret(value) == (
        "hello",
        "evaluated_secret_value",
    )
    value = "hello"
    assert _recursively_evaluate_secret(value) == "hello"
    value = ["hello", secret_dict]
    assert _recursively_evaluate_secret(value) == [
        "hello",
        "evaluated_secret_value",
    ]
    value = {"key": secret_dict}
    assert _recursively_evaluate_secret(value) == {"key": "evaluated_secret_value"}
    value = {"key": "hello"}
    assert _recursively_evaluate_secret(value) == {"key": "hello"}
    value = {"key": ["hello", secret_dict]}
    assert _recursively_evaluate_secret(value) == {
        "key": ["hello", "evaluated_secret_value"]
    }
    value = {"key": {"subkey": secret_dict}}
    assert _recursively_evaluate_secret(value) == {
        "key": {"subkey": "evaluated_secret_value"}
    }
    value = {"key": {"subkey": "hello"}}
    assert _recursively_evaluate_secret(value) == {"key": {"subkey": "hello"}}
    value = {"key": {"subkey": ["hello", secret_dict]}}
    assert _recursively_evaluate_secret(value) == {
        "key": {"subkey": ["hello", "evaluated_secret_value"]}
    }
    value = {"key": {"subkey": {"subsubkey": secret_dict}}}
    assert _recursively_evaluate_secret(value) == {
        "key": {"subkey": {"subsubkey": "evaluated_secret_value"}}
    }
    # Now we use the secret directly instead of the secret_dict
    assert _recursively_evaluate_secret(secret) == "evaluated_secret_value"
    value = ("hello", secret)
    assert _recursively_evaluate_secret(value) == (
        "hello",
        "evaluated_secret_value",
    )
    value = "hello"
    assert _recursively_evaluate_secret(value) == "hello"
    value = ["hello", secret]
    assert _recursively_evaluate_secret(value) == [
        "hello",
        "evaluated_secret_value",
    ]
    value = {"key": secret}
    assert _recursively_evaluate_secret(value) == {"key": "evaluated_secret_value"}
    value = {"key": "hello"}
    assert _recursively_evaluate_secret(value) == {"key": "hello"}
    value = {"key": ["hello", secret]}
    assert _recursively_evaluate_secret(value) == {
        "key": ["hello", "evaluated_secret_value"]
    }
    value = {"key": {"subkey": secret}}
    assert _recursively_evaluate_secret(value) == {
        "key": {"subkey": "evaluated_secret_value"}
    }
    value = {"key": {"subkey": "hello"}}
    assert _recursively_evaluate_secret(value) == {"key": {"subkey": "hello"}}
    value = {"key": {"subkey": ["hello", secret]}}
    assert _recursively_evaluate_secret(value) == {
        "key": {"subkey": ["hello", "evaluated_secret_value"]}
    }
    value = {"key": {"subkey": {"subsubkey": secret}}}
    assert _recursively_evaluate_secret(value) == {
        "key": {"subkey": {"subsubkey": "evaluated_secret_value"}}
    }
