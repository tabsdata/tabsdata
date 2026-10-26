#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.server._tdrunner.function._commons.solver.secrets import (
    ProjectSecret,
    StorageSecrets,
    take_project_secret,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def project_secret() -> ProjectSecret:
    return ProjectSecret({
        "aws": {"key1": "mykey1", "Key2": "mykey2"},
        "warehouse": {"password": "p"},
    })


def test_names_and_keys_are_listed():
    secret = project_secret()
    assert secret.names == ["aws", "warehouse"]
    assert secret.keys("aws") == ["key1", "Key2"]


def test_value_by_secret_and_key():
    assert project_secret().value("aws", "key1") == "mykey1"


def test_value_by_dotted_reference():
    assert project_secret().value("aws.key1") == "mykey1"


def test_names_and_keys_ignore_case():
    secret = project_secret()
    assert secret.value("AWS", "KEY2") == "mykey2"
    assert secret.value("Aws.key2") == "mykey2"
    assert secret.keys("WAREHOUSE") == ["password"]


def test_an_unknown_secret_lists_the_available_ones():
    with pytest.raises(KeyError, match="Available secrets: aws, warehouse"):
        project_secret().value("gcp.key1")


def test_an_unknown_key_lists_the_available_ones_without_values():
    with pytest.raises(KeyError, match="Available keys: key1, Key2") as error:
        project_secret().value("aws", "key3")
    assert "mykey" not in str(error.value)


def test_a_reference_without_a_key_is_refused():
    with pytest.raises(KeyError, match="has no key"):
        project_secret().value("aws")


def test_values_never_appear_in_the_representation():
    shown = repr(project_secret())
    assert "aws" in shown
    assert "mykey" not in shown


def test_taking_the_secret_removes_it_from_the_environment():
    environment = {
        "SECRET_PRJ__aws__key1": "mykey1",
        "SECRET_PRJ__aws__key2": "mykey2",
        "SECRET_PRJ__warehouse__password": "p",
        "PATH": "/bin",
    }
    secret = take_project_secret(environment)
    assert environment == {"PATH": "/bin"}
    assert sorted(secret.names) == ["aws", "warehouse"]
    assert secret.value("aws.key2") == "mykey2"


def test_taking_the_secret_ignores_the_prefix_case():
    environment = {"secret_prj__AWS__KEY1": "mykey1"}
    secret = take_project_secret(environment)
    assert environment == {}
    assert secret.value("aws.key1") == "mykey1"


def test_malformed_variables_are_removed_and_ignored():
    environment = {"SECRET_PRJ__aws": "x", "SECRET_PRJ____key": "y"}
    secret = take_project_secret(environment)
    assert environment == {}
    assert secret.names == []


def test_no_variables_give_no_secrets():
    assert take_project_secret({"PATH": "/bin"}).names == []


STORAGE_SECRET = "SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AWS_SECRET_ACCESS_KEY"
PROJECT_SECRET = "SECRET_PRJ__AWS__KEY1"
OTHER_SECRET = "SECRETS__AGENT__KEY"


def storage_environment() -> dict[str, str]:
    return {
        STORAGE_SECRET: "secret",
        PROJECT_SECRET: "project",
        OTHER_SECRET: "agent",
        "HOME": "/home",
    }


def test_the_storage_secrets_are_out_while_managed_and_back_after():
    variables = storage_environment()

    with StorageSecrets(variables).managed():
        assert STORAGE_SECRET not in variables
        assert variables[PROJECT_SECRET] == "project"
        assert variables[OTHER_SECRET] == "agent"

    assert variables == storage_environment()


def test_the_storage_secrets_come_back_when_the_managed_code_fails():
    variables = storage_environment()

    with pytest.raises(RuntimeError):
        with StorageSecrets(variables).managed():
            raise RuntimeError("Failure inside the managed block")

    assert variables == storage_environment()


def test_exclude_and_include_work_on_their_own():
    variables = storage_environment()
    secrets = StorageSecrets(variables)

    secrets.exclude()
    assert STORAGE_SECRET not in variables
    secrets.include()

    assert variables == storage_environment()
