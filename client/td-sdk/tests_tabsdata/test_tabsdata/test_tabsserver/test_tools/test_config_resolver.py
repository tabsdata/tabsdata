#
# Copyright 2025 Tabs Data Inc.
#

import os
from unittest import mock

import pytest
import yaml

from tabsdata._tabsserver.tools.config_resolver import ConfigResolver
from tests_tabsdata.conftest import (
    HASHICORP_TESTING_TOKEN,
    HASHICORP_TESTING_URL,
    TESTING_RESOURCES_FOLDER,
)

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401

BASE_RESOLVER = ConfigResolver()
HASHICORP_RESOLVER = ConfigResolver(
    hashicorp_url=HASHICORP_TESTING_URL, hashicorp_token=HASHICORP_TESTING_TOKEN
)


@pytest.mark.config_resolver
def test_class_attributes():
    resolver = ConfigResolver()
    assert resolver.strategy_to_function
    assert resolver.hashicorp_config.get("url") is None
    assert resolver.hashicorp_config.get("token") is None
    assert resolver.hashicorp_config.get("namespace") is None

    resolver = ConfigResolver(
        hashicorp_url="url", hashicorp_token="token", hashicorp_namespace="namespace"
    )
    assert resolver.strategy_to_function
    assert resolver.hashicorp_config.get("url") == "url"
    assert resolver.hashicorp_config.get("token") == "token"
    assert resolver.hashicorp_config.get("namespace") == "namespace"


@pytest.mark.config_resolver
@mock.patch.dict(os.environ, {"ENV_VAR_NAME": "env_var_value"}, clear=True)
def test_resolve_env_token_single_value():
    leaf = "${env:ENV_VAR_NAME}"
    resolved_leaf = BASE_RESOLVER.resolve_env_token(leaf)
    assert resolved_leaf == "env_var_value"


@pytest.mark.config_resolver
@mock.patch.dict(
    os.environ,
    {"ENV_VAR_NAME": "env_var_value", "ANOTHER_ENV_VAR_NAME": "another_env_var_value"},
    clear=True,
)
def test_resolve_env_token_multiple_values():
    leaf = "prefix_${env:ENV_VAR_NAME}_suffix_${env:ANOTHER_ENV_VAR_NAME}"
    resolved_leaf = BASE_RESOLVER.resolve_env_token(leaf)
    assert resolved_leaf == "prefix_env_var_value_suffix_another_env_var_value"


@pytest.mark.config_resolver
@mock.patch.dict(
    os.environ,
    {"ENV_VAR_NAME": "env_var_value", "ANOTHER_ENV_VAR_NAME": "another_env_var_value"},
    clear=True,
)
def test_resolve_env_token_no_env_var():
    leaf = "${env:NON_EXISTING_ENV_VAR}"
    with pytest.raises(ValueError):
        BASE_RESOLVER.resolve_env_token(leaf)


@pytest.mark.config_resolver
@mock.patch.dict(
    os.environ,
    {"ENV_VAR_NAME": "env_var_value", "ANOTHER_ENV_VAR_NAME": "another_env_var_value"},
    clear=True,
)
def test_resolve_env_other_token():
    leaf = "${other:NON_EXISTING_TOKEN}_${hashicorp:;}"
    assert BASE_RESOLVER.resolve_env_token(leaf) == leaf


@pytest.mark.config_resolver
@mock.patch.dict(os.environ, {"ENV_VAR_NAME": "env_var_value"}, clear=True)
def test_resolve_leaf_env_strategy():
    leaf = "${env:ENV_VAR_NAME}"
    resolved_leaf = BASE_RESOLVER.resolve_leaf(leaf, "env")
    assert resolved_leaf == "env_var_value"


@pytest.mark.config_resolver
def test_resolve_leaf_wrong_strategy():
    leaf = "${env:ENV_VAR_NAME}"
    with pytest.raises(ValueError):
        BASE_RESOLVER.resolve_leaf(leaf, "wrong_strategy")


@pytest.mark.config_resolver
@pytest.mark.hashicorp
def test_resolve_hashicorp_token_single_value(testing_hashicorp_vault):
    leaf = "${hashicorp:/tabsdata/dev/s3a;region}"
    resolved_leaf = HASHICORP_RESOLVER.resolve_hashicorp_token(leaf)
    assert resolved_leaf == "region_value"


@pytest.mark.config_resolver
@pytest.mark.hashicorp
def test_resolve_hashicorp_token_multiple_values(testing_hashicorp_vault):
    leaf = (
        "prefix_${hashicorp:/tabsdata/dev/s3a;region}_suffix_${"
        "hashicorp:/tabsdata/dev/s3a;bucket}"
    )
    resolved_leaf = HASHICORP_RESOLVER.resolve_hashicorp_token(leaf)
    assert resolved_leaf == "prefix_region_value_suffix_bucket_value"


@pytest.mark.config_resolver
@pytest.mark.hashicorp
def test_resolve_hashicorp_token_no_hashicorp_var(testing_hashicorp_vault):
    leaf = "${hashicorp:/tabsdata/dev/s3a;does_not_exist}"
    with pytest.raises(Exception):
        HASHICORP_RESOLVER.resolve_hashicorp_token(leaf)


@pytest.mark.config_resolver
@pytest.mark.hashicorp
def test_resolve_hashicorp_other_token(testing_hashicorp_vault):
    leaf = "${other:NON_EXISTING_TOKEN}_${env:ENV_VAR_NAME}}"
    assert HASHICORP_RESOLVER.resolve_hashicorp_token(leaf) == leaf


@pytest.mark.config_resolver
@pytest.mark.hashicorp
def test_resolve_leaf_hashicorp_strategy(testing_hashicorp_vault):
    leaf = "${hashicorp:/tabsdata/dev/s3a;region}"
    resolved_leaf = HASHICORP_RESOLVER.resolve_leaf(leaf, "hashicorp")
    assert resolved_leaf == "region_value"


@pytest.mark.config_resolver
@pytest.mark.hashicorp
@mock.patch.dict(
    os.environ,
    {
        "ENV_VAR_NAME": "env_var_value",
        "ANOTHER_ENV_VAR_NAME": "another_env_var_value",
        "HASHICORP_ENV_VAR_NAME": "${hashicorp:/tabsdata/dev/s3a;region}",
    },
    clear=True,
)
def test_resolve_collection_env(testing_hashicorp_vault):
    collection = {
        "region": "the_region_${hashicorp:/tabsdata/dev/s3a;region}",
        "bucket": "${hashicorp:/tabsdata/dev/s3a;bucket}",
        "env_var": "${env:ENV_VAR_NAME}",
        "another_env_var": [
            "${env:ANOTHER_ENV_VAR_NAME}",
            7,
            22,
            {"nested": "${env:ENV_VAR_NAME}"},
        ],
        "hashicorp_env_var": "${env:HASHICORP_ENV_VAR_NAME}",
    }
    expected_result = {
        "region": "the_region_${hashicorp:/tabsdata/dev/s3a;region}",
        "bucket": "${hashicorp:/tabsdata/dev/s3a;bucket}",
        "env_var": "env_var_value",
        "another_env_var": [
            "another_env_var_value",
            7,
            22,
            {"nested": "env_var_value"},
        ],
        "hashicorp_env_var": "${hashicorp:/tabsdata/dev/s3a;region}",
    }
    resolved_collection = HASHICORP_RESOLVER.resolve_collection(collection, "env")
    assert resolved_collection == expected_result


@pytest.mark.config_resolver
@pytest.mark.hashicorp
@mock.patch.dict(
    os.environ,
    {
        "ENV_VAR_NAME": "env_var_value",
        "ANOTHER_ENV_VAR_NAME": "another_env_var_value",
        "HASHICORP_ENV_VAR_NAME": "${hashicorp:/tabsdata/dev/s3a;region}",
    },
    clear=True,
)
def test_resolve_collection_hashicorp(testing_hashicorp_vault):
    collection = {
        "region": "the_region_${hashicorp:/tabsdata/dev/s3a;region}",
        "bucket": "${hashicorp:/tabsdata/dev/s3a;bucket}",
        "env_var": "${env:ENV_VAR_NAME}",
        "another_env_var": [
            "${env:ANOTHER_ENV_VAR_NAME}",
            7,
            22,
            {"nested": "${env:ENV_VAR_NAME}"},
        ],
        "hashicorp_env_var": "${env:HASHICORP_ENV_VAR_NAME}",
    }
    expected_result = {
        "region": "the_region_region_value",
        "bucket": "bucket_value",
        "env_var": "${env:ENV_VAR_NAME}",
        "another_env_var": [
            "${env:ANOTHER_ENV_VAR_NAME}",
            7,
            22,
            {"nested": "${env:ENV_VAR_NAME}"},
        ],
        "hashicorp_env_var": "${env:HASHICORP_ENV_VAR_NAME}",
    }
    resolved_collection = HASHICORP_RESOLVER.resolve_collection(collection, "hashicorp")
    assert resolved_collection == expected_result


@pytest.mark.config_resolver
@pytest.mark.hashicorp
@mock.patch.dict(
    os.environ,
    {
        "ENV_VAR_NAME": "env_var_value",
        "ANOTHER_ENV_VAR_NAME": "another_env_var_value",
        "HASHICORP_ENV_VAR_NAME": "${hashicorp:/tabsdata/dev/s3a;region}",
    },
    clear=True,
)
def test_resolve_yaml_env_hashicorp(testing_hashicorp_vault, tmp_path):
    collection = {
        "region": "the_region_${hashicorp:/tabsdata/dev/s3a;region}",
        "bucket": "${hashicorp:/tabsdata/dev/s3a;bucket}",
        "env_var": "${env:ENV_VAR_NAME}",
        "another_env_var": [
            "${env:ANOTHER_ENV_VAR_NAME}",
            7,
            22,
            {"nested": "${env:ENV_VAR_NAME}"},
        ],
        "hashicorp_env_var": "${env:HASHICORP_ENV_VAR_NAME}",
    }
    destination_path = os.path.join(tmp_path, "test.yaml")
    with open(destination_path, "w") as file:
        yaml.dump(collection, file)
    expected_result = {
        "region": "the_region_region_value",
        "bucket": "bucket_value",
        "env_var": "env_var_value",
        "another_env_var": [
            "another_env_var_value",
            7,
            22,
            {"nested": "env_var_value"},
        ],
        "hashicorp_env_var": "region_value",
    }
    resolved_collection = HASHICORP_RESOLVER.resolve_yaml(
        destination_path, ["env", "hashicorp"]
    )
    assert resolved_collection == expected_result


@pytest.mark.config_resolver
@pytest.mark.hashicorp
@mock.patch.dict(
    os.environ,
    {
        "ENV_VAR_NAME": "env_var_value",
        "ANOTHER_ENV_VAR_NAME": "another_env_var_value",
        "HASHICORP_ENV_VAR_NAME": "${hashicorp:/tabsdata/dev/s3a;region}",
    },
    clear=True,
)
def test_resolve_yaml_hashicorp_env(testing_hashicorp_vault, tmp_path):
    collection = {
        "region": "the_region_${hashicorp:/tabsdata/dev/s3a;region}",
        "bucket": "${hashicorp:/tabsdata/dev/s3a;bucket}",
        "env_var": "${env:ENV_VAR_NAME}",
        "another_env_var": [
            "${env:ANOTHER_ENV_VAR_NAME}",
            7,
            22,
            {"nested": "${env:ENV_VAR_NAME}"},
        ],
        "hashicorp_env_var": "${env:HASHICORP_ENV_VAR_NAME}",
    }
    destination_path = os.path.join(tmp_path, "test.yaml")
    with open(destination_path, "w") as file:
        yaml.dump(collection, file)
    expected_result = {
        "region": "the_region_region_value",
        "bucket": "bucket_value",
        "env_var": "env_var_value",
        "another_env_var": [
            "another_env_var_value",
            7,
            22,
            {"nested": "env_var_value"},
        ],
        "hashicorp_env_var": "${hashicorp:/tabsdata/dev/s3a;region}",
    }
    resolved_collection = HASHICORP_RESOLVER.resolve_yaml(
        destination_path, ["hashicorp", "env"]
    )
    assert resolved_collection == expected_result


@pytest.mark.config_resolver
@pytest.mark.hashicorp
@mock.patch.dict(
    os.environ,
    {
        "TD_URI_REPOSITORY": "td_repository_value",
    },
    clear=True,
)
def test_resolve_example_yaml(testing_hashicorp_vault):
    destination_path = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "config_resolver_resources",
        "example_config_resolver_input.yaml",
    )
    resolved_collection = HASHICORP_RESOLVER.resolve_yaml(
        destination_path, ["hashicorp", "env"]
    )
    expected_result_path = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "config_resolver_resources",
        "expected_config_resolver_output.yaml",
    )
    with open(expected_result_path, "r") as file:
        expected_result = yaml.safe_load(file)
    assert resolved_collection == expected_result
