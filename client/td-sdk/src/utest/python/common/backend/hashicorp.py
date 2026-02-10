#
# Copyright 2026 Tabs Data Inc.
#

"""Hashicorp Vault fixtures and utilities."""

import logging
import os
from time import sleep

import hvac
import pytest
from filelock import FileLock

from tabsdata._secret import HashiCorpSecret
from src.utest.python.common.constants import (
    MAXIMUM_RETRY_COUNT,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
)

logger = logging.getLogger(__name__)

DEFAULT_PYTEST_HASHICORP_DOCKER_CONTAINER_NAME = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}c_hashicorp"
)

HASHICORP_PORT = 8200
HASHICORP_TESTING_SECRET_PATH = "tabsdata/testing/path"
HASHICORP_TESTING_SECRET_NAME = "testing_secret_name"
HASHICORP_TESTING_SECRET_VALUE = "testing_secret_value"
HASHICORP_TESTING_TOKEN = "testing_token"
HASHICORP_TESTING_URL = "http://127.0.0.1:8200"


def create_docker_hashicorp_vault(s3_config: dict):
    # noinspection PyPackageRequirements
    import docker  # Lazy import

    logger.info("Starting HashiCorp vault container")
    client = docker.from_env()
    if client.containers.list(
        filters={"name": DEFAULT_PYTEST_HASHICORP_DOCKER_CONTAINER_NAME}
    ):
        logger.info("HashiCorp vault container already exists")
        return
    else:
        client.containers.run(
            "hashicorp/vault",
            name=DEFAULT_PYTEST_HASHICORP_DOCKER_CONTAINER_NAME,
            environment=[
                f"VAULT_DEV_ROOT_TOKEN_ID={HASHICORP_TESTING_TOKEN}",
            ],
            ports={"8200/tcp": HASHICORP_PORT},
            detach=True,
        )
        # Wait for the database to be ready
        retry = 0
        while True:
            try:
                hashicorp_client = hvac.Client(
                    url=HASHICORP_TESTING_URL,
                    token=HASHICORP_TESTING_TOKEN,
                    verify=True,
                )
                hashicorp_client.secrets.kv.v2.create_or_update_secret(
                    path=HASHICORP_TESTING_SECRET_PATH,
                    secret={
                        HASHICORP_TESTING_SECRET_NAME: HASHICORP_TESTING_SECRET_VALUE
                    },
                )
                # Needed for test_input_s3_hashicorp_secret to work
                hashicorp_client.secrets.kv.v2.create_or_update_secret(
                    path="aws/s3creds",
                    secret={
                        "access_key_id": os.environ.get(s3_config["ACCESS_KEY_ENV"])
                    },
                )
                # Needed for the config_resolver tests to work
                hashicorp_client.secrets.kv.v2.create_or_update_secret(
                    path="/tabsdata/dev",
                    secret={"jwt_secret": "jwt_secret_value"},
                )
                hashicorp_client.secrets.kv.v2.create_or_update_secret(
                    path="/tabsdata/dev/s3a",
                    secret={
                        "bucket": "bucket_value",
                        "region": "region_value",
                        "access_key": "access_key_value",
                        "secret_key": "secret_key_value",
                    },
                )
                hashicorp_client.secrets.kv.v2.create_or_update_secret(
                    path="/td/dev/aza",
                    secret={
                        "azure_account_name": "azure_account_name_value",
                        "azure_account_key": "azure_account_key_value",
                    },
                )
                return
            except Exception as err:
                retry += 1
                # Waiting for up to 10'' & 30'', as young Gauss already knew.
                if retry == MAXIMUM_RETRY_COUNT:
                    logger.error(f"Error connecting to HashiCorp vault: {err}")
                    raise err
                else:
                    logger.warning(
                        f"Error connecting to HashiCorp vault, retrying in {retry} "
                        "second(s)"
                    )
                    sleep(retry)


@pytest.fixture(scope="session")
def testing_hashicorp_vault(tmp_path_factory, worker_id, s3_config):
    os.environ[HashiCorpSecret.VAULT_URL_ENV_VAR] = HASHICORP_TESTING_URL
    os.environ[HashiCorpSecret.VAULT_TOKEN_ENV_VAR] = HASHICORP_TESTING_TOKEN
    # Needed for test_input_s3_hashicorp_secret_vault_name to work
    os.environ[HashiCorpSecret.VAULT_URL_ENV_VAR.replace("HASHICORP", "H1")] = (
        HASHICORP_TESTING_URL
    )
    os.environ[HashiCorpSecret.VAULT_TOKEN_ENV_VAR.replace("HASHICORP", "H1")] = (
        HASHICORP_TESTING_TOKEN
    )
    if worker_id == "master":
        # not executing in with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        yield create_docker_hashicorp_vault(s3_config)
    else:
        # get the temp directory shared by all workers
        root_tmp_dir = tmp_path_factory.getbasetemp().parent

        fn = root_tmp_dir / "docker_hashicorp_vault_creation"
        with FileLock(str(fn) + ".lock"):
            # only one worker will be able to create the database
            create_docker_hashicorp_vault(s3_config)
        yield
