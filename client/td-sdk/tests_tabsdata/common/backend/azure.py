#
# Copyright 2026 Tabs Data Inc.
#

"""Azure fixtures and utilities."""

import os

import pytest
from azure.storage.blob import BlobServiceClient

import tabsdata as td

TESTING_AZURE_ACCOUNT_NAME = "TRANSPORTER_AZURE_ACCOUNT_NAME"
TESTING_AZURE_ACCOUNT_KEY = "TRANSPORTER_AZURE_ACCOUNT_KEY"


@pytest.fixture(scope="session")
def azure_config():

    config = {
        "ACCOUNT_NAME_ENV": "AZ0__AZ_ACCOUNT_NAME",
        "ACCOUNT_KEY_ENV": "AZ0__AZ_ACCOUNT_KEY",
        "URI_ENV": "AZ0__AZ_URI",
    }
    account_name = os.environ.get(config["ACCOUNT_NAME_ENV"])
    account_key = os.environ.get(config["ACCOUNT_KEY_ENV"])
    if not account_name or not account_key:
        raise Exception(
            f"The environment variables {config['ACCOUNT_NAME_ENV']} and/or "
            f"{config['ACCOUNT_KEY_ENV']} are not set. Unable to run tests "
            "using the 'azure_config' fixture."
        )
    credentials = td.AzureAccountKeyCredentials(
        td.EnvironmentSecret(config["ACCOUNT_NAME_ENV"]),
        td.EnvironmentSecret(config["ACCOUNT_KEY_ENV"]),
    )
    config["CREDENTIALS"] = credentials
    uri = os.environ.get(config["URI_ENV"])
    if not uri:
        raise Exception(
            f"The environment variable {config['URI_ENV']} is not set. Unable to run "
            "tests using the 'azure_config' fixture."
        )
    config["URI"] = uri
    container_name = uri.replace("az://", "").split("/", 1)[0]
    config["CONTAINER_NAME"] = container_name
    yield config


@pytest.fixture(scope="session")
def azure_client(azure_config):
    # noinspection PyPackageRequirements

    account_name = os.environ.get(azure_config["ACCOUNT_NAME_ENV"])
    account_key = os.environ.get(azure_config["ACCOUNT_KEY_ENV"])
    service = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key,
    )
    yield service
