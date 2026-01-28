#
# Copyright 2026 Tabs Data Inc.
#

"""Google Cloud Storage fixtures and utilities."""

import os
import uuid

import pytest
from google.cloud import storage


@pytest.fixture(scope="session")
def gcs_config():
    # Note: this is currently very simple, but if needed it can be extended. This
    # dictionary will provide all the pieces necessary for testing against GCP

    config = {
        "ENV": "GCP0__GCP_SERVICE_ACCOUNT_KEY",
        "URI_ENV": "GCP0__GCP_STORAGE_URI",
    }
    uri = os.environ.get(config["URI_ENV"])
    if not uri:
        raise Exception(
            f"The environment variable {config['URI_ENV']} is not set. Unable to run "
            "tests using the 'gcp_config' fixture."
        )
    bucket_name = uri.replace("gs://", "").split("/", 1)[0]
    config["BUCKET"] = bucket_name
    yield config


@pytest.fixture
def gcs_client(tmp_path, gcs_config):

    gcs_credentials_path = os.path.join(
        tmp_path, f"gcs_credentials_{uuid.uuid4().hex[:16]}.json"
    )
    with open(gcs_credentials_path, "w", encoding="utf-8") as f:
        if not os.environ.get(gcs_config["ENV"]):
            raise Exception(
                "The environment variable "
                f"{gcs_config['ENV']} is not set. Unable to run "
                "tests using the 'gcp_client' fixture."
            )
        f.write(os.environ.get(gcs_config["ENV"]))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcs_credentials_path
    yield storage.Client()
    if os.path.exists(gcs_credentials_path):
        os.remove(gcs_credentials_path)
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
