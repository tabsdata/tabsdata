#
# Copyright 2024 Tabs Data Inc.
#

import logging
import os

import pytest
from click.testing import CliRunner
from filelock import FileLock

from tabsdata.cli.cli import cli
from tabsdata.utils.tableframe._generators import _id
from tests.conftest import ABSOLUTE_TEST_FOLDER_LOCATION, API_SERVER_URL

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.getLogger("filelock").setLevel(logging.INFO)


@pytest.fixture(scope="module")
def login(tmp_path_factory, worker_id):
    if worker_id == "master":
        # not executing in with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        yield atomic_login()
    else:
        # get the temp directory shared by all workers
        root_tmp_dir = tmp_path_factory.getbasetemp().parent

        fn = root_tmp_dir / "cli_login"
        with FileLock(str(fn) + ".lock"):
            # only one worker will be able to create the database
            yield atomic_login()


def atomic_login():
    runner = CliRunner()
    result = runner.invoke(
        cli, ["login", API_SERVER_URL, "--user", "admin"], input="tabsdata\n"
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.fixture(scope="function")
def testing_collection(login, worker_id):
    runner = CliRunner()
    # current_time = time.time()
    # random.seed(current_time)
    # random_id = random.randint(0, 1000)
    random_id = _id()
    collection_name = f"testing_collection_{worker_id}_{random_id}"
    runner.invoke(
        cli,
        [
            "collection",
            "create",
            collection_name,
            "--description",
            "description",
        ],
    )
    try:
        yield collection_name
    finally:
        runner.invoke(
            cli,
            ["collection", "delete", collection_name, "--confirm", "delete"],
        )


@pytest.fixture(scope="module")
def function_path():
    file_path = os.path.join(
        ABSOLUTE_TEST_FOLDER_LOCATION,
        "testing_resources",
        "test_input_plugin",
        "example.py",
    )
    yield file_path + "::input_plugin"
