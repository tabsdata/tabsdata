#
# Copyright 2025 Tabs Data Inc.
#

import importlib
import inspect
import logging
import os
from time import sleep

import pytest
from click.testing import CliRunner
from tests_tabsdata.conftest import LOCAL_PACKAGES_LIST, MAXIMUM_RETRY_COUNT

from tabsdata import TabsdataFunction
from tabsdata.cli.cli import cli
from tabsdata.extensions.tableframe.extension_test import instance as checker

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

WORKING_FOLDER = os.environ.get("TDX")


def import_all_tabsdatafunctions(module_name):
    module = importlib.import_module(module_name)
    tabsdatafunctions = {
        name: obj
        for name, obj in inspect.getmembers(
            module, lambda x: isinstance(x, TabsdataFunction)
        )
    }
    return tabsdatafunctions


@pytest.mark.integration
@pytest.mark.requires_internet
def test_examples(login, tabsserver_connection):

    import tabsdata

    print(f"tabsdata namespace locations: {tabsdata.__path__}")
    import tabsdata.extensions

    print(f"tabsdata.extensions namespace locations: {tabsdata.extensions.__path__}")
    import tabsdata.extensions.tableframe

    print(
        "tabsdata.extensions.tableframe namespace locations: "
        f"{tabsdata.extensions.tableframe.__path__}"
    )
    import tabsdata.extensions.tableframe.extension

    print(
        "tabsdata.extensions.tableframe.extension: "
        f"{tabsdata.extensions.tableframe.extension.__file__}"
    )
    import tabsdata.extensions.tableframe.extension

    print(
        "tabsdata.extensions.tableframe.extension_test: "
        f"{tabsdata.extensions.tableframe.extension_test.__file__}"
    )

    assert WORKING_FOLDER
    output_folder = os.path.join(WORKING_FOLDER, "output")
    logger.debug(f"Working folder: {WORKING_FOLDER}")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["example", "--dir", WORKING_FOLDER],
    )
    log_and_assert(result)
    assert os.path.exists(WORKING_FOLDER)
    assert os.path.exists(os.path.join(WORKING_FOLDER, "input", "persons.csv"))
    result = runner.invoke(
        cli, ["collection", "create", "examples", "--description", '"Examples"']
    )
    log_and_assert(result)
    # Test the publisher
    result = runner.invoke(
        cli,
        [
            "fn",
            "register",
            "--collection",
            "examples",
            "--path",
            f"{os.path.join(WORKING_FOLDER, "publisher.py")}::pub",
            *[arg for path in LOCAL_PACKAGES_LIST for arg in ("--local-pkg", path)],
        ],
    )
    log_and_assert(result)
    result = runner.invoke(
        cli, ["fn", "trigger", "--collection", "examples", "--name", "pub"]
    )
    log_and_assert(result)
    result = runner.invoke(cli, ["exec", "list-trx"])
    log_and_assert(result)
    transactions = tabsserver_connection.transactions
    logger.debug(transactions)
    assert transactions
    retry = 0
    while retry < MAXIMUM_RETRY_COUNT:
        sleep(retry)
        result = runner.invoke(
            cli,
            ["table", "schema", "--collection", "examples", "--name", "persons"],
        )
        logger.debug(result.output)
        if result.exit_code == 0:
            break
        retry += 1
    result = runner.invoke(
        cli,
        ["table", "schema", "--collection", "examples", "--name", "persons"],
    )
    log_and_assert(result)

    # Test the transformer
    result = runner.invoke(
        cli,
        [
            "fn",
            "register",
            "--collection",
            "examples",
            "--path",
            f"{os.path.join(WORKING_FOLDER, "transformer.py")}::tfr",
            *[arg for path in LOCAL_PACKAGES_LIST for arg in ("--local-pkg", path)],
        ],
    )
    log_and_assert(result)
    result = runner.invoke(
        cli, ["fn", "trigger", "--collection", "examples", "--name", "tfr"]
    )
    log_and_assert(result)
    result = runner.invoke(cli, ["exec", "list-trx"])
    log_and_assert(result)
    transactions = tabsserver_connection.transactions
    logger.debug(transactions)
    assert transactions
    retry = 0
    while retry < MAXIMUM_RETRY_COUNT:
        sleep(retry)
        result = runner.invoke(
            cli,
            ["table", "schema", "--collection", "examples", "--name", "spanish"],
        )
        logger.debug(result.output)
        if result.exit_code == 0:
            break
        retry += 1
    result = runner.invoke(
        cli,
        ["table", "schema", "--collection", "examples", "--name", "spanish"],
    )
    log_and_assert(result)

    # Test the subscriber
    result = runner.invoke(
        cli,
        [
            "fn",
            "register",
            "--collection",
            "examples",
            "--path",
            f"{os.path.join(WORKING_FOLDER, "subscriber.py")}::sub",
            *[arg for path in LOCAL_PACKAGES_LIST for arg in ("--local-pkg", path)],
        ],
    )
    log_and_assert(result)
    result = runner.invoke(
        cli, ["fn", "trigger", "--collection", "examples", "--name", "sub"]
    )
    log_and_assert(result)
    result = runner.invoke(cli, ["exec", "list-trx"])
    log_and_assert(result)
    transactions = tabsserver_connection.transactions
    logger.debug(transactions)
    assert transactions
    retry = 0
    while retry < MAXIMUM_RETRY_COUNT:
        if os.path.exists(
            os.path.join(output_folder, "spanish.jsonl")
        ) and os.path.exists(os.path.join(output_folder, "french.jsonl")):
            break
        sleep(retry)
        retry += 1

    # See the files were exported correctly
    assert os.path.exists(os.path.join(output_folder, "spanish.jsonl"))
    assert os.path.exists(os.path.join(output_folder, "french.jsonl"))

    # Clean up
    for file in os.listdir(output_folder):
        os.remove(os.path.join(output_folder, file))
    assert not os.path.exists(os.path.join(output_folder, "spanish.jsonl"))
    assert not os.path.exists(os.path.join(output_folder, "french.jsonl"))

    # Multitrigger
    result = runner.invoke(
        cli, ["fn", "trigger", "--collection", "examples", "--name", "pub"]
    )
    log_and_assert(result)
    result = runner.invoke(cli, ["exec", "list-trx"])
    log_and_assert(result)
    transactions = tabsserver_connection.transactions
    logger.debug(transactions)
    assert transactions
    retry_check_test_examples(output_folder)


def log_and_assert(result):
    logger.debug(result.output)
    assert result.exit_code == 0


def retry_check_test_examples(output_folder):
    retry = 0
    while retry < MAXIMUM_RETRY_COUNT:
        sleep(retry)
        retry += 1
        try:
            checker.check_test_examples(output_folder)
            break
        except AssertionError:
            pass
    checker.check_test_examples(output_folder)
