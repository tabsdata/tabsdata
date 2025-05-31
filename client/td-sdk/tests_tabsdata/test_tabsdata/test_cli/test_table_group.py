#
# Copyright 2024 Tabs Data Inc.
#

import logging
import os
import time

import polars as pl
import pytest
from click.testing import CliRunner

from tabsdata.cli.cli import cli

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_wrong_command_raises_exception(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "table", "rex"])
    assert result.exit_code == 2


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_download(login, testing_collection_with_table, tmp_path):
    destination_file = os.path.join(tmp_path, "test_table_download_cli.parquet")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "download",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_list(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--no-prompt", "table", "list", "--collection", testing_collection_with_table],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_file_destination(login, testing_collection_with_table, tmp_path):
    runner = CliRunner()
    destination_file = os.path.join(tmp_path, "test_table_sample_cli.ndjson")
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination_file)
    assert isinstance(pl.read_ndjson(destination_file), pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_schema(login, testing_collection_with_table):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "schema",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_download_at(login, testing_collection_with_table, tmp_path):
    destination_file = os.path.join(tmp_path, "test_table_download_at_cli.parquet")
    epoch_ms = int(time.time() * 1000)  # Current time in milliseconds
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "download",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
            "--at",
            str(epoch_ms),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_at(login, testing_collection_with_table):
    runner = CliRunner()
    epoch_ms = int(time.time() * 1000)  # Current time in milliseconds
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--at",
            str(epoch_ms),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_sample_file_destination_at(
    login, testing_collection_with_table, tmp_path
):
    runner = CliRunner()
    destination_file = os.path.join(tmp_path, "test_table_sample_at_cli.ndjson")
    epoch_ms = int(time.time() * 1000)  # Current time in milliseconds
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "sample",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--file",
            destination_file,
            "--at",
            str(epoch_ms),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert os.path.exists(destination_file)
    assert isinstance(pl.read_ndjson(destination_file), pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
def test_table_schema_at(login, testing_collection_with_table):
    runner = CliRunner()
    epoch_ms = int(time.time() * 1000)  # Current time in milliseconds
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "table",
            "schema",
            "--collection",
            testing_collection_with_table,
            "--name",
            "output",
            "--at",
            str(epoch_ms),
        ],
    )
    logger.debug(result.output)
    assert result.exit_code == 0
