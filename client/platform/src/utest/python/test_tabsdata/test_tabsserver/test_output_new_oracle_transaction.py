#
# Copyright 2026 Tabs Data Inc.
#

import logging
import os
from io import StringIO
from unittest import mock

import polars as pl
import pytest
from fntest import FnError, tdfnrun
from sqlalchemy.exc import NotSupportedError

from test_tabsdata.conftest import (
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
)
from test_tabsdata.test_output_new_oracle_transaction.example import (
    testing_tabsdata_function as output_new_oracle_transaction,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.oracle
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.parametrize("transactional", [True, False])
def test_output_new_oracle_transaction(oracle_config, transactional):
    fn = output_new_oracle_transaction(
        oracle_config.uri,
        credentials=oracle_config.credentials,
        destination_tables=[
            "output_new_oracle_transaction",
            "second_output_new_oracle_transaction",
        ],
        transactional=transactional,
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_oracle_transaction",
        "mock_table.parquet",
    )
    with pytest.raises(FnError) as error:
        tdfnrun(function=fn, input_tables=mock_parquet_table)

    fnrun = error.value.fnrun
    exception = fnrun.exception.as_object()
    assert exception.exception == NotSupportedError.__name__

    output = pl.read_database_uri(
        uri=oracle_config.uri_with_credentials,
        query="SELECT * FROM output_new_oracle_transaction",
    )
    output = clean_polars_df(output)
    assert output.is_empty() if transactional else not output.is_empty()

    output = pl.read_database_uri(
        uri=oracle_config.uri_with_credentials,
        query="SELECT * FROM second_output_new_oracle_transaction",
    )
    output = clean_polars_df(output)
    assert output.is_empty()
