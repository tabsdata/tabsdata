#
# Copyright 2026 Tabsdata Inc.
#

"""AWS Glue data-type coverage on write."""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.awsglue import AwsGlueDomain, awsglue_domain
from testi_tabsdatak.conn.awsglue.backend import dest_conn, store
from testi_tabsdatak.conn.common.sql.datatype_matrix import (
    DATATYPE_CASES,
    DataTypeCase,
    write_named_datatype_table,
)
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir

# noinspection PyProtectedMember
from tabsdatak.conn.awsglue import AwsGlueDest, _plugin as plugin
from tabsdatak.conn.awsglue.error import AwsGlueErrorCode, AwsGlueRuntimeException

pytestmark = pytest.mark.conn_awsglue

# Iceberg has no type for these; the connector refuses the column by name.
REFUSED = {"time", "duration"}


def _params(refused: bool):
    for case in DATATYPE_CASES:
        if (case.id in REFUSED) == refused:
            # One fixed table per case, so a run leaves nothing unpredictable
            # behind.
            yield pytest.param(case, f"td_dt_{case.id}", id=case.id)


def _write(domain: AwsGlueDomain, case: DataTypeCase, table: str, tmp_path: Path):
    return write_named_datatype_table(
        case,
        table=table,
        dest_plugin=plugin.AwsGlueDestPlugin(),
        make_dest=lambda tables: AwsGlueDest(tables=tables, if_table_exists="replace"),
        dest_conn=dest_conn(domain),
        sql=store(domain),
        work_dir=work_dir(tmp_path),
    )


@pytest.mark.parametrize("case, table", list(_params(refused=False)))
@awsglue_domain("awsglue")
def test_datatype_create_and_load(
    domain_awsglue: AwsGlueDomain,
    case: DataTypeCase,
    table: str,
    tmp_path: Path,
):
    store(domain_awsglue).drop_table(table)
    frame = _write(domain_awsglue, case, table, tmp_path)
    assert frame.height == len(case.values)
    assert frame["c"].null_count() == case.values.count(None)


@pytest.mark.parametrize("case, table", list(_params(refused=True)))
@awsglue_domain("awsglue")
def test_datatype_iceberg_cannot_hold_is_refused(
    domain_awsglue: AwsGlueDomain,
    case: DataTypeCase,
    table: str,
    tmp_path: Path,
):
    with pytest.raises(AwsGlueRuntimeException) as raised:
        _write(domain_awsglue, case, table, tmp_path)
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_12
    assert not store(domain_awsglue).exists(table)
