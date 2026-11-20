#
# Copyright 2026 Tabsdata Inc.
#

import pytest

from tabsdatak._medallion.silver.periodic_snapshot import build_periodic_snapshot_silver
from tabsdatak.tableframe import TableFrame

pytestmark = pytest.mark.medallion


@pytest.mark.parametrize(
    "bronze",
    [
        pytest.param([], id="empty_list"),
        pytest.param(None, id="none"),
        pytest.param([None, None], id="all_none_entries"),
        pytest.param([TableFrame.empty()], id="columnless_entry"),
    ],
)
def test_no_bronze_version_returns_none(bronze):
    assert build_periodic_snapshot_silver(bronze=bronze) is None


def test_no_bronze_version_with_a_head_returns_no_rows_with_the_silver_columns():
    """A `None` version would hand the readers of silver at `@NEW` its
    previous period again."""
    bronze = TableFrame.from_dict({"id": [1], "@td.medallion.bronze_ts": [None]})
    out = build_periodic_snapshot_silver(bronze=[], head=bronze)
    expected = build_periodic_snapshot_silver(bronze=[bronze])
    assert out._to_polars_df().is_empty()
    assert out._user_columns() == expected._user_columns()
