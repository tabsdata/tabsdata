#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatak._medallion._silver._scd import _strip_bronze_metadata
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import concat


def build_periodic_snapshot_silver(
    bronze: list[TableFrame],
) -> TableFrame:
    """
    Periodic snapshot silver — pure projection: drops bronze metadata from the
    input.

    Args:
        bronze: periodic snapshot bronze commits.

    Returns:
        Cleansed periodic snapshot silver TableFrame.
    """
    return _strip_bronze_metadata(concat(bronze, how="diagonal_relaxed"))
