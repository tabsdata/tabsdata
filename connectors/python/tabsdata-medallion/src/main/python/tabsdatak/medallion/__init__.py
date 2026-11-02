#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.medallion._bronze.append_only_cdc import build_append_only_cdc_bronze
from tabsdatak.medallion._bronze.cdc import (
    OP_DELETE,
    OP_INSERT,
    OP_LOAD,
    OP_TOMBSTONE,
    OP_UPDATE,
    build_cdc_bronze,
)
from tabsdatak.medallion._bronze.period_snapshot import build_period_snapshot_bronze
from tabsdatak.medallion._bronze.snapshot_diff_cdc import build_snapshot_diff_cdc_bronze
from tabsdatak.medallion._bronze.upd_cdc import build_upd_cdc_bronze
from tabsdatak.medallion._constants import (
    MEDALLION_SYSTEM_COLUMN_BRONZE_TS,
    MEDALLION_SYSTEM_COLUMN_DATE_SK,
    MEDALLION_SYSTEM_COLUMN_ENTITY_SK,
    MEDALLION_SYSTEM_COLUMN_GOLD_TS,
    MEDALLION_SYSTEM_COLUMN_OP,
    MEDALLION_SYSTEM_COLUMN_PERIOD,
    MEDALLION_SYSTEM_COLUMN_PK,
    MEDALLION_SYSTEM_COLUMN_PREFIX,
    MEDALLION_SYSTEM_COLUMN_SILVER_TS,
    MEDALLION_SYSTEM_COLUMN_SOURCE_TS,
    MEDALLION_SYSTEM_COLUMN_VALID_FROM,
    MEDALLION_SYSTEM_COLUMN_VALID_TO,
    MEDALLION_SYSTEM_COLUMN_VERSION_SK,
)
from tabsdatak.medallion._gold.dim import build_type1_dim, build_type2_dim
from tabsdatak.medallion._gold.fact import build_fact
from tabsdatak.medallion._silver.append_only import build_append_only_silver
from tabsdatak.medallion._silver.periodic_snapshot import build_periodic_snapshot_silver
from tabsdatak.medallion._silver.scd import (
    build_scd1_silver,
    build_scd1_silver_cleanse,
    build_scd2_silver,
    build_scd2_silver_cleanse,
)

__all__ = [
    "MEDALLION_SYSTEM_COLUMN_BRONZE_TS",
    "MEDALLION_SYSTEM_COLUMN_DATE_SK",
    "MEDALLION_SYSTEM_COLUMN_ENTITY_SK",
    "MEDALLION_SYSTEM_COLUMN_GOLD_TS",
    "MEDALLION_SYSTEM_COLUMN_OP",
    "MEDALLION_SYSTEM_COLUMN_PERIOD",
    "MEDALLION_SYSTEM_COLUMN_PK",
    "MEDALLION_SYSTEM_COLUMN_PREFIX",
    "MEDALLION_SYSTEM_COLUMN_SILVER_TS",
    "MEDALLION_SYSTEM_COLUMN_SOURCE_TS",
    "MEDALLION_SYSTEM_COLUMN_VALID_FROM",
    "MEDALLION_SYSTEM_COLUMN_VALID_TO",
    "MEDALLION_SYSTEM_COLUMN_VERSION_SK",
    "OP_DELETE",
    "OP_INSERT",
    "OP_LOAD",
    "OP_TOMBSTONE",
    "OP_UPDATE",
    "build_append_only_cdc_bronze",
    "build_append_only_silver",
    "build_cdc_bronze",
    "build_fact",
    "build_period_snapshot_bronze",
    "build_periodic_snapshot_silver",
    "build_scd1_silver",
    "build_scd1_silver_cleanse",
    "build_scd2_silver",
    "build_scd2_silver_cleanse",
    "build_snapshot_diff_cdc_bronze",
    "build_type1_dim",
    "build_type2_dim",
    "build_upd_cdc_bronze",
]
