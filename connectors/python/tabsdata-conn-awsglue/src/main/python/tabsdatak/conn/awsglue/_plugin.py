#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from typing import Any

import polars as pl

from tabsdatak.conn.awsglue import (
    AwsGlueDest,
    AwsGlueDestConn,
    DestCfgKey,
    DestCfgSpec,
)
from tabsdatak.conn.awsglue._explorer import AwsGlueConnectionExplorer
from tabsdatak.conn.awsglue.error import AwsGlueErrorCode
from tabsdatak.conn.common._dest import present
from tabsdatak.conn.common.error import ConnException
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
)

logger = logging.getLogger(__name__)

_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "awsglue.logging_level"

_DEST_CFGS_DEFAULTS: dict[DestCfgKey, Any] = {
    _KEY_DEST_LOGGING_LEVEL: logging.ERROR,
}


def _logging_level(dest_cfg: DestCfgSpec | None) -> int:
    cfg = dest_cfg or {}
    return int(
        cfg.get(_KEY_DEST_LOGGING_LEVEL, _DEST_CFGS_DEFAULTS[_KEY_DEST_LOGGING_LEVEL])
    )


class AwsGlueDestPlugin(
    DestPlugin[AwsGlueDestConn, AwsGlueDest, list[pl.LazyFrame | None]],
):
    def write_out(
        self,
        ctx: DestContext,
        conn: AwsGlueDestConn,
        dest: AwsGlueDest,
        tables: list[pl.LazyFrame | None],
    ) -> None:
        # noinspection PyProtectedMember
        from tabsdatak.conn.awsglue import _impl

        level = _logging_level(dest.dest_cfg)
        logger.setLevel(level)
        logging.getLogger(_impl.__name__).setLevel(level)
        logging.getLogger("pyiceberg").setLevel(level)

        if len(dest.tables) != len(tables):
            raise AwsGlueErrorCode.AWSGLUE_6.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        frames, names = present(tables, list(dest.tables))
        if not frames:
            logger.info("awsglue: nothing to write, every table is None")
            return
        logger.info("awsglue: write_out starting (%d table(s))", len(names))
        resolution = dest._evolution()
        cfg = _impl.WriteCfg(
            conn=conn,
            tables=names,
            if_table_exists=resolution.if_table_exists,
            schema_evolution=resolution.schema_evolution,
            transaction_id=ctx.transaction_id,
        )
        try:
            _impl.write_all_tables(cfg, frames)
        except ConnException:
            raise
        except Exception as e:  # noqa: BLE001 -- runtime wrapper
            raise AwsGlueErrorCode.AWSGLUE_15.exception(
                cause=e,
                table=", ".join(cfg.tables),
            )
        logger.info("awsglue: write_out finished (%d table(s))", len(names))


AWSGLUE_DEST = DestDef(
    conn=AwsGlueDestConn,
    type_="awsglue-iceberg-out",
    system="AWS Glue",
    dest_version="v1",
    dest=AwsGlueDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=AwsGlueDestPlugin,
    explorer=AwsGlueConnectionExplorer,
    icon="conn/awsglue/resources/awsglue.svg",
)
