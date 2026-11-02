#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Publisher scheduled daily with a CronTrigger
kind: publisher
connectors: [postgres-sql-in]
tags: [trigger, cron, schedule]
summary: >-
  Snapshots yesterday's sales every day at 06:00 UTC. CronTrigger takes a
  five-field cron expression evaluated in UTC, and the query takes its day
  in UTC too, whatever the database session's time zone; without a trigger a
  publisher only runs when started manually.
"""

from tabsdatak.api import CronTrigger, TableFrameSpec, publisher
from tabsdatak.conn.postgres import PostgresSrc


@publisher(
    source=PostgresSrc(
        queries=[
            "SELECT * FROM public.sales "
            "WHERE sold_at >= CAST(now() AT TIME ZONE 'utc' AS date) - 1 "
            "AND sold_at < CAST(now() AT TIME ZONE 'utc' AS date)"
        ],
    ),
    output_tables=["daily_sales"],
    trigger_by=CronTrigger(mask="0 6 * * *"),
)
def pub_cron_daily(sales: TableFrameSpec) -> TableFrameSpec:
    return sales
