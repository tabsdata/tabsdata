#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass

from tabsdatak.api import Secret
from tabsdatak.conn.awsglue import AwsGlueDestConn
from tabsdatak.conn.common.types import AwsAccessSecretKey


@dataclass
class FakeCtx:
    transaction_id: str = "trx-1"


def conn(**overrides) -> AwsGlueDestConn:
    kwargs: dict = dict(
        region="us-east-1",
        credentials=AwsAccessSecretKey(
            access_key_id=Secret("AKIA"), secret_access_key=Secret("shh")
        ),
        bucket="my-lake",
        base_path="/warehouse",
        database="sales",
    )
    kwargs.update(overrides)
    return AwsGlueDestConn(**kwargs)
