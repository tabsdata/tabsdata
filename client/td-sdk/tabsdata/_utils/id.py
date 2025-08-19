#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import base64
import datetime
import logging
import uuid

from uuid6 import UUID, uuid7

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def encode_id(debug: bool | None = False) -> tuple[UUID, str]:
    id_uuid = uuid7()
    id_code = base64.b32hexencode(id_uuid.bytes).decode("ascii")[:26]
    if debug:
        logger.debug(f"id uuid.....: {id_uuid}")
        logger.debug(f"id code.....: {id_code}")
    return id_uuid, id_code


def decode_id(id_code: str, debug: bool | None = False) -> tuple[
    UUID,
    int,
    datetime.datetime,
]:
    id_uuid = UUID(
        bytes=base64.b32hexdecode(id_code + "=" * ((8 - len(id_code) % 8) % 8)),
    )
    id_timestamp, id_datetime = extract_time_from_uuidv7(id_uuid)
    if debug:
        logger.debug(f"id code.....: {id_code}")
        logger.debug(f"id uuid.....: {id_uuid}")
        logger.debug(f"id timestamp: {id_timestamp}")
        logger.debug(f"id timestamp: {id_datetime.isoformat(timespec='milliseconds')}")
    return id_uuid, id_timestamp, id_datetime


def extract_time_from_uuidv7(uuid_v7: uuid.UUID) -> tuple[int, datetime.datetime]:
    timestamp_ms = int.from_bytes(uuid_v7.bytes[:6], byteorder="big")
    datetime_ms = datetime.datetime.fromtimestamp(
        timestamp_ms / 1000,
        tz=datetime.timezone.utc,
    )
    return timestamp_ms, datetime_ms
