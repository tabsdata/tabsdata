#
# Copyright 2025 Tabs Data Inc.
#

import datetime

import pytest
import uuid6

from tabsdata._utils.id import decode_id, encode_id, extract_time_from_uuidv7


@pytest.mark.slow
def test_encode_decode_id():
    q_checks = 4_000_000

    last = None

    for _ in range(q_checks):
        id_uuid_encode, id_code_encode = encode_id(debug=False)
        id_timestamp_encode, id_datetime_encode = extract_time_from_uuidv7(
            id_uuid_encode
        )
        id_uuid_decode, id_timestamp_decode, id_datetime_decode = decode_id(
            id_code_encode
        )

        assert isinstance(id_code_encode, str)

        assert isinstance(id_uuid_encode, uuid6.UUID)
        assert isinstance(id_timestamp_encode, int)
        assert isinstance(id_datetime_encode, datetime.datetime)

        assert isinstance(id_uuid_decode, uuid6.UUID)
        assert isinstance(id_timestamp_decode, int)
        assert isinstance(id_datetime_decode, datetime.datetime)

        assert id_datetime_encode.tzinfo is not None
        assert id_datetime_decode.tzinfo is not None

        assert id_datetime_encode == id_datetime_decode

        if last is not None:
            assert (
                id_uuid_encode > last
            ), f"UUID {id_uuid_decode} is not greater than last UUID {last}"
        last = id_uuid_encode
