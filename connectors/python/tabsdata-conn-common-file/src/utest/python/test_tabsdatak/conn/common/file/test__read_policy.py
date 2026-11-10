#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._avro import Avro

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._csv import AutoCsv

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._format import Format

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._log import Log

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._ndjson import AutoNdjson

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._policy import Inference, policy

pytestmark = [pytest.mark.unit]


@pytest.mark.parametrize(
    "file_format, chosen",
    [
        (Format.CSV, AutoCsv),
        (Format.NDJSON, AutoNdjson),
        (Format.LOG, Log),
        (Format.AVRO, Avro),
    ],
    ids=["csv", "ndjson", "log", "avro"],
)
def test_auto_inference_picks_the_policy_of_the_format(file_format, chosen):
    assert isinstance(policy(file_format, "a.file", inference=Inference.AUTO), chosen)


def test_the_inference_is_auto_unless_the_source_says_otherwise():
    assert isinstance(policy(Format.CSV, "a.csv"), AutoCsv)


def test_a_format_without_a_policy_is_refused():
    with pytest.raises(ValueError, match="'xml' has no policy"):
        policy("xml", "a.xml")
