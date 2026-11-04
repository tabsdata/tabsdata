#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re

import pytest

from tabsdatak.conn.localfile import LocalFileSrc

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._select import CODECS

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._source import EXTENSIONS

pytestmark = [pytest.mark.unit]


def test_the_help_names_every_format_and_codec_extension():
    named = set(re.findall(r"`(\.[a-z0-9]+)`", LocalFileSrc.__doc__ or ""))
    assert named == set(EXTENSIONS) | set(CODECS)
