#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._select import WRAPPERS

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._source import EXTENSIONS
from tabsdatak.conn.gcs import GCSSrc

pytestmark = [pytest.mark.unit]


def test_the_help_names_every_format_codec_and_archive_extension():
    named = set(re.findall(r"`(\.[a-z0-9]+)`", GCSSrc.__doc__ or ""))
    assert named == set(EXTENSIONS) | set(WRAPPERS)
