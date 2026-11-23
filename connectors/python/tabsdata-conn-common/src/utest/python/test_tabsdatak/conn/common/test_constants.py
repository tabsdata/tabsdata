#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.common import constants as _constants


def test_meta_column_prefix():
    assert _constants.TD_META_COLUMN_PREFIX == "@td."


def test_src_metadata_drop_key():
    assert _constants.SRC_META_DROP_KEY == "tabsdata.src_metadata.drop"
