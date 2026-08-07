#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.common import constants as _conn_constants
from tabsdatak.conn.common.file import constants as _constants


def test_file_metadata_name():
    assert _constants.FILE_META_PATH == "@td.file.path"


def test_file_metadata_name_carries_the_connector_column_prefix():
    assert _constants.FILE_META_PATH.startswith(
        _conn_constants.TD_CONNECTOR_COLUMN_PREFIX
    )
