#
# Copyright 2026 Tabsdata Inc.
#

import logging

PINNED = logging.getLogger("tabsdatak.conn.example")


def test_a_plugin_run_pins_its_logger():
    PINNED.setLevel(logging.ERROR)


def test_a_later_test_still_sees_its_warnings(caplog):
    PINNED.warning("staged files left behind")
    assert "staged files left behind" in caplog.text
