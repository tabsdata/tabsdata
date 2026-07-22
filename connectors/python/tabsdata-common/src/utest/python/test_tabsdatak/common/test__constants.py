#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.common import _constants as c


def test_module_names_are_strings():
    assert c.TABSDATA_MODULE_NAME == "tabsdata"
    assert c.TABSDATAK_MODULE_NAME == "tabsdatak"
    assert c.POLARS_MODULE_NAME == "polars"


def test_truthy_values_are_lowercase_set():
    assert c.TRUE_VALUES == {"1", "true", "yes", "y", "on"}
    assert c.FALSE_VALUES == {"0", "false", "no", "n", "off"}
    assert c.TRUE_VALUES.isdisjoint(c.FALSE_VALUES)


def test_connectors_table_keys_are_dashed_package_names():
    assert "tabsdata-api" in c.TABSDATA_CONNECTORS
    assert "tabsdata-conn-mysql" in c.TABSDATA_CONNECTORS
    # Every entry has a dev-env-var name; mysql's is the canonical
    # upper-case + dash→underscore translation.
    assert (
        c.TABSDATA_CONNECTORS["tabsdata-conn-mysql"]["is_dev_env"]
        == "TD_TABSDATA_CONN_MYSQL_DEV_PKG"
    )


def test_packages_list_contains_known_members():
    assert c.TABSDATA_MODULE_NAME in c.TABSDATA_PACKAGES
    assert c.TABSDATA_TOOLKIT_PACKAGE_NAME in c.TABSDATA_PACKAGES
    assert c.TABSDATA_AGENT_PACKAGE_NAME in c.TABSDATA_PACKAGES
    assert "tabsdata-api" in c.TABSDATA_PACKAGES


def test_path_constants_are_well_formed():
    assert c.FILE_PROTOCOL == "file://"
    assert c.SLASH == "/"
    assert c.BACK_SLASH == "\\"
    assert c.WHEEL_EXTENSION == ".whl"
    assert c.SRC_MAIN == ("src", "main")


def test_copyright_header_template_has_year_placeholder():
    assert "{}" in c.COPYRIGHT_HEADER
    formatted = c.COPYRIGHT_HEADER.format(2026)
    assert "Copyright 2026" in formatted
