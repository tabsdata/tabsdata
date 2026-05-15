
from __future__ import annotations

import pytest

from tabsdatak.api import Dest, Src
from tabsdatak._connectors import (
    avail_auto_srcs,
    avail_dests,
    avail_srcs,
    get_dest,
    get_src,
    reload_connectors,
)
from tabsdatak.spi import (
    AutoSrcDef,
    DestDef,
    DestPlugin,
    Explorer,
    SrcDef,
    SrcPlugin,
    TableMode,
)


WORKSPACE_SRC_TYPES = {
    "azureBlob",
    # "databricks",
    "db2",
    "gcs",
    "localfile",
    "mssql",
    "mysql",
    "oracle",
    "postgres",
    "s3",
    # "salesforce",
    # "salesforce_report",
}

WORKSPACE_DEST_TYPES = {
    "azureBlob",
    # "bigquery",
    # "clickhouse",
    # "databricks",
    # "druid",
    "gcs",
    "localfile",
#    "mongodb",
    "mssql",
    "mysql",
    "oracle",
    "postgres",
    "s3",
    # "snowflake",
    # "starrocks",
}


def test_registry_includes_every_workspace_src():
    missing = WORKSPACE_SRC_TYPES - set(avail_srcs())
    assert not missing, f"src types not found: {sorted(missing)}"


def test_registry_includes_every_workspace_dest():
    missing = WORKSPACE_DEST_TYPES - set(avail_dests())
    assert not missing, f"dest types not found: {sorted(missing)}"


def test_src_entries_are_well_typed():
    for src_type, defx in avail_srcs().items():
        assert isinstance(defx, SrcDef), src_type
        assert defx.src_type == src_type
        assert issubclass(defx.src, Src), src_type
        assert isinstance(defx.table_mode, TableMode), src_type
        assert callable(defx.cardinality), src_type
        assert issubclass(defx.plugin, SrcPlugin), src_type
        assert defx.explorer is None or issubclass(defx.explorer, Explorer)


def test_dest_entries_are_well_typed():
    for dest_type, defx in avail_dests().items():
        assert isinstance(defx, DestDef), dest_type
        assert defx.dest_type == dest_type
        assert issubclass(defx.dest, Dest), dest_type
        assert callable(defx.cardinality), dest_type
        assert issubclass(defx.plugin, DestPlugin), dest_type
        assert defx.explorer is None or issubclass(defx.explorer, Explorer)


def test_auto_src_entries_are_well_typed():
    for src_type, defx in avail_auto_srcs().items():
        assert isinstance(defx, AutoSrcDef), src_type


def test_avail_returns_a_fresh_dict():
    # Mutating the returned dict must not poison the cached registry.
    snapshot = avail_srcs()
    snapshot["bogus"] = None  # type: ignore[assignment]
    assert "bogus" not in avail_srcs()


@pytest.mark.parametrize(
    "src_type,expected_mode",
    [
        ("mysql",             "SINGLE"),
        ("postgres",          "SINGLE"),
        ("oracle",            "SINGLE"),
        ("mssql",             "SINGLE"),
        ("db2",               "SINGLE"),
        # ("databricks",        "SINGLE"),
        # ("salesforce",        "SINGLE"),
        # ("salesforce_report", "SINGLE"),
        ("localfile",         "ARRAY"),
        ("s3",                "ARRAY"),
        ("gcs",               "ARRAY"),
        ("azureBlob",         "ARRAY"),
    ],
)
def test_src_table_modes_are_as_expected(src_type, expected_mode):
    assert avail_srcs()[src_type].table_mode.name == expected_mode


@pytest.mark.parametrize(
    "src_type,cls_name",
    [
        ("mysql", "MySQLSrc"),
        ("postgres", "PostgresSrc"),
        # ("salesforce", "SalesforceSrc"),
        # ("salesforce_report", "SalesforceReportSrc"),
    ],
)
def test_src_class_names_match(src_type, cls_name):
    assert avail_srcs()[src_type].src.__name__ == cls_name


# @pytest.mark.parametrize(
#     "src_type",
#     ["snowflake", "bigquery", "clickhouse", "druid", "starrocks", "mongodb"],
# )
# def test_dest_only_connectors_have_no_src(src_type):
#     assert src_type not in avail_srcs()
#     assert src_type in avail_dests()


# @pytest.mark.parametrize(
#     "src_type,dest_excluded",
#     [("db2", "db2"),
#      ("salesforce", "salesforce")
#      ],
# )
# def test_src_only_connectors_have_no_dest(src_type, dest_excluded):
#     assert src_type in avail_srcs()
#     assert dest_excluded not in avail_dests()


# ---------------------------------------------------------------------------
# get_src / get_dest by class
# ---------------------------------------------------------------------------


def test_get_src_resolves_class_to_defx():
    from tabsdatak.conn.mysql import MySQLSrc
    defx = get_src(MySQLSrc)
    assert defx.src is MySQLSrc
    assert defx.src_type == "mysql"


def test_get_dest_resolves_class_to_defx():
    # ``snowflake`` is one of the connectors still on the legacy
    # private-error-code surface; substitute any migrated dest.
    from tabsdatak.conn.s3 import S3Dest
    defx = get_dest(S3Dest)
    assert defx.dest is S3Dest
    assert defx.dest_type == "s3"


def test_get_src_unregistered_class_raises_keyerror():
    class NotRegistered(Src):
        pass

    with pytest.raises(KeyError):
        get_src(NotRegistered)


# ---------------------------------------------------------------------------
# Reload behaviour.
# ---------------------------------------------------------------------------


def test_reload_rebuilds_registry():
    # reload_connectors discards the cached registries and rebuilds.
    # After reload, every workspace src/dest must still be present.
    reload_connectors()
    assert WORKSPACE_SRC_TYPES.issubset(avail_srcs())
    assert WORKSPACE_DEST_TYPES.issubset(avail_dests())


# @pytest.mark.xfail(
#     reason=(
#         "unmigrated connectors (snowflake, databricks, druid, "
#         "clickhouse, starrocks, mongodb, salesforce, bigquery) still "
#         "import the legacy ``_ErrorCode`` symbol and fail to load at "
#         "entry-point scan time, emitting RuntimeWarning. Re-enable "
#         "once those connectors are migrated to the public ``ErrorCode`` "
#         "surface."
#     ),
#     strict=True,
# )
# def test_lazy_loading_does_not_warn_under_normal_conditions():
#     import warnings as _warnings
#
#     with _warnings.catch_warnings():
#         _warnings.simplefilter("error", RuntimeWarning)
#         reload_connectors()
#         assert WORKSPACE_SRC_TYPES.issubset(avail_srcs())
#         assert WORKSPACE_DEST_TYPES.issubset(avail_dests())
