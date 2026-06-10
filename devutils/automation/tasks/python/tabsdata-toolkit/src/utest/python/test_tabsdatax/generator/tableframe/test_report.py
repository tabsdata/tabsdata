"""Tests for the signature report build + model."""

import polars as pl

from tabsdatax.generator.tableframe.model.model import Report
from tabsdatax.generator.tableframe.scan.report import build_report

ROOT = "tabsdatak.tableframe"


def test_report_records_env_polars_version():
    report, _ = build_report(ROOT)
    assert report.polars_version == pl.__version__
    assert report.root_module == ROOT
    assert len(report.symbols) > 200  # LazyFrame + Expr + namespaces + functions


def test_lazyframe_maps_to_root_as_tableframe():
    report, _ = build_report(ROOT)
    join = report.by_id()["LazyFrame.join"]
    assert join.target_module == ROOT  # __init__.py
    assert join.target_class == "TableFrame"
    assert join.hook_name == "join"  # pre-seeded augmenting hook
    assert join.whitelisted is True
    assert "other" in join.signature


def test_report_seeds_kinds_and_denies_leaky_ops():
    by = build_report(ROOT)[0].by_id()
    assert by["LazyGroupBy.agg"].hook_name == "agg"
    assert by["LazyFrame.select"].hook_name == "project"
    # Schema is now wrapped, so schema-returning ops are no longer leaks
    assert by["LazyFrame.schema"].whitelisted is True
    assert by["functions.select"].whitelisted is False  # returns pl.DataFrame -> leak
    # a no-hook op keeps the default "-" and stays whitelisted
    assert (
        by["LazyFrame.sort"].hook_name == "-"
        and by["LazyFrame.sort"].whitelisted is True
    )


def test_schema_is_scanned_and_leaky_schema_ops_denied():
    by = build_report(ROOT)[0].by_id()
    # Schema is a wrapped class with a mapping protocol + datatype accessors
    assert by["Schema.names"].target_class == "Schema"
    assert by["Schema.names"].target_module == f"{ROOT}.schema"
    assert (
        by["Schema.dtypes"].whitelisted is True
    )  # returns list[DataType] (passthrough)
    assert (
        "Schema.__len__" in by
        and "Schema.__iter__" in by
        and "Schema.__contains__" in by
    )
    assert by["Schema.to_frame"].whitelisted is False  # returns DataFrame -> leak
    # LazyFrame.__contains__ must NOT be captured (returns bool, breaks the frame path)
    assert "LazyFrame.__contains__" not in by


def test_expr_and_functions_map_to_canonical_submodules():
    by_id = build_report(ROOT)[0].by_id()
    assert by_id["Expr.alias"].target_module == f"{ROOT}.expr"
    assert by_id["Expr.alias"].target_class == "Expr"
    col = by_id["functions.col"]
    assert col.target_module == f"{ROOT}.functions"
    assert col.target_class is None


def test_expr_namespaces_become_flat_modules():
    syms = build_report(ROOT)[0].symbols
    ns_modules = {
        s.target_module
        for s in syms
        if s.target_class and "NameSpace" in s.target_class
    }
    assert f"{ROOT}.string" in ns_modules
    assert f"{ROOT}.datetime" in ns_modules


def test_operator_dunders_captured_on_expr_only():
    syms = build_report(ROOT)[0]
    by = syms.by_id()
    assert "Expr.__mul__" in by
    assert "Expr.__add__" in by
    # namespaces define no operators
    assert not any(
        s.id.endswith(".__mul__") and "NameSpace" in (s.target_class or "")
        for s in syms.symbols
    )


def test_json_roundtrip():
    report, _ = build_report(ROOT)
    restored = Report.from_json(report.to_json())
    assert restored == report


def test_symbols_grouped_by_target_module():
    # symbols for the same module must be contiguous (drives matching .py/.pyi order)
    syms = build_report(ROOT)[0].symbols
    seen: set[str] = set()
    prev = None
    for s in syms:
        if s.target_module != prev:
            assert s.target_module not in seen, f"{s.target_module} is not contiguous"
            seen.add(s.target_module)
            prev = s.target_module
