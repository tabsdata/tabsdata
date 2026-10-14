#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for code/.pyi generation."""

import ast

import pytest

from tabsdatax.generator.tableframe.engine.generate import perform
from tabsdatax.generator.tableframe.model.model import Report, Symbol

ROOT = "pkg.wrap"


@pytest.fixture
def main_root(tmp_path):
    # Mirror the Maven source layout the generator expects: the package tree
    # lives under `src/main/python`, so both the test-root derivation (main ->
    # utest) and the namespace-init walk (anchored on the `python` segment)
    # resolve as they do in a real run.
    return tmp_path / "src" / "main" / "python"


def _report():
    return Report(
        polars_version="1.41.0",
        root_module=ROOT,
        symbols=[
            # TableFrame (root / __init__.py)
            Symbol(
                "LazyFrame.filter",
                "polars.lazyframe.frame.LazyFrame.filter",
                ROOT,
                "TableFrame",
                "(self, *predicates)",
                "LazyFrame",
                False,
                hook_name="-",
            ),
            Symbol(
                "LazyFrame.select",
                "polars.lazyframe.frame.LazyFrame.select",
                ROOT,
                "TableFrame",
                "(self, *exprs)",
                "LazyFrame",
                False,
                hook_name="project",
            ),
            Symbol(
                "LazyFrame.with_columns",
                "polars.lazyframe.frame.LazyFrame.with_columns",
                ROOT,
                "TableFrame",
                "(self, *exprs)",
                "LazyFrame",
                False,
                hook_name="project",
            ),
            Symbol(
                "LazyFrame.join",
                "polars.lazyframe.frame.LazyFrame.join",
                ROOT,
                "TableFrame",
                "(self, other)",
                "LazyFrame",
                False,
                hook_name="join",
            ),
            Symbol(
                "LazyFrame.group_by",
                "polars.lazyframe.frame.LazyFrame.group_by",
                ROOT,
                "TableFrame",
                "(self, *by)",
                "LazyGroupBy",
                False,
                hook_name="group_by",
            ),
            Symbol(
                "LazyFrame.drop",
                "polars.lazyframe.frame.LazyFrame.drop",
                ROOT,
                "TableFrame",
                "(self, *columns)",
                "LazyFrame",
                False,
                hook_name="drop",
            ),
            Symbol(
                "LazyFrame.rename",
                "polars.lazyframe.frame.LazyFrame.rename",
                ROOT,
                "TableFrame",
                "(self, mapping)",
                "LazyFrame",
                False,
                hook_name="rename",
            ),
            Symbol(
                "LazyFrame.schema",
                "polars.lazyframe.frame.LazyFrame.schema",
                ROOT,
                "TableFrame",
                "(self)",
                "Schema",
                True,
                hook_name="-",
            ),
            # LazyGroupBy (group_by.py)
            Symbol(
                "LazyGroupBy.agg",
                "polars.lazyframe.group_by.LazyGroupBy.agg",
                f"{ROOT}.group_by",
                "LazyGroupBy",
                "(self, *aggs)",
                "LazyFrame",
                False,
                hook_name="agg",
            ),
            # value class + function
            Symbol(
                "Expr.alias",
                "polars.expr.expr.Expr.alias",
                f"{ROOT}.expr",
                "Expr",
                "(self, name: 'str') -> 'Expr'",
                "Expr",
                False,
            ),
            Symbol(
                "Expr.__mul__",
                "polars.expr.expr.Expr.__mul__",
                f"{ROOT}.expr",
                "Expr",
                "(self, other)",
                "Expr",
                False,
            ),
            Symbol(
                "functions.col",
                "polars.functions.col.Col",
                f"{ROOT}.functions",
                None,
                "(name: 'str') -> 'Expr'",
                "Expr",
                False,
            ),
        ],
    )


def test_all_modules_generated(main_root):
    result = perform(_report(), ROOT, str(main_root))
    base = main_root / "pkg" / "wrap"
    names = {p.name for p in base.glob("*.py")}
    # datatypes.py is always emitted (aliased leaf types), independent of the report
    assert names == {
        "__init__.py",
        "group_by.py",
        "expr.py",
        "functions.py",
        "datatypes.py",
    }
    assert result.deferred == {}


def test_datatypes_module_aliases_passthrough_and_allowlist(main_root):
    perform(_report(), ROOT, str(main_root))
    dt = (main_root / "pkg" / "wrap" / "datatypes.py").read_text()
    assert "import polars as pl" in dt
    assert (
        "from pkg.wrap._core.translate import "
        "register_passthrough, register_type_validator"
    ) in dt
    assert "Int64 = pl.Int64" in dt and "String = pl.String" in dt
    assert "DataType = pl.DataType" in dt
    assert "ColumnDataType: TypeAlias = DataType | type[DataType]" in dt
    # editable allow-list seeded with every alias, plus the validator + registration
    assert "_ALLOWED_TYPES = [" in dt and "    Int64," in dt
    assert "def _valid_type(dtype):" in dt
    assert "register_passthrough(pl.DataType)" in dt
    assert "register_type_validator(_valid_type)" in dt
    # the __init__ loop imports it so passthrough/validator register on
    # `import pkg.wrap`
    init = (main_root / "pkg" / "wrap" / "__init__.py").read_text()
    assert '"datatypes"' in init


def test_tableframe_has_hooks_and_augmenting_ops(main_root):
    perform(_report(), ROOT, str(main_root))
    text = (main_root / "pkg" / "wrap" / "__init__.py").read_text()  # ruff-formatted
    assert "class TableFrame(EngineFrame):" in text
    # every op routes through compute (no policy in generated code)
    assert "from pkg.wrap._core.hooks import compute" in text
    # augmenting ops route through compute with their params + call thunk
    assert "def join(" in text and 'how="inner"' in text
    assert '"join",\n            (self, other),' in text
    assert "call=lambda left, right: left.join(" in text
    assert "def select(self, *exprs, **named_exprs):" in text
    assert '"replace_all": True' in text  # select replaces
    assert '"select",\n            (self,),' in text
    assert "def with_columns(self, *exprs, **named_exprs):" in text
    assert (
        "return LazyGroupBy(self._lf.group_by(*u_by, **u_named), self, (u_by, u_named))"
        in text
    )
    # transparent LazyFrame op: routed through compute with empty params
    assert '"filter", (self,), {}, ' in text
    assert "call=lambda lf: lf.filter(*u_args, **u_kwargs)" in text
    # property reads (wrapped) underlying frame
    assert "return wrap(self._lf.schema)" in text


def test_drop_and_rename_dispatch_hooks(main_root):
    perform(_report(), ROOT, str(main_root))
    init = (main_root / "pkg" / "wrap" / "__init__.py").read_text()
    assert "def drop(self, *columns, **kwargs):" in init
    assert '"drop",\n            (self,),\n            {"columns": u_columns},' in init
    assert "call=lambda lf: lf.drop(*u_columns, **u_kwargs)" in init
    assert "def rename(self, mapping, *args, **kwargs):" in init
    assert '"rename",\n            (self,),\n' in init
    assert '{"mapping": u_mapping},' in init
    assert "call=lambda lf: lf.rename(u_mapping, *u_args, **u_kwargs)" in init


def test_lazygroupby_agg(main_root):
    perform(_report(), ROOT, str(main_root))
    text = (main_root / "pkg" / "wrap" / "group_by.py").read_text()
    assert "class LazyGroupBy:" in text
    assert "def agg(self, *aggs, **named_aggs):" in text
    assert "by, named_by = self._by" in text
    assert '"agg",\n            (self._parent,),' in text
    assert "call=lambda *_: self._lgb.agg(*u_args, **u_named)" in text


def test_value_class_and_function_still_transparent(main_root):
    perform(_report(), ROOT, str(main_root))
    expr = (main_root / "pkg" / "wrap" / "expr.py").read_text()
    assert "@register(_PlExpr)" in expr
    assert "return wrap(self._inner.alias(*unwrap(args), **unwrap(kwargs)))" in expr
    funcs = (main_root / "pkg" / "wrap" / "functions.py").read_text()
    assert "return wrap(pl.col(*unwrap(args), **unwrap(kwargs)))" in funcs


def test_generates_core_runtime(main_root):
    perform(_report(), ROOT, str(main_root))
    core = main_root / "pkg" / "wrap" / "_core"
    assert {p.name for p in core.glob("*.py")} == {
        "base.py",
        "translate.py",
        "hooks.py",
    }
    hooks = (core / "hooks.py").read_text()
    assert "from pkg.wrap import TableFrame" in hooks  # root module substituted
    assert "__ROOT_MODULE__" not in hooks
    assert (core / "base.py").read_text().splitlines()[0].startswith("# Copyright ")


def test_hooks_module_has_per_op_typed_params(main_root):
    perform(_report(), ROOT, str(main_root))
    hooks = (main_root / "pkg" / "wrap" / "_core" / "hooks.py").read_text()
    assert "__HOOK_TYPES__" not in hooks  # placeholder substituted
    # one TypedDict per distinct hook_name (project shared by select/with_columns)
    assert "class JoinParams(TypedDict):" in hooks
    assert "class ProjectParams(TypedDict):" in hooks
    assert "class AggParams(TypedDict):" in hooks
    # join params carry the real keys
    for field_name in ("on:", "how: str", "left_on:", "right_on:", "suffix: str"):
        assert field_name in hooks
    # the four phase-hook aliases + the compute interceptor + FrameView
    for symbol in ("BeforeHook", "DuringHook", "AfterHook", "StateHook"):
        assert symbol in hooks
    assert "def compute(" in hooks
    assert "class FrameView:" in hooks


def test_hooks_module_types_the_params_of_engine_frame_ops(main_root):
    perform(_report(), ROOT, str(main_root))
    hooks = (main_root / "pkg" / "wrap" / "_core" / "hooks.py").read_text()
    assert "class UdfParams(TypedDict):" in hooks
    assert "inputs: tuple" in hooks
    assert "outputs: dict" in hooks


def test_multiple_classes_in_one_module(main_root):
    # a module may hold several wrapper classes (e.g. when/then builders)
    report = Report(
        polars_version="1.41.0",
        root_module=ROOT,
        symbols=[
            Symbol(
                "When.then",
                "polars.expr.whenthen.When.then",
                f"{ROOT}.whenthen",
                "When",
                "(self, x)",
                "Then",
                False,
            ),
            Symbol(
                "Then.otherwise",
                "polars.expr.whenthen.Then.otherwise",
                f"{ROOT}.whenthen",
                "Then",
                "(self, x)",
                "Expr",
                False,
            ),
        ],
    )
    perform(report, ROOT, str(main_root))
    text = (main_root / "pkg" / "wrap" / "whenthen.py").read_text()
    assert "class When(BaseWrapper):" in text
    assert "class Then(BaseWrapper):" in text
    assert "@register(_PlWhen)" in text and "@register(_PlThen)" in text


def test_operator_dunder_generated(main_root):
    perform(_report(), ROOT, str(main_root))
    expr = (main_root / "pkg" / "wrap" / "expr.py").read_text()
    assert "def __mul__(self, *args, **kwargs):" in expr
    assert "return wrap(self._inner.__mul__(*unwrap(args), **unwrap(kwargs)))" in expr


def test_init_defers_registry_load_off_the_import_path(main_root):
    perform(_report(), ROOT, str(main_root))
    init = (main_root / "pkg" / "wrap" / "__init__.py").read_text()
    # the submodule-import loop lives inside _load_registry (lazy), registered as a
    # loader — NOT run at import, so `import pkg.wrap` stays free of polars.
    assert "def _load_registry() -> None:" in init
    assert 'importlib.import_module(f"{__name__}.{submodule}")' in init
    assert "register_loader(_load_registry)" in init
    # the edition feature seam is installed lazily (guards/lineage/provenance),
    # not a hardcoded hook-module import
    assert (
        "from tabsdatak.extensions.tableframe._engine.features import install" in init
    )
    assert "install(TableFrame)" in init
    assert (
        '"expr"' in init and '"group_by"' in init
    )  # submodules registered for side-effects
    # no module-level polars import / eager loop on the import path
    assert "\nimport polars" not in init
    assert "\nfor _submodule" not in init
    # LazyGroupBy is imported locally inside group_by(), not re-exported at module top
    assert "        from pkg.wrap.group_by import LazyGroupBy" in init
    assert "\nfrom pkg.wrap.group_by import LazyGroupBy" not in init


def test_copyright_header_on_generated_files(main_root):
    perform(_report(), ROOT, str(main_root))
    for name in ("__init__.py", "expr.py", "functions.py", "expr.pyi"):
        first = (main_root / "pkg" / "wrap" / name).read_text().splitlines()[0]
        assert first.startswith("# Copyright ") and first.endswith("Tabsdata Inc.")


def test_generate_tests_into_test_root(tmp_path):
    code = tmp_path / "code" / "src" / "main" / "python"
    tests = tmp_path / "tests" / "src" / "utest" / "python"
    result = perform(_report(), ROOT, str(code), test_root=str(tests))
    names = {p.name for p in tests.glob("*.py")}
    assert {"expr.py", "functions.py", "group_by.py"} <= names
    assert "test_pkg.py" in names  # root module -> test_<first segment of root module>
    assert set(result.tests) >= {"expr", "functions", "test_pkg"}
    expr_test = (tests / "expr.py").read_text()
    assert expr_test.splitlines()[0].startswith("# Copyright ")
    assert "def test_alias_delegates():" in expr_test
    assert (
        'getattr(inner, "alias").assert_called()' in expr_test
    )  # spies the polars call


def _regenerate_with_handwritten_stub(main_root, engine_frame: str) -> str:
    perform(_report(), ROOT, str(main_root))
    base = main_root / "pkg" / "wrap"
    engine = base / "_engine" / "frame" / "frame.py"
    engine.parent.mkdir(parents=True)
    engine.write_text(engine_frame)
    stub = base / "__init__.pyi"
    text = stub.read_text().replace(
        "class TableFrame(EngineFrame):",
        "# devguide-ref: frames\nclass TableFrame(EngineFrame):",
    )
    text = text.replace('"""', '"""Module doc.', 1)
    text += (
        "\n    # MANUAL CHANGE POST GEN: declared by hand.\n"
        "    def grok(self, schema: ColumnDefinition) -> TableFrame:\n"
        '        """Grok doc."""\n'
        "\n    def gone(self) -> TableFrame:\n"
        '        """Not on the engine frame."""\n'
    )
    text = text.replace(
        "from __future__ import annotations\n",
        "from __future__ import annotations\n\nfrom pkg.grok import ColumnDefinition\n",
    )
    stub.write_text(text)
    perform(_report(), ROOT, str(main_root))
    return stub.read_text()


def test_regeneration_keeps_module_docstring_first(main_root):
    text = _regenerate_with_handwritten_stub(main_root, "class EngineFrame: ...\n")
    assert ast.get_docstring(ast.parse(text)) == "Module doc."


def test_regeneration_keeps_comments(main_root):
    text = _regenerate_with_handwritten_stub(main_root, "class EngineFrame: ...\n")
    assert "# devguide-ref: frames\nclass TableFrame(EngineFrame):" in text


def test_regeneration_carries_engine_frame_members_with_imports(main_root):
    text = _regenerate_with_handwritten_stub(
        main_root, "class EngineFrame:\n    def grok(self, schema): ...\n"
    )
    assert "# MANUAL CHANGE POST GEN: declared by hand." in text
    assert "def grok(self, schema: ColumnDefinition) -> TableFrame:" in text
    assert "from pkg.grok import ColumnDefinition" in text
    assert "def gone(" not in text


def test_multiline_docstring_summary_stays_on_the_opening_line(main_root):
    perform(_report(), ROOT, str(main_root))
    stub = main_root / "pkg" / "wrap" / "expr.pyi"
    stub.write_text(stub.read_text().replace('""" """', '"""Summary.\n\n    Body.\n    """', 1))
    perform(_report(), ROOT, str(main_root))
    assert '"""Summary.\n' in stub.read_text()
