"""Tests for code/.pyi generation."""

import pytest

from tabsdatax.generator.tableframe.emit.generate import generate
from tabsdatax.generator.tableframe.model.model import Report, Symbol

ROOT = "pkg.wrap"


@pytest.fixture
def root_dir(tmp_path):
    # Mirror the Maven source layout the generator expects: the package tree
    # lives under `src/main/python`, so both the testdir derivation (main ->
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


def test_all_modules_generated(root_dir):
    result = generate(_report(), ROOT, str(root_dir))
    base = root_dir / "pkg" / "wrap"
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


def test_datatypes_module_aliases_passthrough_and_allowlist(root_dir):
    generate(_report(), ROOT, str(root_dir))
    dt = (root_dir / "pkg" / "wrap" / "datatypes.py").read_text()
    assert "import polars as pl" in dt
    assert (
        "from pkg.wrap._core.translate import "
        "register_passthrough, register_type_validator"
    ) in dt
    assert "Int64 = pl.Int64" in dt and "String = pl.String" in dt
    assert "DataType = pl.DataType" in dt
    # editable allow-list seeded with every alias, plus the validator + registration
    assert "_ALLOWED_TYPES = [" in dt and "    Int64," in dt
    assert "def _valid_type(dtype):" in dt
    assert "register_passthrough(pl.DataType)" in dt
    assert "register_type_validator(_valid_type)" in dt
    # the __init__ loop imports it so passthrough/validator register on
    # `import pkg.wrap`
    init = (root_dir / "pkg" / "wrap" / "__init__.py").read_text()
    assert '"datatypes"' in init


def test_tableframe_has_hooks_and_augmenting_ops(root_dir):
    generate(_report(), ROOT, str(root_dir))
    text = (root_dir / "pkg" / "wrap" / "__init__.py").read_text()  # ruff-formatted
    assert "class TableFrame(EngineFrame):" in text
    # _HOOKABLE_OPS lists the dispatching method names (no policy in generated code)
    assert "_HOOKABLE_OPS = (" in text
    for op in ('"agg"', '"join"', '"select"', '"with_columns"', '"drop"', '"rename"'):
        assert op in text
    assert "from pkg.wrap._core.hooks import run_hook" in text
    # augmenting ops wrap then dispatch run_hook — no typed registrars, no lineage
    assert "def join(" in text and 'how="inner"' in text
    assert "run_hook(" in text and "sources=(self, other)" in text
    assert "def select(self, *exprs, **named_exprs):" in text
    assert '"replace_all": True' in text  # select replaces
    assert "sources=(self,)" in text
    assert "def with_columns(self, *exprs, **named_exprs):" in text
    assert (
        "return LazyGroupBy(self._lf.group_by(*u_by, **u_named), self, (u_by, u_named))"
        in text
    )
    # transparent op: LazyFrame result -> _clone (carry state); else wrap as a value
    assert "out = self._lf.filter(*unwrap(args), **unwrap(kwargs))" in text
    assert (
        "return self._clone(out) if isinstance(out, type(self._lf)) else wrap(out)"
        in text
    )
    # property reads (wrapped) underlying frame
    assert "return wrap(self._lf.schema)" in text


def test_drop_and_rename_dispatch_hooks(root_dir):
    generate(_report(), ROOT, str(root_dir))
    init = (root_dir / "pkg" / "wrap" / "__init__.py").read_text()
    assert "def drop(self, *columns, **kwargs):" in init
    assert (
        'run_hook("drop", out, sources=(self,), params={"columns": u_columns})' in init
    )
    assert "def rename(self, mapping, *args, **kwargs):" in init
    assert (
        'run_hook("rename", out, sources=(self,), params={"mapping": u_mapping})'
        in init
    )


def test_lazygroupby_agg(root_dir):
    generate(_report(), ROOT, str(root_dir))
    text = (root_dir / "pkg" / "wrap" / "group_by.py").read_text()
    assert "class LazyGroupBy:" in text
    assert "def agg(self, *aggs, **named_aggs):" in text
    assert "by, named_by = self._by" in text
    assert "run_hook(" in text and "sources=(self._parent,)" in text


def test_value_class_and_function_still_transparent(root_dir):
    generate(_report(), ROOT, str(root_dir))
    expr = (root_dir / "pkg" / "wrap" / "expr.py").read_text()
    assert "@register(_PlExpr)" in expr
    assert "return wrap(self._inner.alias(*unwrap(args), **unwrap(kwargs)))" in expr
    funcs = (root_dir / "pkg" / "wrap" / "functions.py").read_text()
    assert "return wrap(pl.col(*unwrap(args), **unwrap(kwargs)))" in funcs


def test_generates_core_runtime(root_dir):
    generate(_report(), ROOT, str(root_dir))
    core = root_dir / "pkg" / "wrap" / "_core"
    assert {p.name for p in core.glob("*.py")} == {
        "base.py",
        "translate.py",
        "hooks.py",
    }
    hooks = (core / "hooks.py").read_text()
    assert "from pkg.wrap import TableFrame" in hooks  # root module substituted
    assert "__ROOT_MODULE__" not in hooks
    assert (core / "base.py").read_text().splitlines()[0].startswith("# Copyright ")


def test_hooks_module_has_per_op_typed_hook_types(root_dir):
    generate(_report(), ROOT, str(root_dir))
    hooks = (root_dir / "pkg" / "wrap" / "_core" / "hooks.py").read_text()
    assert "__HOOK_TYPES__" not in hooks  # placeholder substituted
    # one TypedDict per distinct hook_name (project shared by select/with_columns)
    assert "class JoinParams(TypedDict):" in hooks
    assert "class ProjectParams(TypedDict):" in hooks
    assert "class AggParams(TypedDict):" in hooks
    # join params carry the real keys
    for field in ("on:", "how: str", "left_on:", "right_on:", "suffix: str"):
        assert field in hooks
    # one Hook alias per op-name, with the right source arity + params
    assert (
        "JoinHook = Callable[\n    "
        '["TableFrame", tuple["TableFrame", "TableFrame"], JoinParams]' in hooks
    )
    assert (
        "SelectHook = Callable["
        '["TableFrame", tuple["TableFrame"], ProjectParams], "TableFrame"]' in hooks
    )
    assert "WithColumnsHook = Callable[" in hooks and "ProjectParams" in hooks
    assert "AggHook = Callable[" in hooks


def test_multiple_classes_in_one_module(root_dir):
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
    generate(report, ROOT, str(root_dir))
    text = (root_dir / "pkg" / "wrap" / "whenthen.py").read_text()
    assert "class When(BaseWrapper):" in text
    assert "class Then(BaseWrapper):" in text
    assert "@register(_PlWhen)" in text and "@register(_PlThen)" in text


def test_operator_dunder_generated(root_dir):
    generate(_report(), ROOT, str(root_dir))
    expr = (root_dir / "pkg" / "wrap" / "expr.py").read_text()
    assert "def __mul__(self, *args, **kwargs):" in expr
    assert "return wrap(self._inner.__mul__(*unwrap(args), **unwrap(kwargs)))" in expr


def test_init_defers_registry_load_off_the_import_path(root_dir):
    generate(_report(), ROOT, str(root_dir))
    init = (root_dir / "pkg" / "wrap" / "__init__.py").read_text()
    # the submodule-import loop lives inside _load_registry (lazy), registered as a
    # loader — NOT run at import, so `import pkg.wrap` stays free of polars.
    assert "def _load_registry() -> None:" in init
    assert 'importlib.import_module(f"{__name__}.{submodule}")' in init
    assert "register_loader(_load_registry)" in init
    assert (
        '"expr"' in init and '"group_by"' in init
    )  # submodules registered for side-effects
    # no module-level polars import / eager loop on the import path
    assert "\nimport polars" not in init
    assert "\nfor _submodule" not in init
    # LazyGroupBy is imported locally inside group_by(), not re-exported at module top
    assert "        from pkg.wrap.group_by import LazyGroupBy" in init
    assert "\nfrom pkg.wrap.group_by import LazyGroupBy" not in init


def test_copyright_header_on_generated_files(root_dir):
    generate(_report(), ROOT, str(root_dir))
    for name in ("__init__.py", "expr.py", "functions.py", "expr.pyi"):
        first = (root_dir / "pkg" / "wrap" / name).read_text().splitlines()[0]
        assert first.startswith("# Copyright ") and first.endswith("Tabsdata Inc.")


def test_generate_tests_into_testdir(tmp_path):
    code = tmp_path / "code" / "src" / "main" / "python"
    tests = tmp_path / "tests" / "src" / "utest" / "python"
    result = generate(_report(), ROOT, str(code), testdir=str(tests))
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
