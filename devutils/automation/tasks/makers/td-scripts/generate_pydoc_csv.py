#
#  Copyright 2025 Tabs Data Inc.
#

import ast
import csv
import importlib
import importlib.util
import os
from os.path import join
from types import ModuleType

PYTHON_EXTENSION = ".py"


# noinspection DuplicatedCode
def load(module_name) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        module_name,
        os.path.join(
            os.getenv("MAKE_LIBRARIES_PATH"),
            f"{module_name}.py",
        ),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


logger = load("log").get_logger()


def format_module_path(path, root):
    module = os.path.relpath(path, root).replace(os.sep, ".")
    return module.removesuffix(PYTHON_EXTENSION)


def extract_categories(decorator):
    if not isinstance(decorator, ast.Call):
        return None

    func = decorator.func
    if not (
        (isinstance(func, ast.Name) and func.id == "pydoc")
        or (isinstance(func, ast.Attribute) and func.attr == "pydoc")
    ):
        return None
    for keyword in decorator.keywords:
        if keyword.arg == "categories":
            if isinstance(keyword.value, ast.Constant):
                return keyword.value.value
            elif isinstance(keyword.value, ast.List):
                return [
                    elt.value
                    for elt in keyword.value.elts
                    if isinstance(elt, ast.Constant)
                ]
    return None


class PydocVisitor(ast.NodeVisitor):
    def __init__(self, module_path):
        self.module_path = module_path
        self.scope_stack: list[str] = []
        self.report: list[tuple[object, str, str]] = []

    def _handle_function_like(self, _node: ast.AST, name: str, decorators):
        for decorator in decorators:
            categories = extract_categories(decorator)
            if categories is not None:
                qualified_name = (
                    ".".join(self.scope_stack + [name]) if self.scope_stack else name
                )
                self.report.append((categories, qualified_name, self.module_path))

    def visit_ClassDef(self, node: ast.ClassDef):
        self.scope_stack.append(node.name)
        self.generic_visit(node)
        self.scope_stack.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef):
        self._handle_function_like(node, node.name, node.decorator_list)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
        self._handle_function_like(node, node.name, node.decorator_list)


def process_module(path, root):
    try:
        with open(path, "r", encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename=path)
    except (SyntaxError, UnicodeDecodeError):
        raise ValueError(f"Failed to parse file {path}")

    module_path = format_module_path(path, root)
    visitor = PydocVisitor(module_path)
    visitor.visit(tree)
    return visitor.report


def find_pydoc_categories(root, output):
    report = []
    for folder, _, files in os.walk(root):
        for file in sorted(files):
            if file.endswith(PYTHON_EXTENSION):
                report.extend(process_module(join(folder, file), root))
    report.sort(key=lambda x: (str(x[0]), str(x[1]), str(x[2])))
    with open(output, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["categories", "function", "module"])
        writer.writerows(report)
    logger.info(f"📜 The pydoc csv file was saved @ '{output}'")


def generate_pydoc_csv():
    project_root = os.getenv("ROOT_PROJECT_TABSDATA_FOLDER")
    target = join(project_root, "target", "pydoc")
    os.makedirs(target, exist_ok=True)
    code_root = join(project_root, "client", "td-sdk")
    code_root = os.path.normpath(code_root)
    output = join(target, "PYDOC.csv")
    find_pydoc_categories(code_root, output)


generate_pydoc_csv()
