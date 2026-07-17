#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import ast
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def parse_python_file(path: str | Path) -> ast.Module | None:
    try:
        with open(path, "r", encoding="utf-8") as source:
            content = source.read()
    except (OSError, UnicodeDecodeError) as e:
        logger.debug(f"Could not read {path}: {e}")
        return None
    try:
        return ast.parse(content, filename=str(path))
    except SyntaxError as e:
        logger.debug(f"Could not parse {path}: {e}")
        return None


def matches_decorator(decorator: ast.expr, name: str) -> bool:
    if isinstance(decorator, ast.Name):
        return decorator.id == name
    if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name):
        return decorator.func.id == name
    return False


def find_decorator_call(
    tree: ast.Module,
    function_name: str,
    decorator_name: str,
) -> ast.expr | None:
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name != function_name:
            continue
        for decorator in node.decorator_list:
            if matches_decorator(decorator, decorator_name):
                return decorator
    return None


def get_decorator_string_keyword(
    decorator: ast.expr,
    keyword: str,
) -> str | None:
    if not isinstance(decorator, ast.Call):
        return None
    for kw in decorator.keywords:
        if kw.arg != keyword:
            continue
        if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
            return kw.value.value
    return None
