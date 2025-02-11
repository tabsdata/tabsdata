#
#  Copyright 2025 Tabs Data Inc.
#

import ast
import csv
import os
from os.path import join


def format_module_path(path, root):
    module = os.path.relpath(path, root).replace(os.sep, ".")
    return module[:-3] if module.endswith(".py") else module


def extract_categories(decorator):
    if not isinstance(decorator, ast.Call):
        return None

    func = decorator.func
    if not (
        (isinstance(func, ast.Name) and func.id == "pydoc")
        or (isinstance(func, ast.Attribute) and func.attr == "pydoc")
    ):
        return None
    for kw in decorator.keywords:
        if kw.arg == "categories":
            if isinstance(kw.value, ast.Constant):
                return kw.value.value
            elif isinstance(kw.value, ast.List):
                return [
                    el.value for el in kw.value.elts if isinstance(el, ast.Constant)
                ]
    return None


def process_module(path, root):
    try:
        with open(path, "r", encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename=path)
    except (SyntaxError, UnicodeDecodeError) as error:
        print(f"Skipping {path} due to parsing error: {error}")
        return []
    report = []
    module_path = format_module_path(path, root)
    current_class = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            current_class = node.name
        elif isinstance(node, ast.FunctionDef):
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Call):
                    categories = extract_categories(decorator)
                    if categories is not None:
                        report.append(
                            (
                                categories,
                                (
                                    f"{current_class}.{node.name}"
                                    if current_class
                                    else node.name
                                ),
                                module_path,
                            )
                        )

    return report


def find_pydoc_categories(root, output):
    report = []
    for folder, _, files in os.walk(root):
        for file in sorted(files):
            if file.endswith(".py"):
                report.extend(process_module(join(folder, file), root))
    report.sort(key=lambda x: (x[0], str(x[1]), str(x[2])))
    with open(output, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["categories", "function", "module"])
        writer.writerows(report)
    print(f"CSV file saved @ '{output}'")


def main():
    location = os.path.dirname(os.path.abspath(__file__))
    target = join(location, "..", "..", "..", "target", "pydoc")
    os.makedirs(target, exist_ok=True)
    root = join(location, "..", "..", "..", "client", "td-sdk")
    output = join(target, "categories.csv")
    find_pydoc_categories(root, output)


if __name__ == "__main__":
    main()
