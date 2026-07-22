#
# Copyright 2026 Tabsdata Inc.
#

import importlib.util
import json
import os
import subprocess
import sys
from types import ModuleType


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

MAIN_SUFFIX = "src/main/python"
TEST_SUFFIXES = ("src/utest/python", "src/itest/python")

STRICT_LAYER = "src/main/python/tabsdatak/"

GATE_CODES = {
    "import",
    "import-not-found",
    "import-untyped",
    "name-defined",
    "used-before-def",
    "attr-defined",
    "call-arg",
    "no-redef",
    "valid-type",
    "valid-newtype",
    "misc",
    "syntax",
}

MYPY_ARGUMENTS = [
    "--namespace-packages",
    "--explicit-package-bases",
    "--follow-imports=silent",
    "--ignore-missing-imports",
    "--no-error-summary",
    "--show-absolute-path",
    "--output=json",
]

SKIP_PACKAGES = {"resources", "scripts", "tabsdata"}


def python_path_entries() -> list[str]:
    raw = os.getenv("PYTHONPATH", "")
    normalized = [
        entry.replace(os.sep, "/").rstrip("/") for entry in raw.split(os.pathsep)
    ]
    seen = set()
    entries = []
    for entry in normalized:
        if entry and entry not in seen:
            seen.add(entry)
            entries.append(entry)
    return entries


def roots(entries: list[str], suffixes) -> list[str]:
    return [entry for entry in entries if entry.endswith(suffixes)]


def under(path: str, root: str) -> bool:
    return path == root or path.startswith(f"{root}/")


def project_real_prefixes(project_root: str) -> list[str]:
    prefixes = [normalize(os.path.realpath(project_root))]
    try:
        children = os.listdir(project_root)
    except OSError:
        children = []
    for child in children:
        prefixes.append(normalize(os.path.realpath(os.path.join(project_root, child))))
    return prefixes


def in_project(path: str, prefixes: list[str]) -> bool:
    real = normalize(os.path.realpath(path))
    return any(under(real, prefix) for prefix in prefixes)


def normalize(path: str) -> str:
    return path.replace(os.sep, "/")


def relative(file: str, project_root: str) -> str:
    file = normalize(file)
    prefix = f"{project_root}/"
    return file[len(prefix) :] if file.startswith(prefix) else file


def is_strict(file: str) -> bool:
    return STRICT_LAYER in normalize(file)


def error_key(error: dict, project_root: str) -> tuple:
    return (
        relative(error.get("file", ""), project_root),
        error.get("code", "error"),
        error.get("message", "").splitlines()[0],
    )


def load_baseline(baseline_file: str) -> dict:
    if not os.path.exists(baseline_file):
        return {}
    with open(baseline_file) as stream:
        stored = json.load(stream)
    counts = {}
    for entry in stored.get("errors", []):
        counts[tuple(entry)] = counts.get(tuple(entry), 0) + 1
    return counts


def write_json(path: str, payload) -> None:
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    with open(path, "w") as stream:
        json.dump(payload, stream, indent=2)


def write_baseline(baseline_file: str, errors: list[dict], project_root: str) -> None:
    keys = sorted(
        error_key(error, project_root)
        for error in errors
        if not is_strict(error.get("file", ""))
    )
    write_json(baseline_file, {"errors": [list(key) for key in keys]})
    logger.info(
        f"📝 Wrote baseline with {len(keys)} accepted error(s) to '{baseline_file}'."
    )


def summarize(errors: list[dict]) -> None:
    counts = {}
    for error in errors:
        code = error.get("code", "error")
        counts[code] = counts.get(code, 0) + 1
    for code in sorted(counts, key=lambda name: (-counts[name], name)):
        logger.error(f"⭕️ {counts[code]:>5}  {code}")


def top_level_packages(check_targets: list[str]) -> list[str]:
    packages = set()
    for root in check_targets:
        if not os.path.isdir(root):
            continue
        for name in os.listdir(root):
            if name.startswith((".", "_")) or name in SKIP_PACKAGES:
                continue
            if os.path.isdir(os.path.join(root, name)):
                packages.add(name)
    return sorted(packages)


def check_package(
    package: str, python_path: str, mypy_path: str, cache_dir: str, config_file: str
) -> list[dict]:
    command = [
        python_path,
        "-m",
        "mypy",
        *MYPY_ARGUMENTS,
        "--config-file",
        config_file,
        "--cache-dir",
        cache_dir,
        "-p",
        package,
    ]
    environment = dict(os.environ, MYPYPATH=mypy_path)
    logger.debug(f"🚀 mypy -p {package}")

    completed = subprocess.run(command, env=environment, capture_output=True, text=True)

    errors = []
    for line in completed.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        row = json.loads(line)
        if row.get("severity") != "error":
            continue
        if row.get("code") not in GATE_CODES:
            continue
        errors.append(row)
    return errors


def run_mypy(
    project_root: str, python_path: str, cache_dir: str, config_file: str
) -> list[dict] | None:
    entries = python_path_entries()
    main_roots = roots(entries, MAIN_SUFFIX)
    test_roots = roots(entries, TEST_SUFFIXES)
    all_roots = main_roots + test_roots

    prefixes = project_real_prefixes(project_root)
    check_targets = [root for root in all_roots if in_project(root, prefixes)]

    if not check_targets:
        logger.warning(
            "⚠️ No Python source roots found under the project root on PYTHONPATH."
            " Run the test context setup first so that main, utest and itest roots"
            " are populated."
        )
        return []

    packages = top_level_packages(check_targets)
    mypy_path = os.pathsep.join(all_roots)

    logger.info(
        f"🎢 Checking {len(packages)} package(s) across {len(check_targets)} source"
        f" roots under '{project_root}' with the hydrated environment."
    )

    errors = []
    for package in packages:
        for error in check_package(
            package, python_path, mypy_path, cache_dir, config_file
        ):
            error["file"] = normalize(error.get("file", ""))
            if in_project(error["file"], prefixes):
                errors.append(error)
    return errors


def report_error(error: dict) -> None:
    file = error.get("file", "?")
    line = error.get("line", 0)
    code = error.get("code", "error")
    message = error.get("message", "").splitlines()[0]
    logger.error(f"⭕️ {file}:{line} [{code}] {message}")


def check(
    project_root: str,
    python_path: str,
    config_file: str,
    baseline_file: str,
    cache_dir: str,
) -> int:
    project_root = normalize(project_root).rstrip("/")

    errors = run_mypy(project_root, python_path, cache_dir, config_file)
    if errors is None:
        return 1
    if not errors:
        logger.info("✅ Check from mypy emitted no errors.")
        return 0

    baseline = load_baseline(baseline_file)
    remaining = dict(baseline)

    strict_errors = []
    new_errors = []
    for error in errors:
        if is_strict(error.get("file", "")):
            strict_errors.append(error)
            continue
        key = error_key(error, project_root)
        if remaining.get(key, 0) > 0:
            remaining[key] -= 1
        else:
            new_errors.append(error)

    if strict_errors:
        logger.error(
            f"🚫 Errors in the strict layer ('{STRICT_LAYER}') are never allowed:"
        )
        for error in strict_errors:
            report_error(error)

    if new_errors:
        logger.error("🆕 New errors not present in the baseline:")
        for error in new_errors:
            report_error(error)

    failing = strict_errors + new_errors
    if failing:
        summarize(failing)
        baselined = len(errors) - len(failing)
        logger.error(
            f"⛔️ mypy found {len(failing)} disallowed error(s)"
            f" ({len(strict_errors)} strict, {len(new_errors)} new;"
            f" {baselined} baselined)."
        )
        return 1

    logger.info(f"✅ No disallowed errors ({len(errors)} baselined error(s) accepted).")
    return 0


def main() -> None:
    write = "--write-baseline" in sys.argv[1:]
    positionals = [
        argument for argument in sys.argv[1:] if not argument.startswith("--")
    ]
    project_root, python_path, config_file, baseline_file, cache_dir = positionals[:5]
    project_root = normalize(project_root).rstrip("/")

    if write:
        errors = run_mypy(project_root, python_path, cache_dir, config_file)
        if errors is None:
            sys.exit(1)
        write_baseline(baseline_file, errors, project_root)
        sys.exit(0)

    sys.exit(check(project_root, python_path, config_file, baseline_file, cache_dir))


main()
