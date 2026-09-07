#
#  Copyright 2026 Tabsdata Inc.
#

import hashlib
import json
import os
import pathlib
import re
import shlex
import shutil
import subprocess
import sys
import time

from testrs import make_command, outcome, quote, read

CONFIG = "td-test-rs"
WORKER = pathlib.Path(__file__).resolve().parent / "warpworker.sh"
RUNNER = pathlib.Path("target") / "td-test-rs"
POOL = RUNNER / ".pool"
RUNS = RUNNER / ".runs"
COLUMNS = 2
SEPARATOR = "then"
POLL = 0.2
FLUSH = 3.0
LAUNCH_TIMEOUT = 90.0
RETIRE_TIMEOUT = 10.0
LABEL_WIDTH = 18
SIGNATURE = "signature"
STALE = "stale"
BUSY = "busy"
REUSABLE = "reusable"
BLOCKED = {
    "OLDPWD",
    "PWD",
    "SHLVL",
    "TD_TMUX",
    "TD_WARP",
    "TERM_PROGRAM",
    "TERM_PROGRAM_VERSION",
    "TERM_SESSION_ID",
    "TMUX",
    "TMUX_PANE",
    "_",
    "__CFBundleIdentifier",
}
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def scheme() -> str:
    bundle = os.environ.get("__CFBundleIdentifier", "")
    version = os.environ.get("WARP_CLIENT_VERSION", "")
    if "preview" in f"{bundle}{version}".lower():
        return "warppreview"
    return "warp"


def tab_configs() -> pathlib.Path:
    if sys.platform == "darwin":
        return pathlib.Path.home() / ".warp" / "tab_configs"
    root = os.environ.get("XDG_DATA_HOME") or str(
        pathlib.Path.home() / ".local" / "share"
    )
    return pathlib.Path(root) / "warp-terminal" / "tab_configs"


def environment(target: pathlib.Path) -> None:
    lines = [
        f"{key}={shlex.quote(value)}"
        for key, value in sorted(os.environ.items())
        if key not in BLOCKED and IDENTIFIER.match(key)
    ]
    lines.append("CARGO_TERM_COLOR=always")
    lines.append("CLICOLOR_FORCE=1")
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    target.chmod(0o600)


def labels(names: list[str]) -> dict[str, str]:
    short = {name: re.sub(r"^__[a-z]+__(test_rs_)?", "", name) for name in names}
    if len(set(short.values())) != len(names) or not all(short.values()):
        return {name: name for name in names}
    return short


def parameters(
    name: str,
    named: dict[str, str],
    parallel: list[str],
    serial: list[str],
    base: list[str],
) -> str:
    waits = ""
    pending = ""
    if name in serial and parallel:
        waits = " ".join(f"{task}.code" for task in parallel)
        pending = ", ".join(named[task] for task in parallel)
    return (
        f"task={shlex.quote(name)}\n"
        f"waits={shlex.quote(waits)}\n"
        f"pending={shlex.quote(pending)}\n"
        f"make_command=({quote(base)})\n"
    )


def signature(confs: dict[str, str]) -> str:
    digest = hashlib.sha256()
    digest.update(read(WORKER).encode("utf-8"))
    for label in sorted(confs):
        digest.update(label.encode("utf-8"))
        digest.update(confs[label].encode("utf-8"))
    return digest.hexdigest()


def alive(pid: str) -> bool:
    try:
        os.kill(int(pid), 0)
    except (OSError, ValueError):
        return False
    return True


def classify(pool: pathlib.Path, workers: list[str], stamp: str) -> str:
    if read(pool / SIGNATURE).strip() != stamp:
        return STALE
    for label in workers:
        if not alive(read(pool / f"{label}.pid").strip()):
            return STALE
        if (pool / f"{label}.busy").exists() or (pool / f"{label}.job").exists():
            return BUSY
    return REUSABLE


def retire(pool: pathlib.Path) -> None:
    if pool.is_dir():
        (pool / "retire").touch()
        deadline = time.monotonic() + RETIRE_TIMEOUT
        while time.monotonic() < deadline and any(pool.glob("*.pid")):
            time.sleep(POLL)
    shutil.rmtree(pool, ignore_errors=True)


def install(directory: pathlib.Path, confs: dict[str, str], stamp: str) -> None:
    folder = directory / RUNNER
    shutil.rmtree(folder, ignore_errors=True)
    folder.mkdir(parents=True, exist_ok=True)
    pool = directory / POOL
    pool.mkdir(parents=True, exist_ok=True)
    for label, conf in confs.items():
        (pool / f"{label}.conf").write_text(conf, encoding="utf-8")
        (folder / label).symlink_to(WORKER)
    (pool / SIGNATURE).write_text(stamp + "\n", encoding="utf-8")


def column_count() -> int:
    requested = os.environ.get("TD_WARP_COLUMNS", "")
    return int(requested) if requested.isdigit() and int(requested) > 0 else COLUMNS


def grid(names: list[str]) -> list[list[str]]:
    count = column_count()
    if len(names) < 2 * count:
        return [list(names)]
    height = -(-len(names) // count)
    return [names[index : index + height] for index in range(0, len(names), height)]


def layout(parallel: list[str], serial: list[str]) -> list[list[str]]:
    names = parallel + serial
    if not parallel or not serial:
        return grid(names)
    columns = grid(parallel)
    max(columns, key=len).extend(serial)
    crowded = len(columns) < len(grid(names))
    lonely = len(columns) > 1 and any(len(column) < 2 for column in columns)
    if crowded or lonely:
        return grid(names)
    return columns


def tab_config(
    columns: list[list[str]], directory: pathlib.Path, named: dict[str, str]
) -> str:
    folder = directory / RUNNER
    lines = ['name = "Tabsdata: Rust tests"', 'title = "Rust tests"', 'color = "cyan"']

    def split(identity: str, direction: str, children: list[str]) -> None:
        lines.extend([
            "",
            "[[panes]]",
            f"id = {json.dumps(identity)}",
            f"split = {json.dumps(direction)}",
            f"children = [{', '.join(json.dumps(child) for child in children)}]",
        ])

    names = [name for column in columns for name in column]
    if len(columns) > 1:
        identities = [f"column{position}" for position in range(len(columns))]
        split("root", "horizontal", identities)
        for identity, column in zip(identities, columns):
            split(identity, "vertical", column)
    elif len(names) > 1:
        split("root", "vertical", names)
    for position, name in enumerate(names):
        commands = [f'export PATH="{folder}:$PATH"', named[name]]
        lines += [
            "",
            "[[panes]]",
            f"id = {json.dumps(name)}",
            'type = "terminal"',
            f"directory = {json.dumps(str(directory))}",
            f"commands = [{', '.join(json.dumps(entry) for entry in commands)}]",
        ]
        if position == 0:
            lines.append("is_focused = true")
    return "\n".join(lines) + "\n"


def launch() -> bool:
    target = f"{scheme()}://tab_config/{CONFIG}"
    opener = ["open", target] if sys.platform == "darwin" else ["xdg-open", target]
    try:
        completed = subprocess.run(
            opener,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError:
        return False
    return completed.returncode == 0


def wait_paths(paths: list[pathlib.Path], timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while not all(path.exists() for path in paths):
        if time.monotonic() > deadline:
            return False
        time.sleep(POLL)
    return True


def wait_codes(pool: pathlib.Path, run: pathlib.Path, named: dict[str, str]) -> None:
    pending = dict(named)
    while pending:
        for name in list(pending):
            marker = run / f"{name}.code"
            if marker.exists():
                report(marker)
                del pending[name]
        if not pending:
            return
        for name, label in pending.items():
            if not alive(read(pool / f"{label}.pid").strip()):
                print(
                    f"The Warp pane running {label} is gone.",
                    file=sys.stderr,
                )
                return
        time.sleep(POLL)


def report(marker: pathlib.Path) -> None:
    code = read(marker).strip() or "?"
    icon = "✅" if code == "0" else "❌"
    print(f"{icon} {marker.stem} finished with exit code: '{code}'", flush=True)


def sequential(names: list[str], base: list[str]) -> int:
    outcome = 0
    for name in names:
        code = subprocess.run(base + [name], check=False).returncode
        if code and not outcome:
            outcome = code
    return outcome


def open_panes(
    directory: pathlib.Path,
    pool: pathlib.Path,
    named: dict[str, str],
    parallel: list[str],
    serial: list[str],
) -> bool:
    folder = tab_configs()
    folder.mkdir(parents=True, exist_ok=True)
    config = folder / f"{CONFIG}.toml"
    config.write_text(
        tab_config(layout(parallel, serial), directory, named), encoding="utf-8"
    )
    ready = launch() and wait_paths(
        [pool / f"{label}.pid" for label in named.values()], LAUNCH_TIMEOUT
    )
    config.unlink(missing_ok=True)
    return ready


def summarize(run: pathlib.Path, names: list[str], named: dict[str, str]) -> int:
    codes = {name: read(run / f"{name}.code").strip() for name in names}
    failed = []
    print()
    for name in names:
        code = codes[name]
        spent = read(run / f"{name}.secs").strip() or "?"
        if not code.isdigit():
            print(f"❔ {named[name]:<{LABEL_WIDTH}} no exit code")
            continue
        icon = "✅" if code == "0" else "❌"
        print(f"{icon} {named[name]:<{LABEL_WIDTH}} exit {code:<5} {spent}s")
        if code != "0":
            failed.append(name)
    for name in failed:
        print(f"\n{'-' * 30} {name} {'-' * 30}")
        print(read(run / f"{name}.log").strip("\n"))
    result, unknown = outcome(codes)
    if unknown:
        print(
            f"No exit code reported by: {', '.join(unknown)}; treating as failed.",
            file=sys.stderr,
        )
    return result


def main() -> int:
    arguments = sys.argv[1:]
    if not arguments or SEPARATOR in (arguments[0], arguments[-1]):
        print(
            f"Usage: warptestrs.py <task>... [{SEPARATOR} <task>...]", file=sys.stderr
        )
        return 2
    if SEPARATOR in arguments:
        position = arguments.index(SEPARATOR)
        parallel, serial = arguments[:position], arguments[position + 1 :]
    else:
        parallel, serial = arguments, []
    names = parallel + serial
    base = make_command()
    directory = pathlib.Path(os.getcwd())
    pool = directory / POOL
    named = labels(names)
    confs = {
        named[name]: parameters(name, named, parallel, serial, base) for name in names
    }
    stamp = signature(confs)
    state = classify(pool, list(named.values()), stamp)
    if state == BUSY:
        print(
            "The Warp test panes are busy with another run; running the test groups"
            " sequentially in this pane.",
            file=sys.stderr,
        )
        return sequential(names, base)
    run = directory / RUNS / str(os.getpid())
    try:
        if state == STALE:
            retire(pool)
            install(directory, confs, stamp)
        run.mkdir(parents=True, exist_ok=True)
        environment(run / "environment")
        (run / "driver").write_text(f"{os.getpid()}\n", encoding="utf-8")
        if state == STALE:
            if not open_panes(directory, pool, named, parallel, serial):
                retire(pool)
                print(
                    "Warp did not open the test panes; running the test groups"
                    " sequentially in this pane.",
                    file=sys.stderr,
                )
                return sequential(names, base)
            print(f"🖥️ Opened Warp panes: {', '.join(named.values())}", flush=True)
        else:
            print(f"🖥️ Reusing Warp panes: {', '.join(named.values())}", flush=True)
        for label in named.values():
            (pool / f"{label}.job").write_text(f"{run}\n", encoding="utf-8")
        wait_codes(pool, run, named)
        time.sleep(FLUSH)
        return summarize(run, names, named)
    finally:
        shutil.rmtree(run, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
