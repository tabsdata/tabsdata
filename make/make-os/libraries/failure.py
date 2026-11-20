#
# Copyright 2026 Tabsdata Inc.
#

import json
import os
import sys
import time
from pathlib import Path

DUCKSCRIPT_RUNNER = "@duckscript"
# cargo-make runs a duckscript behind two lines of its own, and the line of
# an error it reports counts them.
DUCKSCRIPT_FIRST_LINE = 3
STALE_NOTE_SECONDS = 2 * 24 * 60 * 60
PREFIX = "[failure]"


def unfinished_notes(folder: Path, run: str, level: int) -> list[Path]:
    notes = []
    for note in folder.glob(f"{run}-*.json"):
        fields = note.stem.split("-", 3)
        if len(fields) == 4 and fields[1].isdigit() and fields[2].isdigit():
            if int(fields[1]) > level:
                notes.append((int(fields[1]), int(fields[2]), note))
    return [note for _, _, note in sorted(notes)]


def script_lines(task: dict) -> list[str]:
    script = task.get("script")
    if isinstance(script, str):
        text = script
    elif isinstance(script, list) and all(isinstance(part, str) for part in script):
        text = "\n".join(script)
    else:
        return []
    lines = [line.rstrip("\r") for line in text.split("\n")]
    while lines and not lines[-1].strip():
        lines.pop()
    return lines


def definition(root: Path, name: str) -> str | None:
    header = f"[tasks.{name}]"
    files = [path for path in sorted(root.glob("*.toml")) if path.is_file()]
    for folder, _, names in os.walk(root / "make", followlinks=True):
        files.extend(
            Path(folder) / file for file in sorted(names) if file.endswith(".toml")
        )
    for file in files:
        try:
            lines = file.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeDecodeError):
            continue
        for number, line in enumerate(lines, start=1):
            if line.strip() == header:
                return f"{file.relative_to(root).as_posix()}:{number}"
    return None


def report(root: Path, name: str, task: dict) -> None:
    where = definition(root, name)
    where = f" ({where})" if where else ""
    lines = script_lines(task)
    if lines:
        duckscript = task.get("script_runner") == DUCKSCRIPT_RUNNER
        first = DUCKSCRIPT_FIRST_LINE if duckscript else 1
        numbering = ", numbered as cargo-make reports its lines" if duckscript else ""
        print(f"{PREFIX} Script of task '{name}'{where}{numbering}:")
        for number, line in enumerate(lines, start=first):
            print(f"{number:>6} | {line}")
    elif task.get("command"):
        command = " ".join([task["command"], *(task.get("args") or [])])
        print(f"{PREFIX} Command of task '{name}'{where}: {command}")
    else:
        print(f"{PREFIX} Task '{name}'{where} runs no script or command of its own")


def drop_stale_notes(folder: Path) -> None:
    limit = time.time() - STALE_NOTE_SECONDS
    for note in folder.glob("*.json"):
        try:
            if note.stat().st_mtime < limit:
                note.unlink()
        except OSError:
            pass


def main() -> None:
    run = os.environ.get("TD_MAKE_RUN")
    folder = Path(sys.argv[1])
    if not run or not folder.is_dir():
        return
    root = Path.cwd()
    level = os.environ.get("CARGO_MAKE_INTERNAL_RECURSION_LEVEL", "")
    notes = unfinished_notes(folder, run, int(level) if level.isdigit() else 0)

    tasks = []
    for note in notes:
        name = note.stem.split("-", 3)[3]
        try:
            task = json.loads(note.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            task = {}
        tasks.append((name, task if isinstance(task, dict) else {}))
        try:
            note.unlink()
        except OSError:
            pass
    drop_stale_notes(folder)
    if not tasks:
        return

    print()
    print(f"{PREFIX} Tasks that did not finish, outermost first:")
    for name, _ in tasks:
        print(f"{PREFIX}   {name}")
    name, task = tasks[-1]
    report(root, name, task)
    print()


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    # noinspection PyBroadException
    try:
        main()
    except Exception as error:
        print(f"{PREFIX} Could not report the failed task: {error}")
