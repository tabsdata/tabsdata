#
#  Copyright 2026 Tabsdata Inc.
#

import os
import pathlib
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time

from testrs import make_command, outcome, quote, read

SESSION = "td-test-rs"

HOLD = 3.0


def tmux_command(tmux: str, *arguments: str, check: bool = False) -> None:
    subprocess.run(
        [tmux, *arguments],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=check,
    )


def capture(tmux: str) -> list[str]:
    listed = subprocess.run(
        [tmux, "list-panes", "-t", SESSION, "-F", "#{pane_id}"],
        capture_output=True,
        text=True,
        check=False,
    )
    transcripts = []
    for identity in listed.stdout.split():
        rendered = subprocess.run(
            [tmux, "capture-pane", "-t", identity, "-p", "-e", "-S", "-"],
            capture_output=True,
            text=True,
            check=False,
        )
        transcripts.append(rendered.stdout.strip("\n"))
    return transcripts


def main() -> int:
    names = sys.argv[1:]
    if not names:
        print("Usage: tmuxtestrs.py <task>...", file=sys.stderr)
        return 2

    tmux = shutil.which("tmux")
    if tmux is None:
        print("Required tool tmux not found.", file=sys.stderr)
        return 2

    base = make_command()

    codes = tempfile.mkdtemp(prefix="td-test-rs-")
    columns, rows = shutil.get_terminal_size((160, 40))

    def pane(name: str) -> str:
        marker = os.path.join(codes, f"{name}.code")
        done = os.path.join(codes, "done")
        return (
            f"{quote(base + [name])}; printf %s $? > {shlex.quote(marker)}; "
            f"while [ ! -f {shlex.quote(done)} ]; do sleep 0.2; done"
        )

    tmux_command(tmux, "kill-session", "-t", SESSION)
    tmux_command(
        tmux,
        "new-session",
        "-d",
        "-s",
        SESSION,
        "-x",
        str(columns),
        "-y",
        str(rows),
        pane(names[0]),
        check=True,
    )
    for name in names[1:]:
        tmux_command(tmux, "split-window", "-t", SESSION, "-v", pane(name), check=True)
    tmux_command(tmux, "select-layout", "-t", SESSION, "even-vertical")

    transcripts: list[str] = []

    def release() -> None:
        markers = [os.path.join(codes, f"{name}.code") for name in names]
        while not all(os.path.exists(marker) for marker in markers):
            time.sleep(0.2)
        time.sleep(HOLD)
        transcripts.extend(capture(tmux))
        pathlib.Path(os.path.join(codes, "done")).touch()

    watcher = threading.Thread(target=release, daemon=True)
    watcher.start()
    subprocess.run([tmux, "attach-session", "-t", SESSION], check=False)
    watcher.join(timeout=HOLD + 10)
    if not transcripts:
        transcripts = capture(tmux)
    tmux_command(tmux, "kill-server")
    for name, transcript in zip(names, transcripts):
        print(f"\n{'-' * 30} {name} {'-' * 30}")
        print(transcript)

    reported = {
        name: read(pathlib.Path(codes) / f"{name}.code").strip() for name in names
    }
    shutil.rmtree(codes, ignore_errors=True)

    result, unknown = outcome(reported)
    if unknown:
        print(
            f"No exit code reported by: {', '.join(unknown)}; treating as failed.",
            file=sys.stderr,
        )
    return result


if __name__ == "__main__":
    sys.exit(main())
