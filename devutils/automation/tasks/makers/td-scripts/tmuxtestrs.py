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

SESSION = "td-test-rs"

HOLD = 3.0


def quote(command: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in command)


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

    makefile = os.environ.get("CARGO_MAKE_MAKEFILE_PATH", "Makefile.toml")
    profile = os.environ.get("CARGO_MAKE_PROFILE", "development")
    base = [
        "cargo",
        "make",
        "--disable-check-for-updates",
        "--no-on-error",
        "--loglevel=info",
        f"--profile={profile}",
        "--allow-private",
        "--skip-init-end-tasks",
        "--makefile",
        makefile,
    ]

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

    outcome = 0
    unknown = []
    for name in names:
        marker = os.path.join(codes, f"{name}.code")
        try:
            with open(marker, encoding="utf-8") as handle:
                code = int(handle.read().strip() or 0)
        except (OSError, ValueError):
            unknown.append(name)
            continue
        if code and not outcome:
            outcome = code
    shutil.rmtree(codes, ignore_errors=True)

    if unknown:
        print(
            f"No exit code reported by: {', '.join(unknown)}; treating as failed.",
            file=sys.stderr,
        )
        return outcome or 1
    return outcome


if __name__ == "__main__":
    sys.exit(main())
