#
# Copyright 2026 Tabsdata Inc.
#

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

SYMLINK_MODE = "120000"
SUBMODULE_MODE = "160000"
MAX_LINK_DEPTH = 32

TD_ON_GA = "TD_ON_GA"
TD_CHECK_DIRTINESS = "TD_CHECK_DIRTINESS"


def on_ga() -> bool:
    return os.environ.get(TD_ON_GA, "").strip() == "true"


def strict_dirtiness() -> bool:
    return on_ga() or os.environ.get(TD_CHECK_DIRTINESS, "").strip().lower() == "true"


@dataclass(frozen=True)
class Change:
    status: str
    path: str
    reason: str | None = None

    def __str__(self) -> str:
        if self.reason:
            return f"{self.status} {self.path} ({self.reason})"
        return f"{self.status} {self.path}"


@dataclass(frozen=True)
class IndexEntry:
    mode: str
    object: str
    path: str


def git(folder: Path, arguments: list[str]) -> bytes:
    completed = subprocess.run(
        ["git", "-C", str(folder), *arguments],
        capture_output=True,
        timeout=60,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"'git {' '.join(arguments)}' failed in '{folder}': "
            f"{completed.stderr.decode('utf-8', errors='replace').strip()}"
        )
    return completed.stdout


def split_records(output: bytes) -> list[str]:
    return [
        record.decode("utf-8", errors="replace")
        for record in output.split(b"\0")
        if record
    ]


def tracked_changes(repository: Path) -> list[Change]:
    records = iter(
        split_records(
            git(
                repository,
                ["status", "--porcelain=v1", "-z", "--untracked-files=no"],
            )
        )
    )
    changes = []
    for record in records:
        if len(record) < 4:
            continue
        status = record[:2]
        if "R" in status or "C" in status:
            next(records, None)
        changes.append(Change(status=status, path=record[3:]))
    return changes


def index_entries(folder: Path, pathspec: str) -> list[IndexEntry]:
    entries = []
    for record in split_records(
        git(folder, ["ls-files", "--stage", "-z", "--", pathspec])
    ):
        header, separator, path = record.partition("\t")
        fields = header.split()
        if separator and len(fields) >= 2:
            entries.append(IndexEntry(mode=fields[0], object=fields[1], path=path))
    return entries


def exists_without_following(path: Path) -> bool:
    return os.path.lexists(path)


def same_file(materialized: Path, source: Path) -> str | None:
    try:
        source_content = source.read_bytes()
    except FileNotFoundError:
        if exists_without_following(materialized):
            return f"'{source}' does not exist"
        return None
    try:
        content = materialized.read_bytes()
    except FileNotFoundError:
        return f"'{materialized}' is missing"
    if content != source_content:
        return f"'{materialized}' differs from '{source}'"
    return None


def same_link(materialized: Path, source: Path) -> bool:
    return (
        materialized.is_symlink()
        and source.is_symlink()
        and os.readlink(materialized) == os.readlink(source)
    )


def compare_materialization(materialized: Path, source: Path, depth: int) -> str | None:
    if depth > MAX_LINK_DEPTH:
        return f"symbolic link chain at '{source}' is too deep"
    try:
        source = source.resolve(strict=True)
    except (FileNotFoundError, RuntimeError, OSError):
        return f"symbolic link target '{source}' does not exist"
    if source.is_file():
        return same_file(materialized, source)
    if not source.is_dir():
        return f"symbolic link target '{source}' is neither a file nor a folder"
    if not materialized.is_dir():
        return (
            f"'{materialized}' is not a folder, "
            f"but its symbolic link target '{source}' is"
        )
    for entry in index_entries(source, "."):
        if entry.mode == SUBMODULE_MODE:
            continue
        materialized_entry = materialized / entry.path
        source_entry = source / entry.path
        if entry.mode == SYMLINK_MODE:
            if same_link(materialized_entry, source_entry):
                difference = None
            else:
                difference = compare_materialization(
                    materialized_entry, source_entry, depth + 1
                )
        else:
            difference = same_file(materialized_entry, source_entry)
        if difference is not None:
            return difference
    return None


MATERIALIZED_VERDICT = object()


def materialization_verdict(repository: Path, change: Change):
    staged, unstaged = change.status[0], change.status[1]
    if staged != " " or unstaged not in ("T", "D"):
        return None
    entry = next(
        (
            entry
            for entry in index_entries(repository, change.path)
            if entry.path == change.path
        ),
        None,
    )
    if entry is None or entry.mode != SYMLINK_MODE:
        return None
    materialized = repository / change.path
    if not exists_without_following(materialized):
        return "symbolic link was deleted"
    if materialized.is_symlink():
        return "symbolic link was retargeted"
    target = (
        git(repository, ["cat-file", "blob", entry.object])
        .decode("utf-8", errors="replace")
        .rstrip("\n")
    )
    difference = compare_materialization(materialized, materialized.parent / target, 0)
    if difference is None:
        return MATERIALIZED_VERDICT
    return f"materialized symbolic link differs: {difference}"


def changed_tracked_files(repository: Path, strict: bool) -> list[Change]:
    repository = Path(repository)
    changes = tracked_changes(repository)
    if not strict:
        return changes
    real_changes = []
    for change in changes:
        verdict = materialization_verdict(repository, change)
        if verdict is MATERIALIZED_VERDICT:
            continue
        real_changes.append(
            Change(status=change.status, path=change.path, reason=verdict)
        )
    return real_changes


def repository_dirty(repository_name: str, repository: Path, strict: bool) -> str:
    if not strict:
        try:
            return "true" if changed_tracked_files(repository, False) else "false"
        except Exception:
            return "-"
    changes = changed_tracked_files(repository, True)
    if changes:
        listing = "\n".join(f"   {change}" for change in changes)
        raise RuntimeError(
            f"Repository '{repository_name}' at '{repository}' has "
            f"{len(changes)} modified tracked file(s). GA builds (and builds with "
            f"{TD_CHECK_DIRTINESS}=true) must not modify tracked files; review "
            f"which step changed them:\n{listing}"
        )
    return "false"
