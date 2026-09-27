#
# Copyright 2026 Tabsdata Inc.
#

import shutil
import subprocess
from pathlib import Path

from git import BadName, InvalidGitRepositoryError, NoSuchPathError, Repo

from tabsdatax.cmake.kupgrader.common.errors import KupgraderError

KPROFILE_SUBPATH = Path("variant") / "resources" / "kprofile"
PATCHES_SUBPATH = Path("variant") / "config" / "kupgrade" / "patches"


class SourceError(KupgraderError):
    pass


def reset_folder(folder: Path) -> None:
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir(parents=True)


def open_repository(root_folder: Path) -> Repo:
    try:
        return Repo(root_folder)
    except (InvalidGitRepositoryError, NoSuchPathError) as error:
        raise SourceError(
            f"The folder '{root_folder}' is not a git repository."
        ) from error


def commit_of(root_folder: Path, tag: str):
    repository = open_repository(root_folder)
    try:
        return repository.commit(tag)
    except (BadName, ValueError) as error:
        raise SourceError(
            f"The tag '{tag}' does not exist in '{root_folder}'. Fetch the tags with "
            f"'git fetch --tags' and run the task again."
        ) from error


def resolve_tag(root_folder: Path, tag: str) -> str:
    return commit_of(root_folder, tag).hexsha


def extract_kprofile(root_folder: Path, tag: str, target_folder: Path) -> Path:
    commit = commit_of(root_folder, tag)

    try:
        tree = commit.tree / str(KPROFILE_SUBPATH)
    except KeyError as error:
        raise SourceError(
            f"The tag '{tag}' holds no '{KPROFILE_SUBPATH}' folder."
        ) from error

    reset_folder(target_folder)
    for blob in tree.traverse():
        if blob.type != "blob":
            continue
        target = target_folder / Path(blob.path).relative_to(KPROFILE_SUBPATH)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(blob.data_stream.read())
    return target_folder


def apply_published_patch(root_folder: Path, tag: str, kprofile_folder: Path) -> None:
    patch = root_folder / PATCHES_SUBPATH / f"{tag}.patch"
    if not patch.is_file():
        return
    completed = subprocess.run(
        ["git", "apply", "--unsafe-paths", "-p1", str(patch)],
        cwd=kprofile_folder,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise SourceError(
            f"The patch '{patch}' does not apply to the kprofile of tag '{tag}': "
            f"{completed.stderr.strip()[:400]}"
        )


def copy_kprofile(root_folder: Path, target_folder: Path) -> Path:
    source = root_folder / KPROFILE_SUBPATH
    if not source.is_dir():
        raise SourceError(f"The kprofile folder '{source}' does not exist.")
    if target_folder.exists():
        shutil.rmtree(target_folder)
    target_folder.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, target_folder)
    return target_folder
