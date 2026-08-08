#
# Copyright 2026 Tabsdata Inc.
#

"""Audit copyright headers across a repository checkout.

Walks every git-tracked file whose format supports comments and reports the
ones with no header, with a header naming an organization other than
``Tabsdata Inc.``, or with a header that deviates from the canonical form.
"""

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path

ORGANIZATION = "Tabsdata Inc."
CANONICAL_FORM = f"Copyright <year> {ORGANIZATION}"

HEAD_LINES = 15

HASH = "#"
SLASH = "//"
DASH = "--"
MARKUP = "<!--"
BLOCK = "/*"

COMMENT_BY_EXTENSION = {
    "bash": HASH,
    "c": SLASH,
    "cfg": HASH,
    "cjs": SLASH,
    "cmake": HASH,
    "cmd": HASH,
    "coveragerc": HASH,
    "cpp": SLASH,
    "cs": SLASH,
    "css": BLOCK,
    "d2": HASH,
    "ddl": DASH,
    "env": HASH,
    "fish": HASH,
    "go": SLASH,
    "gradle": SLASH,
    "graphql": HASH,
    "h": SLASH,
    "hpp": SLASH,
    "htm": MARKUP,
    "html": MARKUP,
    "ini": HASH,
    "java": SLASH,
    "js": SLASH,
    "jsx": SLASH,
    "kt": SLASH,
    "less": BLOCK,
    "lua": DASH,
    "md": MARKUP,
    "mjs": SLASH,
    "php": SLASH,
    "pl": HASH,
    "properties": HASH,
    "proto": SLASH,
    "ps1": HASH,
    "py": HASH,
    "pyi": HASH,
    "pyx": HASH,
    "r": HASH,
    "rb": HASH,
    "requirements": HASH,
    "rs": SLASH,
    "scala": SLASH,
    "scss": BLOCK,
    "sh": HASH,
    "sql": DASH,
    "svg": MARKUP,
    "swift": SLASH,
    "tf": HASH,
    "tmpl": HASH,
    "toml": HASH,
    "ts": SLASH,
    "tsx": SLASH,
    "txt": HASH,
    "vue": MARKUP,
    "xml": MARKUP,
    "yaml": HASH,
    "yml": HASH,
    "zsh": HASH,
}

COMMENT_BY_FILENAME = {
    ".dockerignore": HASH,
    ".editorconfig": HASH,
    ".gitattributes": HASH,
    ".gitignore": HASH,
    ".npmrc": HASH,
    "dockerfile": HASH,
    "justfile": HASH,
    "makefile": HASH,
}

DEFAULT_EXCLUDES = (
    "*_generated/*",
    "*.lock",
    "variant/resources/lock/*",
    "~noop",
    ".gitkeep",
    ".ignore",
    ".tdkeep",
    "LICENSE*",
    "NOTICE*",
    "THIRD-PARTY*",
    # Vendor product logos: a Tabsdata header would misattribute a third-party mark.
    "*/resources/*.svg",
)

MISSING = "missing"
WRONG_ORGANIZATION = "wrong-organization"
MALFORMED = "malformed"

ISSUE_ORDER = (MISSING, WRONG_ORGANIZATION, MALFORMED)

ISSUE_LABELS = {
    MISSING: "No copyright header",
    WRONG_ORGANIZATION: f"Header does not name {ORGANIZATION}",
    MALFORMED: f"Header names {ORGANIZATION} but deviates from the canonical form",
}

COPYRIGHT = re.compile(r"\bcopyright\b", re.IGNORECASE)
PLACEHOLDER = re.compile(r"__COPYRIGHT__")
YEARS = r"\d{4}(?:\s*[-,]\s*\d{4})*"
CANONICAL = re.compile(rf"Copyright\s+(?:{YEARS}|\{{[^{{}}]+}})\s+Tabsdata\s+Inc\.")
ORGANIZATION_FORM = re.compile(r"Tabsdata\s+Inc\.")


@dataclass(frozen=True)
class Finding:
    path: str
    issue: str
    line: int | None
    text: str | None


def comment_style(path: Path, first_line: str | None) -> str | None:
    style = COMMENT_BY_FILENAME.get(path.name.lower())
    if style:
        return style
    extension = path.name.rsplit(".", 1)[-1] if "." in path.name[1:] else ""
    style = COMMENT_BY_EXTENSION.get(extension.lower())
    if style:
        return style
    if first_line and first_line.startswith("#!"):
        return HASH
    return None


def is_excluded(relative: str, patterns: tuple[str, ...]) -> bool:
    name = relative.rsplit("/", 1)[-1]
    return any(fnmatch(relative, p) or fnmatch(name, p) for p in patterns)


def is_selected(relative: str, prefixes: tuple[str, ...]) -> bool:
    if not prefixes:
        return True
    return any(
        relative == prefix or relative.startswith(f"{prefix}/") for prefix in prefixes
    )


def git_files(root: Path, untracked: bool) -> list[str]:
    command = ["git", "-C", str(root), "ls-files", "-z"]
    if untracked:
        command += ["--cached", "--others", "--exclude-standard"]
    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.returncode != 0:
        raise SystemExit(f"git ls-files failed in {root}: {completed.stderr.strip()}")
    return sorted({name for name in completed.stdout.split("\0") if name})


def head_lines(path: Path, limit: int) -> list[str] | None:
    try:
        with path.open("r", encoding="utf-8") as stream:
            return [line for _, line in zip(range(limit), stream)]
    except (OSError, UnicodeDecodeError):
        return None


def inspect(lines: list[str]) -> list[tuple[str, int | None, str | None]]:
    canonical = False
    outcomes: list[tuple[str, int | None, str | None]] = []
    for number, line in enumerate(lines, start=1):
        if PLACEHOLDER.search(line):
            canonical = True
            continue
        if not COPYRIGHT.search(line):
            continue
        if CANONICAL.search(line):
            canonical = True
            continue
        issue = MALFORMED if ORGANIZATION_FORM.search(line) else WRONG_ORGANIZATION
        outcomes.append((issue, number, line.strip()))
    if not canonical and not outcomes:
        return [(MISSING, None, None)]
    return outcomes


def audit(
    root: Path,
    prefixes: tuple[str, ...],
    extensions: tuple[str, ...],
    excludes: tuple[str, ...],
    limit: int,
    symlinks: bool,
    untracked: bool,
) -> tuple[list[Finding], int]:
    findings: list[Finding] = []
    scanned = 0
    for relative in git_files(root, untracked):
        if not is_selected(relative, prefixes) or is_excluded(relative, excludes):
            continue
        path = root / relative
        if path.is_symlink() and not symlinks:
            continue
        if not path.is_file():
            continue
        lines = head_lines(path, limit)
        if lines is None:
            continue
        style = comment_style(path, lines[0] if lines else None)
        if style is None:
            continue
        if extensions and path.name.rsplit(".", 1)[-1].lower() not in extensions:
            continue
        scanned += 1
        for issue, number, text in inspect(lines):
            findings.append(Finding(relative, issue, number, text))
    return findings, scanned


def report_text(findings: list[Finding], scanned: int) -> None:
    for issue in ISSUE_ORDER:
        group = [finding for finding in findings if finding.issue == issue]
        if not group:
            continue
        print(f"{ISSUE_LABELS[issue]} ({len(group)}):")
        for finding in group:
            if finding.line is None:
                print(f"  {finding.path}")
            else:
                print(f"  {finding.path}:{finding.line}: {finding.text}")
        print("")

    counts: dict[str, int] = {}
    for finding in findings:
        extension = finding.path.rsplit("/", 1)[-1]
        extension = extension.rsplit(".", 1)[-1] if "." in extension[1:] else "<none>"
        counts[extension] = counts.get(extension, 0) + 1
    if counts:
        by_extension = ", ".join(
            f"{extension}={count}"
            for extension, count in sorted(
                counts.items(), key=lambda item: (-item[1], item[0])
            )
        )
        print(f"By format: {by_extension}")

    print(f"Expected form: {CANONICAL_FORM}")
    print(f"Scanned {scanned} commentable files, {len(findings)} need attention.")


def report_json(findings: list[Finding], scanned: int) -> None:
    payload = {
        "expected": CANONICAL_FORM,
        "scanned": scanned,
        "findings": [
            {
                "path": finding.path,
                "issue": finding.issue,
                "line": finding.line,
                "text": finding.text,
            }
            for finding in findings
        ],
    }
    print(json.dumps(payload, indent=2))


def list_formats() -> None:
    print(f"Expected form: {CANONICAL_FORM}")
    print("")
    print("By extension:")
    for extension, style in sorted(COMMENT_BY_EXTENSION.items()):
        print(f"  .{extension:<14} {style}")
    print("")
    print("By file name:")
    for name, style in sorted(COMMENT_BY_FILENAME.items()):
        print(f"  {name:<15} {style}")
    print("")
    print("Extensionless files are checked when they start with a shebang.")
    print("")
    print("Excluded by default:")
    for pattern in DEFAULT_EXCLUDES:
        print(f"  {pattern}")


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tdx-copyright",
        description=(
            "Report files whose copyright header is missing or does not match "
            f"'{CANONICAL_FORM}'."
        ),
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="restrict the scan to these paths, relative to the repository root",
    )
    parser.add_argument(
        "--root",
        default=".",
        help="repository checkout to scan (default: the current folder)",
    )
    parser.add_argument(
        "--ext",
        action="append",
        default=[],
        metavar="EXT",
        help="only check this extension; repeatable",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        metavar="GLOB",
        help="skip paths matching this glob; repeatable",
    )
    parser.add_argument(
        "--no-default-excludes",
        action="store_true",
        help="also check generated, lock and third-party files",
    )
    parser.add_argument(
        "--head-lines",
        type=int,
        default=HEAD_LINES,
        help=f"how many leading lines to search (default: {HEAD_LINES})",
    )
    parser.add_argument(
        "--symlinks",
        action="store_true",
        help="follow symlinks into sibling repositories instead of skipping them",
    )
    parser.add_argument(
        "--untracked",
        action="store_true",
        help="also check untracked files that git would not ignore",
    )
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument(
        "--list-formats",
        action="store_true",
        help="print the formats that are checked and exit",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.list_formats:
        list_formats()
        return 0

    root = Path(args.root).resolve()
    if not root.is_dir():
        raise SystemExit(f"not a folder: {root}")

    prefixes = tuple(Path(path).as_posix().strip("/") for path in args.paths)
    extensions = tuple(ext.lstrip(".").lower() for ext in args.ext)
    excludes = tuple(args.exclude)
    if not args.no_default_excludes:
        excludes += DEFAULT_EXCLUDES

    findings, scanned = audit(
        root=root,
        prefixes=prefixes,
        extensions=extensions,
        excludes=excludes,
        limit=max(1, args.head_lines),
        symlinks=args.symlinks,
        untracked=args.untracked,
    )
    findings.sort(key=lambda finding: (ISSUE_ORDER.index(finding.issue), finding.path))

    if args.format == "json":
        report_json(findings, scanned)
    else:
        report_text(findings, scanned)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
