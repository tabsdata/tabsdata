#
# Copyright 2026 Tabs Data Inc.
#

import importlib
import importlib.util
import json
import os
import shlex
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType

from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)


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


sys.stdout.reconfigure(encoding="utf-8")


THESEUS_REPOSITORY = "theseus-rs/postgresql-binaries"
RELEASES_API = f"https://api.github.com/repos/{THESEUS_REPOSITORY}/releases/latest"
RELEASE_DOWNLOAD = (
    f"https://github.com/{THESEUS_REPOSITORY}/releases/download"
    "/{version}/postgresql-{version}-{target}.tar.gz"
)

DOWNLOAD_ATTEMPTS = 8
DOWNLOAD_BACKOFF_SECONDS = 4
DOWNLOAD_CHUNK_BYTES = 64 * 1024
CONNECT_TIMEOUT_SECONDS = 32
REQUEST_TIMEOUT_SECONDS = 128


@dataclass
class Report:
    target_triple: str | None = None
    target_triple_source: str | None = None
    target_triple_td_target: str | None = None
    target_triple_rustc: str | None = None
    version_pin_environment: str | None = None
    version: str | None = None
    version_source: str | None = None
    releases_api: str = field(default_factory=lambda: RELEASES_API)
    authenticated: bool = field(default_factory=lambda: GITHUB_TOKEN is not None)
    cache_directory: Path | None = None
    archive_filename: str | None = None
    archive_path: Path | None = None
    archive_already_cached: bool | None = None
    archive_size_bytes: int | None = None
    download_url: str | None = None
    download_attempts_used: int = 0
    version_marker_path: Path | None = None
    version_marker_written: bool = False
    outcome: str = "not started"
    error: str | None = None
    warnings: list[str] = field(default_factory=list)

    def warn(self, message: str) -> None:
        self.warnings.append(message)

    def emit(self) -> None:
        caption_width = 28

        def row(caption: str, value: object) -> str:
            padded = (caption + " ").ljust(caption_width, ".")
            return f"{padded}: {value}"

        header = "===== PostgreSQL cache priming report ====="
        footer = "=" * (caption_width + 1)

        lines: list[str] = []
        lines.append("♻️ Priming embedded PostgreSQL archive cache")
        lines.append(header)
        lines.append(row("outcome", self.outcome))
        lines.append(row("target triple", self.target_triple or "<unresolved>"))
        lines.append(
            row("target triple source", self.target_triple_source or "<unresolved>")
        )
        lines.append(
            row("target triple (td_target)", self.target_triple_td_target or "<unset>")
        )
        lines.append(
            row("target triple (rustc)", self.target_triple_rustc or "<unresolved>")
        )
        lines.append(row("resolved version", self.version or "<unresolved>"))
        lines.append(row("version source", self.version_source or "<unresolved>"))
        if self.version_pin_environment:
            lines.append(
                row(
                    "version pin (env)",
                    f"TD_POSTGRESQL_VERSION={self.version_pin_environment}",
                )
            )
        lines.append(row("theseus releases api", self.releases_api))
        lines.append(
            row(
                "github authentication",
                "gh auth token" if self.authenticated else "<unauthenticated>",
            )
        )
        lines.append(row("cache directory", self.cache_directory or "<unresolved>"))
        lines.append(row("archive filename", self.archive_filename or "<unresolved>"))
        lines.append(row("archive path", self.archive_path or "<unresolved>"))
        if self.archive_already_cached is None:
            lines.append(row("archive already cached", "<unknown>"))
        else:
            lines.append(row("archive already cached", self.archive_already_cached))
        if self.archive_size_bytes is not None:
            lines.append(row("archive size (bytes)", self.archive_size_bytes))
        if self.download_url:
            lines.append(row("download url", self.download_url))
            lines.append(
                row(
                    "download attempts used",
                    f"{self.download_attempts_used}/{DOWNLOAD_ATTEMPTS}",
                )
            )
        lines.append(
            row("version marker path", self.version_marker_path or "<not provided>")
        )
        lines.append(row("version marker written", self.version_marker_written))
        if self.outcome == "success" and self.version:
            lines.append(
                row("environment pin to apply", f"POSTGRESQL_VERSION==={self.version}")
            )
        if self.warnings:
            lines.append("warnings:")
            for warning in self.warnings:
                lines.append(f"  - {warning}")
        if self.error:
            lines.append(row("error", self.error))
        lines.append(footer)

        emit_logger = logger.error if self.error else logger.info
        for line in lines:
            emit_logger(line)


def resolve_target_triple(report: Report) -> str:
    td_target = os.getenv("TD_TARGET")
    if td_target:
        report.target_triple_td_target = td_target

    rustc_target: str | None = None
    try:
        completed = subprocess.run(
            ["rustc", "-vV"],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (
        FileNotFoundError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
    ) as error:
        if not td_target:
            raise RuntimeError(
                f"Cannot determine target triple: 'rustc -vV' failed: {error}"
            ) from error
    else:
        for line in completed.stdout.splitlines():
            if line.startswith("host:"):
                host = line.split(":", 1)[1].strip()
                if host:
                    rustc_target = host
                    break
        if rustc_target is None and not td_target:
            raise RuntimeError(
                "Cannot determine target triple: 'rustc -vV' output had no"
                f" 'host:' line:\n{completed.stdout}"
            )

    report.target_triple_rustc = rustc_target

    if td_target:
        report.target_triple = td_target
        report.target_triple_source = "td_target"
    else:
        report.target_triple = rustc_target
        report.target_triple_source = "rustc"
    return report.target_triple


def resolve_github_token() -> str | None:
    try:
        completed = subprocess.run(
            ["gh", "auth", "token"],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (
        FileNotFoundError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
    ):
        return None
    token = completed.stdout.strip()
    return token or None


GITHUB_TOKEN = resolve_github_token()


def equivalent_curl(
    url: str, accept: str | None = None, output: Path | None = None
) -> str:
    parts = ["curl"]
    parts += ["--fail"]
    parts += ["--location"]
    parts += ["--show-error"]
    parts += ["--retry", str(DOWNLOAD_ATTEMPTS)]
    parts += ["--retry-delay", str(DOWNLOAD_BACKOFF_SECONDS)]
    parts += ["--retry-all-errors"]
    parts += ["--connect-timeout", str(CONNECT_TIMEOUT_SECONDS)]
    parts += ["--max-time", str(REQUEST_TIMEOUT_SECONDS)]
    if accept:
        parts += ["-H", f"Accept: {accept}"]
    if GITHUB_TOKEN:
        parts += ["-H", "Authorization: Bearer $(gh auth token)"]
    if output is not None:
        parts += ["-o", str(output)]
    parts.append(url)
    return " ".join(shlex.quote(part) for part in parts)


def open_url(url: str, accept: str | None = None):
    request = urllib.request.Request(url)
    if accept:
        request.add_header("Accept", accept)
    if GITHUB_TOKEN:
        request.add_header("Authorization", f"Bearer {GITHUB_TOKEN}")
    context = ssl.create_default_context()
    return urllib.request.urlopen(
        request, timeout=REQUEST_TIMEOUT_SECONDS, context=context
    )


def fetch_latest_version(report: Report) -> str:
    pinned = os.getenv("TD_POSTGRESQL_VERSION")
    if pinned:
        report.version_pin_environment = pinned
        report.version = pinned
        report.version_source = "TD_POSTGRESQL_VERSION (environment pin)"
        logger.debug(
            f"Using PostgreSQL version pin from TD_POSTGRESQL_VERSION: {pinned}"
        )
        return pinned

    logger.debug(f"Resolving latest PostgreSQL version from: {RELEASES_API}")
    last_error: Exception | None = None
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        TimeElapsedColumn(),
        transient=False,
    ) as progress:
        for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
            description = (
                f"resolve latest version attempt {attempt}/{DOWNLOAD_ATTEMPTS}"
            )
            task = progress.add_task(description, total=None)
            try:
                with open_url(
                    RELEASES_API, accept="application/vnd.github+json"
                ) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                tag = payload.get("tag_name")
                if not tag:
                    raise RuntimeError("GitHub API response missing tag_name.")
                progress.update(task, description=f"{description} ok ({tag})")
                progress.stop_task(task)
                report.version = tag
                report.version_source = f"theseus-rs latest release ({RELEASES_API})"
                logger.debug(f"Resolved latest theseus-rs PostgreSQL release: {tag}")
                return tag
            except (
                urllib.error.URLError,
                TimeoutError,
                json.JSONDecodeError,
                RuntimeError,
            ) as error:
                last_error = error
                progress.update(task, description=f"{description} FAILED")
                progress.stop_task(task)
                report.warn(
                    f"version resolution attempt {attempt}/{DOWNLOAD_ATTEMPTS}"
                    f" failed: {error}"
                )
                if attempt < DOWNLOAD_ATTEMPTS:
                    time.sleep(DOWNLOAD_BACKOFF_SECONDS * attempt)
    report.warn(
        "equivalent command to reproduce version-resolution failure:"
        f" {equivalent_curl(RELEASES_API, accept='application/vnd.github+json')}"
    )
    raise RuntimeError(
        "Could not resolve latest PostgreSQL release after"
        f" {DOWNLOAD_ATTEMPTS} attempts: {last_error}"
    )


def download(url: str, destination: Path, report: Report) -> None:
    temporary = destination.with_suffix(destination.suffix + ".partial")
    if temporary.exists():
        temporary.unlink()

    last_error: Exception | None = None
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        transient=False,
    ) as progress:
        for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
            report.download_attempts_used = attempt
            description = f"download attempt {attempt}/{DOWNLOAD_ATTEMPTS}"
            task = progress.add_task(description, total=None)
            logger.debug(
                "Downloading PostgreSQL archive"
                f" (attempt {attempt}/{DOWNLOAD_ATTEMPTS}): {url}"
            )
            try:
                with open_url(url) as response, open(temporary, "wb") as output_file:
                    content_length = response.headers.get("Content-Length")
                    if content_length:
                        try:
                            progress.update(task, total=int(content_length))
                        except ValueError:
                            pass
                    while True:
                        chunk = response.read(DOWNLOAD_CHUNK_BYTES)
                        if not chunk:
                            break
                        output_file.write(chunk)
                        progress.update(task, advance=len(chunk))
                progress.update(task, description=f"{description} ok")
                progress.stop_task(task)
                temporary.replace(destination)
                report.archive_size_bytes = destination.stat().st_size
                logger.debug(f"PostgreSQL archive cached at: {destination}")
                return
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                last_error = error
                progress.update(task, description=f"{description} FAILED")
                progress.stop_task(task)
                report.warn(
                    f"download attempt {attempt}/{DOWNLOAD_ATTEMPTS} failed: {error}"
                )
                if temporary.exists():
                    temporary.unlink()
                if attempt < DOWNLOAD_ATTEMPTS:
                    time.sleep(DOWNLOAD_BACKOFF_SECONDS * attempt)

    report.warn(
        "equivalent command to reproduce download failure:"
        f" {equivalent_curl(url, output=destination)}"
    )
    raise RuntimeError(
        f"Could not download PostgreSQL archive after {DOWNLOAD_ATTEMPTS} attempts:"
        f" {last_error}"
    )


def cache_directory() -> Path:
    return Path.home() / ".theseus" / "postgresql"


def run(report: Report) -> None:
    target = resolve_target_triple(report)
    version = fetch_latest_version(report)

    cache_directory_path = cache_directory()
    report.cache_directory = cache_directory_path
    cache_directory_path.mkdir(parents=True, exist_ok=True)

    archive_name = f"postgresql-{version}-{target}.tar.gz"
    archive_path = cache_directory_path / archive_name
    report.archive_filename = archive_name
    report.archive_path = archive_path

    if archive_path.is_file() and archive_path.stat().st_size > 0:
        report.archive_already_cached = True
        report.archive_size_bytes = archive_path.stat().st_size
    else:
        report.archive_already_cached = False
        url = RELEASE_DOWNLOAD.format(version=version, target=target)
        report.download_url = url
        download(url, archive_path, report)

    version_marker = sys.argv[1] if len(sys.argv) > 1 else None
    if version_marker:
        marker_path = Path(version_marker)
        report.version_marker_path = marker_path
        marker_path.write_text(version, encoding="utf-8")
        report.version_marker_written = True


def main() -> int:
    report = Report()
    exit_code = 0
    try:
        run(report)
        report.outcome = "success"
    except Exception as error:
        report.outcome = "failed"
        report.error = str(error)
        exit_code = 1
    finally:
        report.emit()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
