#
# Copyright 2026 Tabsdata Inc.
#

import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path
from types import ModuleType

BUILD_FINISHED = "HTML book written to"
PAGEFIND_GLOB = "{index.html,chapters/**/*.html,book.os/**/*.html}"


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


def tool(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        logger.error(f"{name} not found ('cargo make permit' installs it).")
        sys.exit(1)
    return path


def serve(book: Path, environment: dict[str, str]) -> int:
    mdbook = tool("mdbook")
    pagefind = shutil.which("pagefind")
    if pagefind is None:
        logger.warning(
            "pagefind not found; serving the book without search"
            " ('cargo make permit' installs it)."
        )
        return subprocess.run(
            [mdbook, "serve", "--open"], cwd=book, env=environment
        ).returncode
    with subprocess.Popen(
        [mdbook, "serve", "--open"],
        cwd=book,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    ) as server:
        for line in server.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            if BUILD_FINISHED in line:
                subprocess.run(
                    [
                        pagefind,
                        "--site",
                        "book",
                        "--root-selector",
                        "main",
                        "--glob",
                        PAGEFIND_GLOB,
                        "--silent",
                    ],
                    cwd=book,
                    env=environment,
                )
        return server.wait()


def main() -> int:
    book = Path(sys.argv[1])
    teds = sys.argv[2] if len(sys.argv) > 2 else ""
    environment = {**os.environ, "D2_NO_XML_TAG": "true"}
    if teds:
        subprocess.run([sys.executable, teds], env=environment, check=True)
    try:
        return serve(book, environment)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
