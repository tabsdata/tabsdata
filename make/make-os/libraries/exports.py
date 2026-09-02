#
#  Copyright 2026 Tabsdata Inc.
#

import ast
import pathlib
import shutil
import subprocess
import sys


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: exports.py <manifest> <folder>", file=sys.stderr)
        return 2
    manifest, folder = sys.argv[1], pathlib.Path(sys.argv[2])

    tool = shutil.which("tdx-domain")
    if tool is None:
        print("Required tool tdx-domain not found.", file=sys.stderr)
        return 1

    rendered = subprocess.run(
        [tool, "info", "--manifest", manifest, "--format", "export"],
        capture_output=True,
        text=True,
    )
    if rendered.returncode != 0:
        sys.stderr.write(rendered.stderr)
        return rendered.returncode

    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir(parents=True)

    exported = 0
    for line in rendered.stdout.splitlines():
        line = line.strip()
        if not line.startswith("export "):
            continue
        name, separator, quoted = line[len("export ") :].partition("=")
        if not separator:
            continue
        try:
            value = ast.literal_eval(quoted)
        except (ValueError, SyntaxError) as error:
            print(f"Could not decode {name}: {error}", file=sys.stderr)
            return 1
        if not isinstance(value, str):
            value = str(value)
        (folder / name).write_bytes(value.encode("utf-8"))
        print(name)
        exported += 1

    if not exported:
        print(
            f"{tool} info exported no variables for {manifest}.",
            file=sys.stderr,
        )
        for stream, text in (("stdout", rendered.stdout), ("stderr", rendered.stderr)):
            text = text.strip()
            if text:
                print(f"--- {stream} ---\n{text}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
