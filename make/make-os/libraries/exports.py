#
#  Copyright 2026 Tabsdata Inc.
#

import json
import shlex
import subprocess
import sys


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: exports.py <tdd-exe> <manifest>", file=sys.stderr)
        return 2
    tool, manifest = sys.argv[1], sys.argv[2]

    rendered = subprocess.run(
        [tool, "domain", "info", "--manifest", manifest, "--format", "export"],
        capture_output=True,
        text=True,
    )
    if rendered.returncode != 0:
        sys.stderr.write(rendered.stderr)
        return rendered.returncode

    exported: dict[str, str] = {}
    for line in rendered.stdout.splitlines():
        line = line.strip()
        if not line.startswith("export "):
            continue
        name, separator, quoted = line[len("export ") :].partition("=")
        if not separator:
            continue
        try:
            parts = shlex.split(quoted)
        except ValueError as error:
            print(f"Could not decode {name}: {error}", file=sys.stderr)
            return 1
        exported[name] = parts[0] if parts else ""

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

    print(json.dumps(exported))
    return 0


if __name__ == "__main__":
    sys.exit(main())
