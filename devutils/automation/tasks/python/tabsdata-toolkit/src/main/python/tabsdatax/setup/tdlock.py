#
# Copyright 2025 Tabs Data Inc.
#

import subprocess
from datetime import datetime


def write_requirements_lock(root_path, output_path):
    requirements = root_path / "requirements" / "requirements-third-party-all.txt"
    try:
        uv_output = subprocess.run(
            [
                "uv",
                "pip",
                "compile",
                "--no-annotate",
                "--no-header",
                "--quiet",
                "--format",
                "requirements.txt",
                "--output-file",
                output_path,
                requirements,
            ],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=root_path,
        )
        if uv_output.returncode == 0:
            with open(output_path, "r") as f:
                content = f.read()
            year = datetime.now().year
            header = f"#\n# Copyright {year} Tabs Data Inc.\n#\n\n"
            with open(output_path, "w") as f:
                f.write(header + content)
            return
    except Exception as exception:
        raise RuntimeError(
            f"Failed to write requirements lock to {output_path}: {exception}"
        ) from exception
