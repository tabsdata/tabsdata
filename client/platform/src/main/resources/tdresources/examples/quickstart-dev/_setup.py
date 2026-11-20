#
# Copyright 2026 Tabsdata Inc.
#

# `tdk examples extract` runs this from the extracted directory: each
# connection's base_path must be absolute for the functions to find their files.

from pathlib import Path

from ruamel.yaml import YAML

CONNECTION_DATA_DIRS = {
    "conn-input.yaml": "input-data",
    "conn-output.yaml": "output-data",
}


def main() -> None:
    root = Path.cwd()
    (root / "output-data").mkdir(exist_ok=True)

    yaml = YAML()
    yaml.width = 4096
    for connection_file, data_dir in CONNECTION_DATA_DIRS.items():
        path = root / connection_file
        document = yaml.load(path)
        document["spec"]["base_path"] = str((root / data_dir).resolve())
        with path.open("w", encoding="utf-8") as stream:
            yaml.dump(document, stream)


if __name__ == "__main__":
    main()
