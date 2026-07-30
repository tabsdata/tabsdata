#
# Copyright 2026 Tabsdata Inc.
#

# Quickstart setup step. Run automatically by `tdk examples extract` from
# inside the extracted directory, with no arguments. It creates the
# output-data/ folder and points each connection file's `base_path` at the
# absolute location of its data folder, so the functions resolve their files
# regardless of where the example was extracted.

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
        yaml.dump(document, path)


if __name__ == "__main__":
    main()
