#
#  Copyright 2025 Tabs Data Inc.
#

import sys
from pathlib import Path

import yaml


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: python testing_database.py <yaml_path> <instance_name>",
            file=sys.stderr,
        )
        sys.exit(1)

    yaml_path = Path(sys.argv[1])
    if not yaml_path.exists():
        print(
            f"Error: File '{yaml_path}' not found",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        with open(yaml_path, "r", encoding="utf-8") as yaml_file:
            data = yaml.safe_load(yaml_file)
    except Exception as e:
        print(
            f"Error parsing yaml {yaml_path}: {e}",
            file=sys.stderr,
        )
        sys.exit(1)

    if data is None:
        sys.exit(0)

    instance_name = sys.argv[2]
    instance = data.get(instance_name)
    if not isinstance(instance, dict):
        sys.exit(0)

    database = instance.get("database")
    if not isinstance(database, dict):
        sys.exit(0)

    port = database.get("port")
    if port is not None:
        print(port)


if __name__ == "__main__":
    main()
