#
#  Copyright 2025 Tabs Data Inc.
#

import sys
from pathlib import Path

import yaml


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: python testing_instance.py <yaml_path> <instance_name>",
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
    if instance_name in data:
        data = data[instance_name]
    else:
        sys.exit(0)

    for root, values in data.items():
        print("--")
        print(root)
        if values is not None and isinstance(values, dict):
            for key, value in values.items():
                print(f"--{key}")
                print(str(value))


if __name__ == "__main__":
    main()
