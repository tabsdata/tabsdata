#
# Copyright 2025 Tabs Data Inc.
#

import sys

import yaml

START_TAG = "<message><i>"
END_TAG = "<message><f>"


def get_resolved_mounts(identifier: str, options: dict) -> dict:
    resolved_mounts = {}
    for key, value in options.items():
        variable_name = f"{identifier}_{key}".upper()
        variable_value = value
        resolved_mounts[variable_name] = variable_value
    return resolved_mounts


def resolve(input_data: str) -> dict[str, str]:
    parsed_data = yaml.safe_load(input_data)
    mount_list = parsed_data.get("storage", {}).get("mounts", [])
    resolved_mounts = {}
    for mount in mount_list:
        if "options" in mount:
            resolved_mounts.update(get_resolved_mounts(mount["id"], mount["options"]))
    return resolved_mounts


def main():
    input_data = sys.stdin.read()
    output_data = resolve(input_data)
    sys.stdout.write(START_TAG)
    yaml.dump(output_data, sys.stdout)
    sys.stdout.write(END_TAG)


if __name__ == "__main__":
    main()
