#
# Copyright 2025 Tabs Data Inc.
#

import sys

import yaml

START_TAG = "<mount><i>"
END_TAG = "<mount><f>"


def get_resolved_mounts(id: str, options: dict) -> dict:
    resolved_mounts = {}
    for key, value in options.items():
        variable_name = f"{id}_{key}".upper()
        variable_value = value
        resolved_mounts[variable_name] = variable_value
    return resolved_mounts


def main(piped_input: str):
    data = yaml.safe_load(piped_input)
    mount_list = data.get("storage", {}).get("mounts", [])
    resolved_mounts = {}
    for mount in mount_list:
        if "options" in mount:
            resolved_mounts.update(get_resolved_mounts(mount["id"], mount["options"]))
    return resolved_mounts


if __name__ == "__main__":
    result = main(sys.stdin.read())
    sys.stdout.write(START_TAG)
    yaml.dump(result, sys.stdout)
    sys.stdout.write(END_TAG)
