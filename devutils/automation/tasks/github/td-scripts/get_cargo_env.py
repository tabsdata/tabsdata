#
# Copyright 2026 Tabsdata Inc.
#

import os
import sys
import tomllib


def get(path, name):
    with open(path, "rb") as file:
        config = tomllib.load(file)
    value = config.get("env", {}).get(name)
    includes = config.get("include", [])
    if isinstance(includes, str):
        includes = [includes]
    for include in reversed(includes):
        if value is not None:
            break
        if not isinstance(include, dict):
            include = {"path": include}
        target = os.path.normpath(os.path.join(os.path.dirname(path), include["path"]))
        if include.get("optional", False) and not os.path.exists(target):
            continue
        value = get(target, name)
    return value["value"] if isinstance(value, dict) else value


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("Usage: get_cargo_env.py <config.toml> <variable>")
    config_path, variable = sys.argv[1], sys.argv[2]
    variable_value = get(config_path, variable)
    if variable_value is None:
        sys.exit(f"{variable} is not set in {config_path} or its includes")
    print(variable_value)
