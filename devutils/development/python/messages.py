#
# Copyright 2024 Tabs Data Inc.
#

# Disclaimer: This is an experimental script not for use in production environments.
# There might be some hard-code values that you might need to adjust to your current
# setup. You might also need to install some additional Python packages to run it.

import argparse
import getpass
import os

from uuid_v7.base import uuid7

message_template = """class: ephemeral
worker: dataset
action: start
arguments:
  - {}
  - --context-folder {}
"""

script_folder = os.path.dirname(os.path.abspath(__file__))
user_name = getpass.getuser()

context_path = os.path.abspath(
    os.path.join(
        script_folder,
        f"../../.{user_name}/tabsdata/"
        f"{user_name}/repository/dataspace/"
        f"{user_name}_store/"
        f"{user_name}_set/function",
    )
)
workspace_path = os.path.abspath(
    os.path.join(
        script_folder, f"../../.{user_name}/tabsdata/{user_name}/workbench/messages"
    )
)
script_path = os.path.abspath(
    os.path.join(script_folder, "../../../tabsdata/tabsserver/main.py")
)


def generate_messages(quantity, directory, script, context):
    if not os.path.exists(directory):
        os.makedirs(directory)
    content = message_template.format(script, context)
    for _ in range(quantity):
        message = os.path.join(directory, f"{uuid7()}.yaml")
        with open(message, "w") as file:
            file.write(content)
        print(f"Generated {message}")


def main():
    parser = argparse.ArgumentParser(description="Generate dataset worker messages.")
    parser.add_argument(
        "-q",
        "--quantity",
        type=int,
        default=128,
        help="Quantity of message files to generate (default: 128)",
    )
    parser.add_argument(
        "-f",
        "--folder",
        type=str,
        default=workspace_path,
        help=f"Folder to save message files (default: {workspace_path})",
    )
    parser.add_argument(
        "-s",
        "--script",
        type=str,
        default=script_path,
        help=f"Path to the script file to use in the message (default: {script_path})",
    )
    parser.add_argument(
        "-c",
        "--context",
        type=str,
        default=context_path,
        help=f"Context folder for the dataset function (default: {context_path})",
    )
    args = parser.parse_args()
    print("Generation values:")
    for arg, value in vars(args).items():
        print(f"{arg}: {value}")

    generate_messages(args.quantity, args.folder, args.script, args.context)


if __name__ == "__main__":
    main()
