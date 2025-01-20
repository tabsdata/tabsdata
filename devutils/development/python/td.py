#
# Copyright 2024 Tabs Data Inc.
#

# Disclaimer: This is an experimental script not for use in production environments.
# There might be some hard-code values that you might need to adjust to your current
# setup. You might also need to install some additional Python packages to run it.

import html
import json
import os
import re
import subprocess
from pathlib import Path

import webview
from nicegui import app, ui

commands = {
    "Start": ["profile", "instance", "repository", "workspace", "worker_arguments"],
    "Stop": ["instance", "workspace", "force"],
    "Restart": [
        "profile",
        "instance",
        "repository",
        "workspace",
        "force",
        "worker_arguments",
    ],
    "Profile": ["+name", "folder"],
    "Log": ["instance", "workspace"],
    "Status": ["instance", "workspace"],
}

storage_path = Path(os.path.expanduser("~/.tabsdata/nicegui/webstorage.json"))


def load_stored_values():
    if storage_path.exists():
        try:
            with open(storage_path, "r") as file:
                return json.load(file)
        except json.JSONDecodeError as e:
            print(f"Error deserializing web storage json: {e}")
    return {}


def save_values(values):
    os.makedirs(storage_path.parent, exist_ok=True)
    with open(storage_path, "w") as file:
        json.dump(values, file)


stored_values = load_stored_values()
form_inputs = {}


def update_form(command: str):
    global form_inputs
    form_inputs.clear()
    form_container.clear()
    args = commands.get(command, [])
    if not args:
        ui.label("No fields available for this command").classes(
            "text-lg text-gray-600"
        )
    else:
        with form_container:
            for arg in args:
                saved_value = stored_values.get(command, {}).get(arg, "")
                if arg == "force":
                    ui.label(f"{arg.capitalize()}").classes("text-lg text-gray-600")
                    checkbox = ui.checkbox().classes("w-full text-lg text-gray-600")
                    checkbox.value = bool(saved_value)
                    form_inputs[arg] = checkbox
                elif arg == "worker_arguments":
                    with ui.row().classes(
                        "w-full items-center justify-start flex space-x-2"
                    ):
                        input_field = ui.input(
                            label="Worker Arguments",
                            placeholder="Enter worker arguments",
                        ).classes("flex-1 max-w-full text-lg text-gray-600")
                        input_field.value = saved_value
                        form_inputs[arg] = input_field
                else:
                    with ui.row().classes(
                        "w-full items-center justify-start flex space-x-2"
                    ):
                        input_field = ui.input(
                            label=f"{arg.capitalize()}", placeholder=f"Enter {arg}"
                        ).classes("flex-1 max-w-full text-lg text-gray-600")
                        input_field.value = saved_value
                        form_inputs[arg] = input_field
                        ui.button(
                            "Pick", on_click=lambda a=input_field: pick_folder(a)
                        ).classes("text-lg normal-case w-16").props('color="orange"')


async def pick_folder(input_element):
    folder = await app.native.main_window.create_file_dialog(
        dialog_type=webview.FOLDER_DIALOG, allow_multiple=False
    )
    if folder:
        input_element.value = folder[0]


def strip_ansi_sequences(text: str) -> str:
    ansi_escape = re.compile(r"(?:\x1B[@-_][0-?]*[ -/]*[@-~])")
    return ansi_escape.sub("", text)


def execute_command():
    selected_command = command_selector.value
    args = []
    command_map = {
        "Start": "start",
        "Stop": "stop",
        "Restart": "restart",
        "Profile": "profile",
        "Log": "log",
        "Status": "status",
    }
    command_keyword = command_map.get(selected_command, "")
    if selected_command not in stored_values:
        stored_values[selected_command] = {}
    for arg, input_element in form_inputs.items():
        if arg == "force" and input_element.value:
            args.append("--force")
        else:
            value = input_element.value
            if value:
                if arg == "worker_arguments":
                    args.append("--")
                else:
                    args.append(f"--{arg}")
                args.append(value)
            stored_values[selected_command][arg] = input_element.value
    save_values(stored_values)
    command = [
        "~/Solutions/TabsData/Work/tabsdata/target/debug/td",
        command_keyword,
    ] + args
    try:
        output = subprocess.check_output(" ".join(command), shell=True, text=True)
        clean_output = strip_ansi_sequences(output)
        formatted_output = clean_output.replace("\n", "<br>")
        result_output.content = (
            f'<span style="color:#1E90FF;">{formatted_output}</span>'
        )
    except subprocess.CalledProcessError as e:
        result_output.content = (
            "<span style='color:red;'>Error executing"
            f" command:<br>{html.escape(str(e))}</span>"
        )


@ui.page("/")
def index():
    global command_selector, result_output, form_container
    ui.label("TabsData Client").classes("text-xl text-gray-600")
    command_selector = ui.select(
        options=list(commands.keys()), label="Select Command"
    ).classes("text-lg w-72 my-4 text-gray-600")
    form_container = ui.column().classes("space-y-4 w-full")
    ui.button("Run", on_click=execute_command).classes("mt-4 text-lg normal-case")
    result_output = ui.markdown().classes("text-lg w-full h-48")
    command_selector.on(
        "update:modelValue", lambda: update_form(command_selector.value)
    )
    initial_command = list(commands.keys())[0]
    command_selector.value = initial_command
    update_form(initial_command)


ui.run(native=True, window_size=(1200, 800), title="TabsData CLI GUI", dark=True)
