#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os
from abc import ABC

import yaml

logger = logging.getLogger(__name__)


EXCEPTION_YAML_FILE_NAME = "exception.yaml"

MAXIMUM_ERROR_CODE_LENGTH = 16
MAXIMUM_KIND_LENGTH = 64
MAXIMUM_MESSAGE_LENGTH = 128


class ExceptionYaml(ABC):
    """Just an abstract class to help with type hinting."""


class V1ExceptionFormat:
    """
    Simple class to enable us to generate exception yaml files
    """

    def __init__(self, exception: Exception, exit_status: int):
        self.exception = exception
        self.exit_status = exit_status

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(exception={self.exception}, exit_status="
            f"{self.exit_status})"
        )


def v1_exception_format_representer(
    dumper: yaml.SafeDumper, v1_exception_format: V1ExceptionFormat
) -> yaml.nodes.MappingNode:
    """Represent a V1ExceptionFormat instance as a YAML mapping node."""
    mapping = {}
    e = v1_exception_format.exception
    mapping["kind"] = type(e).__name__[:MAXIMUM_KIND_LENGTH]
    mapping["message"] = str(e)[:MAXIMUM_MESSAGE_LENGTH]
    if error_code := getattr(e, "error_code", None):
        mapping["error_code"] = error_code[:MAXIMUM_ERROR_CODE_LENGTH]
    mapping["exit_status"] = v1_exception_format.exit_status
    return dumper.represent_mapping("!V1", mapping)


def get_exception_dumper():
    """Add representers to a YAML serializer."""

    # This prevents overwriting the global SafeDumper
    class ExceptionDumper(yaml.SafeDumper):
        pass

    ExceptionDumper.add_representer(V1ExceptionFormat, v1_exception_format_representer)
    return ExceptionDumper


def store_exception_yaml(exception: Exception, exit_status: int, output_folder: str):
    logging.debug(f"Storing exception information in folder {output_folder}")
    exception_object = V1ExceptionFormat(exception, exit_status)
    logging.debug(f"Exception information: {exception_object}")
    with open(os.path.join(output_folder, EXCEPTION_YAML_FILE_NAME), "w") as file:
        yaml.dump(
            exception_object,
            file,
            Dumper=get_exception_dumper(),
        )
