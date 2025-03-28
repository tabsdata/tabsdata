#
# Copyright 2024 Tabs Data Inc.
#

from abc import ABC, abstractmethod

import yaml


# Define a custom constructor for the !Table tag
class Table:
    def __init__(
        self,
        data: dict,
    ):
        self.data = data

    @property
    def location(self):
        return self.data.get("location")

    @property
    def uri(self):
        return self.location.get("uri") if self.location else None

    @property
    def env_prefix(self):
        return self.location.get("env_prefix") if self.location else None

    @property
    def table(self):
        return self.data.get("table")

    @property
    def table_id(self):
        return self.data.get("table_id")

    @property
    def name(self):
        return self.data.get("name")

    def __repr__(self):
        return f"Table(name={self.name}, uri={self.uri})"


# Define a custom constructor for the !TableVersions tag
class TableVersions:
    def __init__(
        self,
        list_of_tables: list[dict],
    ):
        self.list_of_tables = list_of_tables

    @property
    def list_of_table_objects(self):
        # This is a somewhat complicated solution to avoid an issue with PyYAML, where
        # it will slowly populate the parameters while parsing. If defined in the
        # __init__, the Table will be built with an empty directory as the input.
        return [Table(table) for table in self.list_of_tables]

    def __repr__(self):
        return f"TableVersions(list_of_tables={self.list_of_table_objects})"


class InputYaml(ABC):
    """Just an abstract class to help with type hinting."""

    @property
    @abstractmethod
    def function_bundle_uri(self) -> str:
        """Return the function bundle URI."""

    @property
    @abstractmethod
    def input(self) -> list[Table | TableVersions]:
        """Return the input section of the YAML file."""

    @property
    @abstractmethod
    def output(self) -> list[Table]:
        """Return the output section of the YAML file."""

    @property
    @abstractmethod
    def system_input(self) -> list[Table]:
        """Return the input section of the YAML file."""

    @property
    @abstractmethod
    def system_output(self) -> list[Table]:
        """Return the output section of the YAML file."""

    @property
    @abstractmethod
    def dataset_data_version(self) -> str:
        """Dataset data version."""

    @property
    @abstractmethod
    def triggered_on(self) -> str:
        """Timestamp of the trigger of the dataset."""

    @property
    @abstractmethod
    def execution_plan_triggered_on(self) -> str:
        """Timestamp of the trigger of the whole execution plan."""


class V1(InputYaml):
    def __init__(self, content):
        self.content = content

    @property
    def info(self):
        return self.content.get("info")

    @property
    def dataset_data_version(self) -> str:
        return self.info.get("dataset_data_version") if self.info else None

    @property
    def triggered_on(self) -> str:
        return self.info.get("triggered_on") if self.info else None

    @property
    def execution_plan_triggered_on(self) -> str:
        return self.info.get("execution_plan_triggered_on") if self.info else None

    @property
    def function_bundle(self):
        return self.info.get("function_bundle") if self.info else None

    @property
    def function_bundle_uri(self):
        return self.function_bundle.get("uri") if self.function_bundle else None

    @property
    def function_bundle_env_prefix(self):
        return self.function_bundle.get("env_prefix") if self.function_bundle else None

    @property
    def input(self) -> list[Table | TableVersions]:
        return self.content.get("input")

    @property
    def output(self) -> list[Table]:
        return self.content.get("output")

    @property
    def system_input(self) -> list[Table]:
        return self.content.get("system-input")

    @property
    def system_output(self) -> list[Table]:
        return self.content.get("system-output")

    def __repr__(self):
        return f"V1(content={self.content})"


def v1_table_constructor(loader, node):
    return Table(loader.construct_mapping(node))


def v1_table_versions_constructor(loader, node):
    list_of_tables = loader.construct_sequence(node, deep=True)
    return TableVersions(list_of_tables)


def v1_constructor(loader, node):
    loader.add_constructor("!Table", v1_table_constructor)
    loader.add_constructor("!TableVersions", v1_table_versions_constructor)
    return V1(loader.construct_mapping(node))


def get_input_yaml_loader():
    """Add constructors to PyYAML loader."""
    loader = yaml.SafeLoader
    # When more versions are added, they will be listed here, and each will have its
    # own constructor.
    loader.add_constructor("!V1", v1_constructor)
    return loader


def parse_request_yaml(yaml_file: str) -> InputYaml:
    with open(yaml_file, "r") as file:
        return yaml.load(file, Loader=get_input_yaml_loader())


class Data:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name})"


class NoData:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name})"


class ResponseYaml(ABC):
    """Just an abstract class to help with type hinting."""


class V1ResponseFormat:
    """
    Simple class to enable us to generate response yaml files
    """

    def __init__(self, content):
        self.content = content

    def __repr__(self):
        return f"{self.__class__.__name__}(content={self.content})"


def v1_response_format_representer(
    dumper: yaml.SafeDumper, v1_response_format: V1ResponseFormat
) -> yaml.nodes.MappingNode:
    """Represent a V1_yaml instance as a YAML mapping node."""
    dumper.add_representer(Data, v1_data_representer)
    dumper.add_representer(NoData, v1_no_data_representer)
    return dumper.represent_mapping("!V1", v1_response_format.content)


def v1_data_representer(dumper: yaml.SafeDumper, data: Data) -> yaml.nodes.MappingNode:
    """Represent a Data instance as a YAML mapping node."""
    return dumper.represent_mapping("!Data", {"name": data.name})


def v1_no_data_representer(
    dumper: yaml.SafeDumper, no_data: NoData
) -> yaml.nodes.MappingNode:
    """Represent a NoData instance as a YAML mapping node."""
    return dumper.represent_mapping("!NoData", {"name": no_data.name})


def get_response_dumper():
    """Add representers to a YAML serializer."""
    safe_dumper = yaml.SafeDumper
    safe_dumper.add_representer(V1ResponseFormat, v1_response_format_representer)
    return safe_dumper


def store_response_as_yaml(tables: list[Data | NoData], response_file: str):
    with open(response_file, "w") as file:
        yaml.dump(
            V1ResponseFormat(
                content={
                    "context": tables,
                }
            ),
            file,
            Dumper=get_response_dumper(),
        )


class CopyYaml(ABC):
    """Just an abstract class to help with type hinting."""


class V1CopyFormat(CopyYaml):
    """
    Simple class to enable us to generate copy yaml files for the transporter
    """

    def __init__(self, source_target_pairs: list):
        self.content = {"source_target_pairs": source_target_pairs, "parallelism": None}

    def __repr__(self):
        return f"{self.__class__.__name__}(content={self.content})"


class TransporterEnv:
    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name})"


class TransporterLiteral:
    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return f"{self.__class__.__name__}(value={self.value})"


class TransporterLocalFile:
    def __init__(self, url: str):
        self.content = {"url": url}

    def __repr__(self):
        return f"{self.__class__.__name__}(content={self.content})"


class TransporterS3:
    def __init__(
        self,
        url: str,
        access_key: TransporterEnv | TransporterLiteral,
        secret_key: TransporterEnv | TransporterLiteral,
        region: TransporterEnv | TransporterLiteral,
        extra_configs: dict = None,
    ):
        configs = {
            "access_key": access_key,
            "secret_key": secret_key,
            "region": region,
            "extra_configs": extra_configs or {},
        }
        self.content = {"url": url, "configs": configs}

    def __repr__(self):
        return f"{self.__class__.__name__}(content={self.content})"


class TransporterAzure:
    def __init__(
        self,
        url: str,
        account_name: TransporterEnv | TransporterLiteral,
        account_key: TransporterEnv | TransporterLiteral,
        extra_configs: dict = None,
    ):
        configs = {
            "account_name": account_name,
            "account_key": account_key,
            "extra_configs": extra_configs or {},
        }
        self.content = {"url": url, "configs": configs}

    def __repr__(self):
        return f"{self.__class__.__name__}(content={self.content})"


def v1_copy_format_representer(
    dumper: yaml.SafeDumper, v1_copy_format: V1CopyFormat
) -> yaml.nodes.MappingNode:
    """Represent a V1_yaml instance as a YAML mapping node."""
    dumper.add_representer(TransporterEnv, v1_env_representer)
    dumper.add_representer(TransporterLiteral, v1_literal_representer)
    dumper.add_representer(TransporterLocalFile, v1_local_file_representer)
    dumper.add_representer(TransporterAzure, v1_azure_representer)
    dumper.add_representer(TransporterS3, v1_s3_representer)
    return dumper.represent_mapping("!CopyV1", v1_copy_format.content)


def v1_literal_representer(
    dumper: yaml.SafeDumper, literal: TransporterLiteral
) -> yaml.nodes.ScalarNode:
    """Represent a Literal instance as a YAML mapping node."""
    return dumper.represent_scalar("!Literal", literal.value)


def v1_env_representer(
    dumper: yaml.SafeDumper, env: TransporterEnv
) -> yaml.nodes.ScalarNode:
    """Represent an Env instance as a YAML mapping node."""
    return dumper.represent_scalar("!Env", env.name)


def v1_local_file_representer(
    dumper: yaml.SafeDumper, local_file: TransporterLocalFile
) -> yaml.nodes.MappingNode:
    """Represent a LocalFile instance as a YAML mapping node."""
    return dumper.represent_mapping("!LocalFile", local_file.content)


def v1_azure_representer(
    dumper: yaml.SafeDumper, azure_file: TransporterAzure
) -> yaml.nodes.MappingNode:
    """Represent an Azure instance as a YAML mapping node."""
    return dumper.represent_mapping("!Azure", azure_file.content)


def v1_s3_representer(
    dumper: yaml.SafeDumper, s3_file: TransporterS3
) -> yaml.nodes.MappingNode:
    """Represent an S3 instance as a YAML mapping node."""
    return dumper.represent_mapping("!S3", s3_file.content)


def get_copy_dumper():
    """Add representers to a YAML serializer."""
    safe_dumper = yaml.SafeDumper
    safe_dumper.add_representer(V1CopyFormat, v1_copy_format_representer)
    return safe_dumper


def store_copy_as_yaml(copy: V1CopyFormat, copy_file: str):
    with open(copy_file, "w") as file:
        yaml.dump(
            copy,
            file,
            Dumper=get_copy_dumper(),
        )
