#
# Copyright 2024 Tabs Data Inc.
#
from abc import ABC, abstractmethod
from dataclasses import dataclass

import yaml


# Define a custom constructor for basic locations-like entries
@dataclass
class Location:
    data: dict

    @property
    def uri(self):
        return self.data.get("uri")

    @property
    def env_prefix(self):
        return self.data.get("env_prefix")

    def __eq__(self, other):
        return isinstance(other, Location) and self.data == other.data

    def __repr__(self):
        return f"Location(uri={self.uri}, env_prefix={self.env_prefix})"


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

    @property
    def table_pos(self) -> int:
        return self.data.get("table_pos")

    @property
    def version_pos(self) -> int:
        return self.data.get("version_pos")

    @property
    def input_idx(self) -> int:
        return self.data.get("input_idx")

    def __eq__(self, other):
        return isinstance(other, Table) and self.data == other.data

    def __repr__(self):
        return f"Table(name={self.name}, uri={self.uri}, data={self.data})"


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
    def execution_id(self):
        """Return the execution ID of the request."""

    @property
    @abstractmethod
    def execution_plan_triggered_on(self) -> str:
        """Timestamp of the trigger of the whole execution plan."""

    @property
    @abstractmethod
    def function_bundle_uri(self) -> str:
        """Return the function bundle URI."""

    @property
    @abstractmethod
    def function_data(self) -> Table:
        """Return the function_data section of the YAML file."""

    @property
    @abstractmethod
    def function_run_id(self):
        """Return the function run ID of the request."""

    @property
    @abstractmethod
    def info(self) -> dict:
        """Return the info section of the YAML file."""

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
    def transaction_id(self):
        """Return the transaction ID of the request."""

    @property
    @abstractmethod
    def triggered_on(self) -> str:
        """Timestamp of the trigger of the dataset."""

    @property
    @abstractmethod
    def work(self):
        """Return the work section of the YAML file."""

    @work.setter
    @abstractmethod
    def work(self, value):
        """Set the work section of the YAML file."""


class V2(InputYaml):
    def __init__(self, content):
        self.content = content

    @property
    def execution_id(self):
        return self.info["execution_id"]

    @property
    def execution_plan_triggered_on(self) -> str:
        return self.info["execution_plan_triggered_on"]

    @property
    def function_bundle(self):
        return self.info.get("function_bundle") if self.info else None

    @property
    def function_bundle_env_prefix(self):
        return self.function_bundle.get("env_prefix") if self.function_bundle else None

    @property
    def function_bundle_uri(self):
        return self.function_bundle.get("uri") if self.function_bundle else None

    @property
    def function_data(self) -> Location:
        return Location(self.info.get("function_data"))

    @property
    def function_run_id(self):
        return self.info["function_run_id"]

    @property
    def info(self):
        return self.content.get("info", {})

    @property
    def input(self) -> list[Table | TableVersions]:
        return self.content.get("input")

    @property
    def output(self) -> list[Table]:
        return self.content.get("output")

    @property
    def system_input(self) -> list[Table]:
        return self.content.get("system_input")

    @property
    def system_output(self) -> list[Table]:
        return self.content.get("system_output")

    @property
    def transaction_id(self):
        return self.info["transaction_id"]

    @property
    def triggered_on(self) -> str:
        return self.info.get("triggered_on") if self.info else None

    @property
    def work(self) -> str:
        return self.content.get("work")

    @work.setter
    def work(self, value: str):
        self.content["work"] = value

    def __repr__(self):
        return f"V2(content={self.content})"


def v2_table_constructor(loader, node):
    return Table(loader.construct_mapping(node))


def v2_table_versions_constructor(loader, node):
    list_of_tables = loader.construct_sequence(node, deep=True)
    return TableVersions(list_of_tables)


def v2_constructor(loader, node):
    loader.add_constructor("!Table", v2_table_constructor)
    loader.add_constructor("!TableVersions", v2_table_versions_constructor)
    return V2(loader.construct_mapping(node))


def get_input_yaml_loader():
    """Add constructors to PyYAML loader."""
    loader = yaml.SafeLoader
    # When more versions are added, they will be listed here, and each will have its
    # own constructor.
    loader.add_constructor("!V2", v2_constructor)
    return loader


def parse_request_yaml(yaml_file: str) -> InputYaml:
    with open(yaml_file, "r") as file:
        return yaml.load(file, Loader=get_input_yaml_loader())


class Data:
    def __init__(self, table):
        self.table = table

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.table})"


class NoData:
    def __init__(self, table):
        self.table = table

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.table})"


class ResponseYaml(ABC):
    """Just an abstract class to help with type hinting."""


class V2ResponseFormat:
    """
    Simple class to enable us to generate response yaml files
    """

    def __init__(self, content):
        self.content = content

    def __repr__(self):
        return f"{self.__class__.__name__}(content={self.content})"


def v2_response_format_representer(
    dumper: yaml.SafeDumper, v2_response_format: V2ResponseFormat
) -> yaml.nodes.MappingNode:
    """Represent a V2ResponseFormat instance as a YAML mapping node."""
    # Convert Data and NoData objects to dictionaries
    output_list = []
    for item in v2_response_format.content["context"]["V2"]["output"]:
        if isinstance(item, Data):
            output_list.append({"Data": {"table": item.table}})
        elif isinstance(item, NoData):
            output_list.append({"NoData": {"table": item.table}})
    return dumper.represent_mapping("!V2", {"context": {"V2": {"output": output_list}}})


def v2_data_representer(dumper: yaml.SafeDumper, data: Data) -> yaml.nodes.MappingNode:
    """Represent a Data instance as a YAML mapping node."""
    return dumper.represent_mapping(None, {"Data": {"table": data.table}})


def v2_no_data_representer(
    dumper: yaml.SafeDumper, no_data: NoData
) -> yaml.nodes.MappingNode:
    """Represent a NoData instance as a YAML mapping node."""
    return dumper.represent_mapping(None, {"NoData": {"table": no_data.table}})


def get_response_dumper():
    """Add representers to a YAML serializer."""

    # This prevents overwriting the global SafeDumper
    class ResponseDumper(yaml.SafeDumper):
        pass

    ResponseDumper.add_representer(V2ResponseFormat, v2_response_format_representer)
    return ResponseDumper


def store_response_as_yaml(tables: list[Data | NoData], response_file: str):
    with open(response_file, "w") as file:
        yaml.dump(
            V2ResponseFormat(
                content={
                    "context": {
                        "V2": {
                            "output": tables,
                        }
                    }
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
