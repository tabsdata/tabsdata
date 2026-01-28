#
# Copyright 2026 Tabs Data Inc.
#

from typing import Callable, Literal, TypeAlias, Union

from tabsdata._credentials import S3AccessKeyCredentials, UserPasswordCredentials

SchemaIdSpec: TypeAlias = Union[str, dict[str, str]]

SchemaIdCompatibles: TypeAlias = Callable[[SchemaIdSpec, SchemaIdSpec], bool]

KafkaServersSpec: TypeAlias = Union[str, list[str]]

KafkaMessageFormatSpec = Literal["avro", "json", "protobuf"]

AwsGlueAccessKeyCredentials: TypeAlias = S3AccessKeyCredentials
