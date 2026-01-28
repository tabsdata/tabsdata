#
# Copyright 2026 Tabs Data Inc.
#

from enum import Enum

AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
AWS_DEFAULT_REGION = "AWS_DEFAULT_REGION"


CONSUME_TIMEOUT_SECS = 2
CONNECTION_TIMEOUT_SECS = 5


class MessageFormat(Enum):
    AVRO = "avro"
    JSON = "json"
    PROTOBUF = "protobuf"
