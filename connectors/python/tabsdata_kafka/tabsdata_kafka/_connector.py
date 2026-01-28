#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from abc import ABC
import logging
import signal
import sys
import time
import boto3
from datetime import datetime
from typing import Optional, TypeAlias, Union, Literal

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from mypy_boto3_glue.client import GlueClient
from botocore.client import BaseClient
from pydantic import PositiveInt

from tabsdata._io.connections.connections import Conn
from tabsdata._io.stage_trigger import (
    StageTrigger,
    StageTriggerContext,
    StageTriggerStatus,
)
from tabsdata._credentials import UserPasswordCredentials
from tabsdata.dataquality._utils import _td_validate_call
from tabsdata.exceptions import ErrorCode, StageTriggerConfigurationError
from tabsdata_kafka.constants import (
    CONNECTION_TIMEOUT_SECS,
    CONSUME_TIMEOUT_SECS,
    MessageFormat,
)
from tabsdata_kafka.shared_types import (
    KafkaMessageFormatSpec,
    KafkaServersSpec,
    SchemaIdCompatibles,
    SchemaIdSpec,
    AwsGlueAccessKeyCredentials,
)

from tabsdata_kafka.consumer.consumer import KafkaConsumer
from tabsdata_kafka.consumer.messages_buffer import MessagesBuffer
from tabsdata_kafka.consumer.rollovers import (
    MessagesRollover,
    RolloverReason,
    SizeRollover,
)
from tabsdata_kafka.serdes.aws_glue_sr_serde import (
    AvroGlueKafkaSerde,
    JsonGlueKafkaSerde,
    ProtobufGlueKafkaSerde,
)
from tabsdata_kafka.serdes.confluent_sr_serde import (
    AvroSrKafkaSerde,
    JsonSrKafkaSerde,
    ProtobufSrKafkaSerde,
)
from tabsdata_kafka.serdes.non_sr_serde import (
    AvroKafkaSerde,
    JsonKafkaSerde,
    ProtobufKafkaSerde,
)
from tabsdata_kafka.serdes.serde import (
    KafkaSerde,
)
from tabsdata_kafka.utils import handle_rollover, obtain_and_set_aws_glue_credentials

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class KafkaSchemaRegistry(ABC):
    """
    Base class for schema registry configurations.
    """


class ConfluentSchemaRegistry(KafkaSchemaRegistry):
    @_td_validate_call(ErrorCode.PVCX7)
    def __init__(
        self,
        url: str,
        credentials: Optional[UserPasswordCredentials] = None,
        configs: Optional[dict] = None,
    ):
        self._url = url
        self._credentials = credentials
        self._configs = configs or {}

    @property
    def url(self) -> str:
        return self._url

    @property
    def credentials(self) -> Optional[UserPasswordCredentials]:
        return self._credentials

    @property
    def configs(self) -> dict:
        return self._configs

    def __repr__(self):
        return f"ConfluentSchemaRegistry(url={self.url}, configs={self.configs})"


class AwsGlueSchemaRegistry(KafkaSchemaRegistry):
    """
    Configuration for AWS Glue Schema Registry.
    """

    @_td_validate_call(ErrorCode.PVCX8)
    def __init__(
        self,
        region_name: str,
        credentials: Optional[AwsGlueAccessKeyCredentials] = None,
        configs: Optional[dict] = None,
    ):
        self._region_name = region_name
        self._credentials = credentials
        self._configs = configs or {}

    @property
    def region_name(self) -> str:
        return self._region_name

    @property
    def credentials(self) -> Optional[AwsGlueAccessKeyCredentials]:
        return self._credentials

    @property
    def configs(self) -> dict:
        return self._configs

    def __repr__(self):
        return (
            f"AwsGlueSchemaRegistry(region_name={self.region_name},"
            f" configs={self.configs})"
        )


class KafkaConn(Conn):
    """
    Kafka Consumer Connection configuration.
    """

    @_td_validate_call(ErrorCode.PVCX6)
    def __init__(
        self,
        servers: KafkaServersSpec,
        credentials: Optional[UserPasswordCredentials] = None,
        group_id: Optional[str] = None,
        schema_registry: Optional[
            ConfluentSchemaRegistry | AwsGlueSchemaRegistry
        ] = None,
        enforce_connection_params: bool = True,
        cx_src_configs: Optional[dict] = None,
    ):
        """
        Initialize a KafkaConn instance.

        Args:
            servers: Kafka bootstrap servers.
            credentials: Optional Kafka credentials.
            group_id: Optional Kafka consumer group ID.
            schema_registry: Optional Kafka schema registry configuration.
            enforce_connection_params: Whether to enforce connection parameters for the
                Kafka connection.
            cx_src_configs: Source-specific configuration parameters for the Kafka
                connection. Defaults to None.
        """
        self._servers = servers
        self._credentials = credentials
        self._group_id = group_id
        self._schema_registry = schema_registry
        self._enforce_connection_params = enforce_connection_params
        self._cx_src_configs = cx_src_configs or {}

    @property
    def servers(self) -> KafkaServersSpec:
        """
        Kafka bootstrap servers.
        """
        return self._servers

    @property
    def credentials(self) -> Optional[UserPasswordCredentials]:
        """
        Kafka credentials.
        """
        return self._credentials

    @property
    def group_id(self) -> Optional[str]:
        """
        Kafka consumer group ID.
        """
        return self._group_id

    @property
    def schema_registry(
        self,
    ) -> Optional[ConfluentSchemaRegistry | AwsGlueSchemaRegistry]:
        """
        Kafka schema registry configuration.
        """
        return self._schema_registry

    @property
    def enforce_connection_params(self) -> bool:
        """
        Whether to enforce connection parameters.
        """
        return self._enforce_connection_params

    @property
    def cx_src_configs(self) -> dict:
        """
        Source-specific configuration parameters for the Kafka connection.
        """
        return self._cx_src_configs


class KafkaStageTrigger(StageTrigger):

    @_td_validate_call(ErrorCode.PVTRGR1)
    def __init__(
        self,
        conn: KafkaConn,
        topic: str,
        data_format: KafkaMessageFormatSpec,
        schema: str,
        group_id: Optional[str] = None,
        message_name: Optional[str] = None,
        registry_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        time_rollover_secs: PositiveInt = 60,
        size_rollover_mb: Optional[SizeRollover] = None,
        messages_rollover: Optional[MessagesRollover] = None,
        dlq_topic: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ):
        """
        Initialize a KafkaTrigger instance.

        Args:
            conn: The Kafka consumer connection configuration.
            topic: The Kafka topic to consume from.
            data_format: The data format of the messages ("avro", "json", "protobuf").
            schema: The schema string for deserializing messages.
            group_id: Optional Kafka consumer group ID.
            schema_registry_client: Optional Schema Registry client (Confluent or AWS
                Glue).
            message_name: Optional message name for Protobuf schemas.
            registry_name: Optional registry name for AWS Glue Schema Registry.
            schema_name: Optional schema name for AWS Glue Schema Registry.
            time_rollover_secs: The timeout in seconds before staging and committing a
                message buffer. Defaults to 60.
            size_rollover: Optional size-based rollover configuration. Size is specified
                in megabytes.
            messages_rollover: Optional message count-based rollover configuration.
            dlq_topic: Optional dead-letter queue topic.
            start: Optional datetime for the trigger to start working.
            end: Optional datetime for the trigger to stop working.
        """
        super().__init__(start=start, end=end)
        self._conn = conn
        self._topic = topic
        self._data_format = data_format
        self._schema = schema
        self._group_id = group_id
        self._message_name = message_name
        self._registry_name = registry_name
        self._schema_name = schema_name
        self._time_rollover_secs = time_rollover_secs
        self._size_rollover_mb = size_rollover_mb
        self._messages_rollover = messages_rollover
        self._dlq_topic = dlq_topic

        self._schema_registry_client = self._build_schema_registry_client()
        self._kafka_configs = self._build_kafka_configs()

    @property
    def conn(self) -> KafkaConn:
        """
        Kafka consumer connection configuration.
        """
        return self._conn

    @property
    def topic(self) -> str:
        """
        Kafka topic to consume from.
        """
        return self._topic

    @property
    def data_format(self) -> KafkaMessageFormatSpec:
        """
        Data format of the messages ("avro", "json", "protobuf").
        """
        return self._data_format

    @property
    def schema(self) -> str:
        """
        Schema string for deserializing messages.
        """
        return self._schema

    @property
    def group_id(self) -> Optional[str]:
        """
        Kafka consumer group ID.
        """
        return self._group_id

    @property
    def message_name(self) -> Optional[str]:
        """
        Message name for Protobuf schemas.
        """
        return self._message_name

    @property
    def registry_name(self) -> Optional[str]:
        """
        Registry name for AWS Glue Schema Registry.
        """
        return self._registry_name

    @property
    def schema_name(self) -> Optional[str]:
        """
        Schema name for AWS Glue Schema Registry.
        """
        return self._schema_name

    @property
    def time_rollover_secs(self) -> int:
        """
        Timeout in seconds for consuming each message.
        """
        return self._time_rollover_secs

    @property
    def size_rollover_mb(self) -> Optional[SizeRollover]:
        """
        Size-based rollover configuration. Size is specified in megabytes.
        """
        return self._size_rollover_mb

    @property
    def messages_rollover(self) -> Optional[MessagesRollover]:
        """
        Message count-based rollover configuration.
        """
        return self._messages_rollover

    @property
    def dlq_topic(self) -> Optional[str]:
        """
        Dead-letter queue topic.
        """
        return self._dlq_topic

    def _build_schema_registry_client(self) -> SchemaRegistryClient | GlueClient | None:
        sr = self.conn.schema_registry
        if not sr:
            return None
        elif isinstance(sr, ConfluentSchemaRegistry):
            sr_configs = {}
            if sr.configs:
                sr_configs = sr_configs | sr.configs

            sr_configs["url"] = sr.url
            if sr.credentials:
                sr_configs["basic.auth.user.info"] = (
                    f"{sr.credentials.user.secret_value}:{sr.credentials.password.secret_value}"
                )
            return SchemaRegistryClient(sr_configs)
        elif isinstance(sr, AwsGlueSchemaRegistry):

            boto_kwargs = {"region_name": sr.region_name}
            if sr.configs:
                boto_kwargs = boto_kwargs | sr.configs
            if sr.credentials:
                obtain_and_set_aws_glue_credentials(sr.credentials)
            sr: GlueClient = boto3.client("glue", **boto_kwargs)
            return sr
        raise ValueError(f"Unsupported schema registry type: {type(sr)}")

    def _build_kafka_configs(self):
        kafka_configs = {}
        if self.conn.cx_src_configs:
            kafka_configs = kafka_configs | self.conn.cx_src_configs

        kafka_configs["bootstrap.servers"] = (
            self.conn.servers
            if isinstance(self.conn.servers, str)
            else ",".join(self.conn.servers)
        )
        kafka_configs["auto.offset.reset"] = "earliest"

        kafka_configs["enable.auto.commit"] = False

        if self.conn.credentials:
            kafka_configs["security.protocol"] = "sasl_plaintext"
            kafka_configs["sasl.mechanism"] = "SCRAM-SHA-256"
            kafka_configs["sasl.username"] = self.conn.credentials.user.secret_value
            kafka_configs["sasl.password"] = self.conn.credentials.password.secret_value

        # Set group.id based on enforce_connection_params flag
        # If enforce_connection_params is True, use conn.group_id if provided,
        # otherwise use the group_id passed to the trigger
        # If enforce_connection_params is False, always use the group_id passed to the
        # trigger
        if self.conn.enforce_connection_params:
            kafka_configs["group.id"] = (
                self.conn.group_id if self.conn.group_id else self.group_id
            )
        else:
            kafka_configs["group.id"] = self.group_id

        # Validate that group.id is set
        if not kafka_configs.get("group.id", None):
            raise StageTriggerConfigurationError(
                ErrorCode.STE6,
                self.conn.__class__.__name__,
                self.__class__.__name__,
                kafka_configs.get("group.id", None),
            )

        return kafka_configs

    def run(self, context: StageTriggerContext):
        """
        Run the Kafka stage trigger to consume messages, buffer them, and stage them
        based on rollover conditions.

        Args:
            context: The StageTriggerContext instance.
        """

        # Add SIGTERM handler to raise KeyboardInterrupt
        def _sigterm_handler(signum, frame):
            raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, _sigterm_handler)

        serde = self._get_serde(context.working_dir)
        logger.debug(f"Using serde: {type(serde).__name__}")

        consumer = None
        try:
            consumer = Consumer(self._kafka_configs)
            # This forces a connection timeout if the broker is unreachable
            consumer.list_topics(timeout=CONNECTION_TIMEOUT_SECS)
        except Exception as e:
            logger.error(f"Error connecting to the Kafka broker: {e}")
            raise e

        dlq_producer = None
        try:
            dlq_producer = Producer(self._kafka_configs) if self.dlq_topic else None
            # This forces a connection timeout if the broker is unreachable
            if dlq_producer:
                dlq_producer.list_topics(timeout=CONNECTION_TIMEOUT_SECS)
        except Exception as e:
            logger.error(f"Error creating DLQ producer: {e}")
            raise e

        consumer.subscribe([self.topic])
        kafka_consumer = KafkaConsumer(
            topic=self.topic,
            consumer=consumer,
            serde=serde,
            size_rollover=self.size_rollover_mb,
            messages_rollover=self.messages_rollover,
            dlq_producer=dlq_producer,
            dlq_topic=self.dlq_topic,
        )
        logger.debug(f"Kafka consumer created. {kafka_consumer}")

        try:
            start = time.time()  # we keep track of the start time to be able to timeout
            buffer = MessagesBuffer(self.topic, serde.schema_id_compatibles_checker())
            rollover_reason: RolloverReason = None
            reset_buffer_on_time_rollover = False
            stage_trigger_status = StageTriggerStatus.SUCCESS
            while True:
                # Break the loop if end time is set and current time exceeds it
                if self._end is not None and datetime.now() > self._end:
                    stage_trigger_status = StageTriggerStatus.SUCCESS
                    logger.info("KafkaStageTrigger end time reached, exiting loop.")
                    break
                context.trigger_monitor()
                rollover_reason, buffer = kafka_consumer.consume(
                    timeout_secs=CONSUME_TIMEOUT_SECS,
                    reset_buffer=reset_buffer_on_time_rollover,
                )
                logger.debug(
                    f"Rollover reason: {rollover_reason}, Buffer size:"
                    f" {buffer.size_mb()} MB, Message count: {buffer.message_count()}"
                )
                max_timeout = self.time_rollover_secs - (time.time() - start)
                if buffer is not None and (
                    rollover_reason != RolloverReason.Time or max_timeout <= 0
                ):
                    start = handle_rollover(
                        context=context,
                        buffer=buffer,
                        rollover_reason=rollover_reason,
                        working_dir=context.working_dir,
                        kafka_consumer=kafka_consumer,
                    )
                    buffer = MessagesBuffer(
                        self.topic, serde.schema_id_compatibles_checker()
                    )
                reset_buffer_on_time_rollover = max_timeout <= 0
        except KeyboardInterrupt:
            stage_trigger_status = StageTriggerStatus.STOPPED
            logger.info("KafkaStageTrigger interrupted, exiting loop.")
        except Exception as e:
            stage_trigger_status = StageTriggerStatus.ERROR
            logger.error(f"Error in KafkaStageTrigger run loop: {e}")
            raise e
        finally:
            if buffer is not None:
                logger.debug("Final buffer rollover before shutdown.")
                handle_rollover(
                    context=context,
                    buffer=buffer,
                    rollover_reason=rollover_reason,
                    working_dir=context.working_dir,
                    kafka_consumer=kafka_consumer,
                )
            if dlq_producer:
                logger.debug("Flushing DLQ producer before shutdown.")
                dlq_producer.flush()
            logger.debug("Closing Kafka consumer.")
            kafka_consumer.close()
            consumer.close()
            logger.debug("Kafka consumer closed.")
            sys.exit(stage_trigger_status.value)

    def _get_serde(self, working_dir: str) -> KafkaSerde:
        """
        Get the appropriate KafkaSerde based on the data format and schema registry
        client.

        Args:
            working_dir: A working directory used for runtime files by the serde.

        Returns:
            A KafkaSerde instance.
        """

        def _raise_unsupported():
            raise ValueError(
                f"Serde not created: unsupported data format '{self.data_format}'."
            )

        def non_sr_serde():
            return {
                MessageFormat.AVRO.value: lambda: AvroKafkaSerde(
                    avro_schema=self.schema
                ),
                MessageFormat.JSON.value: lambda: JsonKafkaSerde(
                    json_schema=self.schema
                ),
                MessageFormat.PROTOBUF.value: lambda: ProtobufKafkaSerde(
                    protobuf_schema=self.schema,
                    message_name=self.message_name,
                    working_dir=working_dir,
                ),
            }.get(self.data_format, lambda: _raise_unsupported())()

        def sr_serde():
            return {
                MessageFormat.AVRO.value: lambda: AvroSrKafkaSerde(
                    schema_registry_client=self._schema_registry_client,
                    avro_schema=self.schema,
                ),
                MessageFormat.JSON.value: lambda: JsonSrKafkaSerde(
                    schema_registry_client=self._schema_registry_client,
                    json_schema=self.schema,
                ),
                MessageFormat.PROTOBUF.value: lambda: ProtobufSrKafkaSerde(
                    schema_registry_client=self._schema_registry_client,
                    protobuf_schema=self.schema,
                    message_name=self.message_name,
                    working_dir=working_dir,
                ),
            }.get(self.data_format, lambda: _raise_unsupported())()

        def glue_serde():
            return {
                MessageFormat.AVRO.value: lambda: AvroGlueKafkaSerde(
                    glue_client=self._schema_registry_client,
                    registry_name=self.registry_name,
                    schema_name=self.schema_name,
                    avro_schema=self.schema,
                ),
                MessageFormat.JSON.value: lambda: JsonGlueKafkaSerde(
                    glue_client=self._schema_registry_client,
                    registry_name=self.registry_name,
                    schema_name=self.schema_name,
                    json_schema=self.schema,
                ),
                MessageFormat.PROTOBUF.value: lambda: ProtobufGlueKafkaSerde(
                    glue_client=self._schema_registry_client,
                    registry_name=self.registry_name,
                    schema_name=self.schema_name,
                    protobuf_schema=self.schema,
                    message_name=self.message_name,
                    working_dir=working_dir,
                ),
            }.get(self.data_format, lambda: _raise_unsupported())()

        match self._schema_registry_client:
            case None:
                return non_sr_serde()
            case SchemaRegistryClient():
                return sr_serde()
            case BaseClient():
                return glue_serde()
            case _:
                raise ValueError(
                    "Unsupported schema registry client:"
                    f" {self._schema_registry_client}"
                )
