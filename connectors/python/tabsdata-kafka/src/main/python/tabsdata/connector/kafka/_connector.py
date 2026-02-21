#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import signal
import sys
import time
from abc import ABC
from datetime import datetime, timezone
from typing import TYPE_CHECKING, TypeAlias, Union, cast

import boto3
from botocore.client import BaseClient
from mypy_boto3_glue.client import GlueClient
from pydantic import PositiveInt
from typing_extensions import deprecated

# noinspection PyProtectedMember
from tabsdata._io.connections.connections import Conn

# noinspection PyProtectedMember
from tabsdata._io.stage_trigger import (
    StageTrigger,
    StageTriggerContext,
    StageTriggerStatus,
)
from tabsdata._utils.annotations import unstable, unstable_category

# noinspection PyProtectedMember
from tabsdata._utils.validation import _td_validate_call
from tabsdata.connector.kafka.constants import (
    _AVRO,
    _JSON,
    _PROTOBUF,
    AUTO_OFFSET_RESET_KEY,
    BASIC_AUTH_USER_INFO_KEY,
    BOOTSTRAP_SERVERS_KEY,
    CONNECTION_TIMEOUT_SECS,
    CONSUME_TIMEOUT_SECS,
    DEFAULT_TIME_ROLLOVER_SECS,
    ENABLE_AUTO_COMMIT_KEY,
    GROUP_ID_KEY,
    REGION_NAME_KEY,
    SASL_MECHANISM_KEY,
    SASL_PASSWORD_KEY,
    SASL_USERNAME_KEY,
    SECURITY_PROTOCOL_KEY,
    URL_KEY,
)
from tabsdata.connector.kafka.consumer.consumer import KafkaConsumer, RolloverReason
from tabsdata.connector.kafka.consumer.messages_buffer import MessagesBuffer
from tabsdata.connector.kafka.serdes.serde import KafkaSerde
from tabsdata.connector.kafka.typing import (
    AwsGlueSchemaRegistryCredentialsSpec,
    ConfluentSchemaRegistryCredentialsSpec,
    KafkaCredentialsSpec,
    KafkaMessageFormatSpec,
    KafkaServersSpec,
    Protobuf,
)
from tabsdata.connector.kafka.utils import (
    handle_rollover,
    obtain_and_set_aws_glue_credentials,
)
from tabsdata.exceptions import (
    ErrorCode,
    StageTriggerConfigurationError,
    StageTriggerExecutionError,
)

if TYPE_CHECKING:
    from confluent_kafka import Consumer, Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@unstable()
@deprecated(
    """Class 'KafkaSchemaRegistry' is considered unstable and is subject to change in future releases.""",  # noqa: E231,E241,E501
    category=unstable_category(
        reason=(
            "Class 'KafkaSchemaRegistry' may undergo API and feature changes as it"
            " evolves toward full capability and stability."
        ),
        since="1.6.0",
        replacement="There is no current stable replacement for this class.",
    ),
)
class KafkaSchemaRegistry(ABC):
    """
    Base class for schema registry configurations.
    """


@unstable()
@deprecated(
    """Class 'ConfluentSchemaRegistry' is considered unstable and is subject to change in future releases.""",  # noqa: E231,E241,E501
    category=unstable_category(
        reason=(
            "Class 'ConfluentSchemaRegistry' may undergo API and feature changes as it"
            " evolves toward full capability and stability."
        ),
        since="1.6.0",
        replacement="There is no current stable replacement for this class.",
    ),
)
class ConfluentSchemaRegistry(KafkaSchemaRegistry):
    """
    Configuration for Confluent Schema Registry.
    """

    @_td_validate_call(ErrorCode.STCE101)
    def __init__(
        self,
        url: str,
        credentials: ConfluentSchemaRegistryCredentialsSpec | None = None,
        configs: dict | None = None,
    ):
        """
        Initializes the Confluent Schema Registry configuration.

        Args:
            url: The URL of the Schema Registry.
            credentials: Credentials for authentication.
            configs: Optional dictionary of additional configuration properties.
        """
        self._url = url
        self._credentials = credentials
        self._configs = configs or {}

    @property
    def url(self) -> str:
        """
        The URL of the Schema Registry.
        """
        return self._url

    @property
    def credentials(self) -> ConfluentSchemaRegistryCredentialsSpec | None:
        """
        Credentials for authenticating with the Schema Registry.
        """
        return self._credentials

    @property
    def configs(self) -> dict:
        """
        Additional configuration properties for the Schema Registry client.
        """
        return self._configs

    def __repr__(self):
        return f"ConfluentSchemaRegistry(url={self.url}, configs={self.configs})"


@unstable()
@deprecated(
    """Class 'AwsGlueSchemaRegistry' is considered unstable and is subject to change in future releases.""",  # noqa: E231,E241,E501
    category=unstable_category(
        reason=(
            "Class 'AwsGlueSchemaRegistry' may undergo API and feature changes as it"
            " evolves toward full capability and stability."
        ),
        since="1.6.0",
        replacement="There is no current stable replacement for this class.",
    ),
)
class AwsGlueSchemaRegistry(KafkaSchemaRegistry):
    """
    Configuration for AWS Glue Schema Registry.
    """

    @_td_validate_call(ErrorCode.STCE102)
    def __init__(
        self,
        region_name: str,
        credentials: AwsGlueSchemaRegistryCredentialsSpec,
        registry_name: str,
        schema_name: str,
        configs: dict | None = None,
    ):
        """
        Initializes the AWS Glue Schema Registry configuration.

        Args:
            region_name: The AWS region where the registry is located.
            credentials: Credentials for authenticating with AWS.
            registry_name: The name of the Glue Schema Registry.
            schema_name: The name of the schema within the registry.
            configs: Optional dictionary of additional configuration properties.
        """
        self._region_name = region_name
        self._credentials = credentials
        self._registry_name = registry_name
        self._schema_name = schema_name
        self._configs = configs or {}

    @property
    def region_name(self) -> str:
        """
        The AWS region of the Schema Registry.
        """
        return self._region_name

    @property
    def credentials(self) -> AwsGlueSchemaRegistryCredentialsSpec:
        """
        Credentials for authenticating with AWS Glue.
        """
        return self._credentials

    @property
    def registry_name(self) -> str:
        """
        Registry name for AWS Glue Schema Registry.
        """
        return self._registry_name

    @property
    def schema_name(self) -> str:
        """
        Schema name for AWS Glue Schema Registry.
        """
        return self._schema_name

    @property
    def configs(self) -> dict:
        """
        Additional configuration properties for the AWS Glue client.
        """
        return self._configs

    def __repr__(self):
        return (
            f"AwsGlueSchemaRegistry(region_name={self.region_name},"
            f" configs={self.configs})"
        )


SchemaRegistrySpec: TypeAlias = Union[ConfluentSchemaRegistry | AwsGlueSchemaRegistry]


@unstable()
@deprecated(
    """Class 'KafkaConn' is considered unstable and is subject to change in future releases.""",  # noqa: E231,E241,E501
    category=unstable_category(
        reason=(
            "Class 'KafkaConn' may undergo API and feature changes as it evolves toward"
            " full capability and stability."
        ),
        since="1.6.0",
        replacement="There is no current stable replacement for this class.",
    ),
)
class KafkaConn(Conn):
    """
    Kafka Consumer Connection configuration.
    """

    @_td_validate_call(ErrorCode.STCE100)
    def __init__(
        self,
        servers: KafkaServersSpec,
        credentials: KafkaCredentialsSpec,
        group_id: str | None = None,
        schema_registry: SchemaRegistrySpec | None = None,
        enforce_connection_params: bool = True,
        cx_src_configs: dict | None = None,
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
    def credentials(self) -> KafkaCredentialsSpec:
        """
        Kafka credentials.
        """
        return self._credentials

    @property
    def group_id(self) -> str | None:
        """
        Kafka consumer group ID.
        """
        return self._group_id

    @property
    def schema_registry(self) -> SchemaRegistrySpec | None:
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


@unstable()
@deprecated(
    """Class 'KafkaTrigger' is considered unstable and is subject to change in future releases.""",  # noqa: E231,E241,E501
    category=unstable_category(
        reason=(
            "Class 'KafkaTrigger' may undergo API and feature changes as it evolves"
            " toward full capability and stability."
        ),
        since="1.6.0",
        replacement="There is no current stable replacement for this class.",
    ),
)
class KafkaTrigger(StageTrigger):
    """
    :categories: trigger
    A `StageTrigger` that consumes messages from a Kafka topic and stages them.

    This trigger connects to a Kafka cluster, consumes messages from a specified
    topic, and uses rollover conditions (time, size, and message count) to
    buffer and stage the data.
    """

    @_td_validate_call(ErrorCode.STCE103)
    def __init__(
        self,
        conn: KafkaConn,
        topic: str,
        data_format: KafkaMessageFormatSpec,
        schema: str,
        group_id: str | None = None,
        time_rollover_secs: PositiveInt = DEFAULT_TIME_ROLLOVER_SECS,
        size_rollover_mb: PositiveInt | None = None,
        messages_rollover: PositiveInt | None = None,
        dlq_topic: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ):
        """
        Initialize a KafkaTrigger instance.

        Args:
            conn: The Kafka consumer connection configuration.
            topic: The Kafka topic to consume from.
            data_format: The data format of the messages
                         ("avro", "json", "protobuf").
            schema: The schema string for deserializing messages.
            group_id: Optional Kafka consumer group ID.
            schema: Optional schema name for AWS Glue Schema Registry.
            time_rollover_secs: The timeout in seconds before staging and
                                committing a message buffer. Defaults to 60.
            size_rollover_mb: Optional size-based rollover configuration. Size
                              is specified in megabytes.
            messages_rollover: Optional message count-based rollover
                               configuration.
            dlq_topic: Optional dead-letter queue topic.
            start: Optional datetime for the trigger to start working.
            end: Optional datetime for the trigger to stop working.
        """
        super().__init__(start=start, end=end)
        try:
            from confluent_kafka import Consumer, Producer  # noqa: F401
            from confluent_kafka.schema_registry import (  # noqa: F401
                SchemaRegistryClient,
            )
        except ImportError:
            raise ImportError(
                "The 'tabsdata-kafka' package is missing some dependencies. You"
                "can get them by installing 'tabsdata['kafka']'"
            )
        self._conn = conn
        self._topic = topic
        self._data_format = data_format
        self._schema = schema
        self._group_id = group_id
        self._time_rollover_secs = time_rollover_secs
        self._size_rollover_mb = size_rollover_mb
        self._messages_rollover = messages_rollover
        self._dlq_topic = dlq_topic

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
    def group_id(self) -> str | None:
        """
        Kafka consumer group ID.
        """
        return self._group_id

    @property
    def time_rollover_secs(self) -> int:
        """
        Timeout in seconds for consuming each message.
        """
        return self._time_rollover_secs

    @property
    def size_rollover_mb(self) -> PositiveInt | None:
        """
        Size-based rollover configuration. Size is specified in megabytes.
        """
        return self._size_rollover_mb

    @property
    def messages_rollover(self) -> PositiveInt | None:
        """
        Message count-based rollover configuration.
        """
        return self._messages_rollover

    @property
    def dlq_topic(self) -> str | None:
        """
        Dead-letter queue topic.
        """
        return self._dlq_topic

    def _build_schema_registry_client(self) -> SchemaRegistryClient | GlueClient | None:
        """
        Builds and returns a client for the configured schema registry.

        Returns:
            A `SchemaRegistryClient` for Confluent, a `GlueClient` for AWS Glue,
            or `None` if no schema registry is configured.

        Raises:
            ValueError: If the schema registry type is not supported.
        """
        from confluent_kafka.schema_registry import SchemaRegistryClient

        sr = self.conn.schema_registry
        if not sr:
            return None
        elif isinstance(sr, ConfluentSchemaRegistry):
            sr_configs = {}
            if sr.configs:
                sr_configs = sr_configs | sr.configs

            sr_configs[URL_KEY] = sr.url
            if sr.credentials:
                sr_configs[BASIC_AUTH_USER_INFO_KEY] = (
                    f"{sr.credentials.user.secret_value}:"
                    f"{sr.credentials.password.secret_value}"
                )
            return SchemaRegistryClient(sr_configs)
        elif isinstance(sr, AwsGlueSchemaRegistry):
            boto_kwargs = {REGION_NAME_KEY: sr.region_name}
            if sr.configs:
                boto_kwargs = boto_kwargs | sr.configs
            if sr.credentials:
                obtain_and_set_aws_glue_credentials(sr.credentials)
            return boto3.client("glue", **boto_kwargs)
        raise ValueError(f"Unsupported schema registry type: {type(sr)}")

    def _build_kafka_configs(self) -> dict:
        """
        Builds the configuration dictionary for the Kafka consumer.

        Returns:
            A dictionary of Kafka consumer configurations.

        Raises:
            StageTriggerConfigurationError: If `group.id` is not set.
        """
        kafka_configs = {}
        if self.conn.cx_src_configs:
            kafka_configs = kafka_configs | self.conn.cx_src_configs

        kafka_configs[BOOTSTRAP_SERVERS_KEY] = (
            self.conn.servers
            if isinstance(self.conn.servers, str)
            else ",".join(self.conn.servers)
        )
        kafka_configs[AUTO_OFFSET_RESET_KEY] = "earliest"
        kafka_configs[ENABLE_AUTO_COMMIT_KEY] = False

        if self.conn.credentials:
            kafka_configs[SECURITY_PROTOCOL_KEY] = "sasl_plaintext"
            kafka_configs[SASL_MECHANISM_KEY] = "SCRAM-SHA-256"
            kafka_configs[SASL_USERNAME_KEY] = self.conn.credentials.user.secret_value
            kafka_configs[SASL_PASSWORD_KEY] = (
                self.conn.credentials.password.secret_value
            )

        # Set group.id based on enforce_connection_params flag
        if self.conn.enforce_connection_params:
            kafka_configs[GROUP_ID_KEY] = (
                self.conn.group_id if self.conn.group_id else self.group_id
            )
        else:
            kafka_configs[GROUP_ID_KEY] = self.group_id

        # Validate that group.id is set
        if not kafka_configs.get(GROUP_ID_KEY):
            raise StageTriggerConfigurationError(
                ErrorCode.STE6,
                self.conn.__class__.__name__,
                self.__class__.__name__,
                kafka_configs.get(GROUP_ID_KEY),
            )

        return kafka_configs

    def run(self, context: StageTriggerContext):
        """
        Run the Kafka stage trigger to consume messages, buffer them, and stage them
        based on rollover conditions.

        Args:
            context: The StageTriggerContext instance.
        """
        self._setup_sigterm_handler()
        serde = self._get_serde(str(context.working_dir))
        logger.debug(f"Using serde: {type(serde).__name__}")

        kafka_configs = self._build_kafka_configs()
        consumer = self._create_consumer(kafka_configs)
        dlq_producer = self._create_dlq_producer(kafka_configs)

        consumer.subscribe([self.topic])
        kafka_consumer = self._create_kafka_consumer(consumer, serde, dlq_producer)

        self._consume_loop(context, kafka_consumer, consumer, dlq_producer, serde)

    @staticmethod
    def _setup_sigterm_handler():
        """
        Sets up a signal handler for SIGTERM to raise a KeyboardInterrupt.
        This allows for graceful shutdown when the process is terminated.
        """

        def _sigterm_handler(_signum, _frame):
            raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, _sigterm_handler)

    @staticmethod
    def _create_consumer(kafka_configs: dict) -> Consumer:
        """
        Creates and returns a Kafka Consumer instance.

        Args:
            kafka_configs: The configuration for the Kafka consumer.

        Returns:
            A configured `confluent_kafka.Consumer` instance.

        Raises:
            Exception: If the connection to the Kafka broker fails.
        """
        from confluent_kafka import Consumer

        try:
            consumer = Consumer(kafka_configs)
            consumer.list_topics(timeout=CONNECTION_TIMEOUT_SECS)
            return consumer
        except Exception as e:
            logger.error(f"Error connecting to the Kafka broker: {e}")
            raise e

    def _create_dlq_producer(self, kafka_configs: dict) -> Producer | None:
        """
        Creates a Kafka Producer for the dead-letter queue if configured.

        Args:
            kafka_configs: The configuration for the Kafka producer.

        Returns:
            A `confluent_kafka.Producer` instance if a DLQ topic is set,
            otherwise `None`.

        Raises:
            Exception: If the producer fails to connect to the Kafka broker.
        """
        from confluent_kafka import Producer

        try:
            if self.dlq_topic:
                dlq_producer = Producer(kafka_configs)
                dlq_producer.list_topics(timeout=CONNECTION_TIMEOUT_SECS)
                return dlq_producer
            return None
        except Exception as e:
            logger.error(f"Error creating DLQ producer: {e}")
            raise e

    def _create_kafka_consumer(
        self, consumer: Consumer, serde: KafkaSerde, dlq_producer: Producer | None
    ) -> KafkaConsumer:
        """
        Creates the internal `KafkaConsumer` wrapper.

        Args:
            consumer: The `confluent_kafka.Consumer` instance.
            serde: The serializer/deserializer instance.
            dlq_producer: The producer for the dead-letter queue.

        Returns:
            An initialized `KafkaConsumer` instance.
        """
        kafka_consumer = KafkaConsumer(
            topic=self.topic,
            consumer=consumer,
            serde=serde,
            size_rollover_mb=self.size_rollover_mb,
            messages_rollover=self.messages_rollover,
            dlq_producer=dlq_producer,
            dlq_topic=self.dlq_topic,
        )
        logger.debug(f"Kafka consumer created. {kafka_consumer}")
        return kafka_consumer

    def _consume_loop(
        self,
        context: StageTriggerContext,
        kafka_consumer: KafkaConsumer,
        consumer: Consumer,
        dlq_producer: Producer | None,
        serde: KafkaSerde,
    ):
        """
        The main loop for consuming messages from Kafka.

        This method continuously polls for messages, handles rollovers, and manages
        the message buffer.

        Args:
            context: The trigger context for monitoring and staging.
            kafka_consumer: The internal consumer wrapper.
            consumer: The underlying `confluent_kafka.Consumer`.
            dlq_producer: The DLQ producer.
            serde: The serializer/deserializer.
        """
        from tabsdata.connector.kafka.consumer.messages_buffer import MessagesBuffer

        buffer: MessagesBuffer | None = None
        stage_trigger_status = None
        rollover_reason: RolloverReason | None = None
        try:
            start = time.time()
            buffer = MessagesBuffer(self.topic, serde.schema_id_compatibles_checker())
            reset_buffer_on_time_rollover = False
            stage_trigger_status = StageTriggerStatus.SUCCESS
            while True:
                if self._end is not None and datetime.now(timezone.utc) > self._end:
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
                    f" {buffer.size_mb} MB, Message count: {buffer.message_count}"
                )
                max_timeout = self.time_rollover_secs - (time.time() - start)
                if buffer is not None and (
                    rollover_reason != RolloverReason.Time or max_timeout <= 0
                ):
                    start = handle_rollover(
                        context=context,
                        buffer=buffer,
                        rollover_reason=cast(RolloverReason, rollover_reason),
                        working_dir=str(context.working_dir),
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
            self._finalize_consumption(
                context,
                buffer,
                rollover_reason,
                kafka_consumer,
                consumer,
                dlq_producer,
                stage_trigger_status,
            )

    def _finalize_consumption(
        self,
        context: StageTriggerContext,
        buffer: MessagesBuffer | None,
        rollover_reason: RolloverReason | None,
        kafka_consumer: KafkaConsumer,
        consumer: Consumer,
        dlq_producer: Producer | None,
        stage_trigger_status: StageTriggerStatus | None,
    ):
        """
        Handles the final steps of consumption before shutting down.

        This includes a final buffer rollover, flushing the DLQ producer, and
        closing the Kafka consumer.

        Args:
            context: The trigger context.
            buffer: The current message buffer.
            rollover_reason: The reason for the last rollover.
            kafka_consumer: The internal consumer wrapper.
            consumer: The underlying `confluent_kafka.Consumer`.
            dlq_producer: The DLQ producer.
            stage_trigger_status: The final status of the trigger.
        """
        if buffer is not None:
            logger.debug("Final buffer rollover before shutdown.")
            handle_rollover(
                context=context,
                buffer=buffer,
                rollover_reason=rollover_reason,
                working_dir=str(context.working_dir),
                kafka_consumer=kafka_consumer,
            )
        if dlq_producer:
            logger.debug("Flushing DLQ producer before shutdown.")
            dlq_producer.flush()
        logger.debug("Closing Kafka consumer.")
        kafka_consumer.close()
        consumer.close()
        logger.debug("Kafka consumer closed.")

        # Check if return_on_end attribute exists and is True
        if getattr(self, "return_on_end", False):
            if stage_trigger_status == StageTriggerStatus.SUCCESS:
                pass
            else:
                raise StageTriggerExecutionError(
                    ErrorCode.STR1,
                    "KafkaStageTrigger execution failed with status:"
                    f" {stage_trigger_status.name}",
                )
        else:
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

        from tabsdata.connector.kafka.serdes.aws_glue_sr_serde import (
            AvroGlueKafkaSerde,
            JsonGlueKafkaSerde,
            ProtobufGlueKafkaSerde,
        )
        from tabsdata.connector.kafka.serdes.confluent_sr_serde import (
            AvroSrKafkaSerde,
            JsonSrKafkaSerde,
            ProtobufSrKafkaSerde,
        )
        from tabsdata.connector.kafka.serdes.non_sr_serde import (
            AvroKafkaSerde,
            JsonKafkaSerde,
            ProtobufKafkaSerde,
        )

        schema_registry_client = self._build_schema_registry_client()

        from confluent_kafka.schema_registry import SchemaRegistryClient

        def _raise_unsupported():
            raise ValueError(
                f"Serde not created: unsupported data format '{self.data_format}'."
            )

        def format_key() -> str:
            if isinstance(self.data_format, Protobuf):
                return _PROTOBUF
            else:
                return cast(str, self.data_format)

        def non_sr_serde():
            return {
                _AVRO: lambda: AvroKafkaSerde(avro_schema=self.schema),
                _JSON: lambda: JsonKafkaSerde(json_schema=self.schema),
                _PROTOBUF: lambda: ProtobufKafkaSerde(
                    protobuf_schema=self.schema,
                    message_name=self.data_format.message_name,
                    working_dir=working_dir,
                ),
            }.get(format_key(), lambda: _raise_unsupported())()

        def sr_serde():
            return {
                _AVRO: lambda: AvroSrKafkaSerde(
                    schema_registry_client=schema_registry_client,
                    avro_schema=self.schema,
                ),
                _JSON: lambda: JsonSrKafkaSerde(
                    schema_registry_client=schema_registry_client,
                    json_schema=self.schema,
                ),
                _PROTOBUF: lambda: ProtobufSrKafkaSerde(
                    schema_registry_client=schema_registry_client,
                    protobuf_schema=self.schema,
                    message_name=self.data_format.message_name,
                    working_dir=working_dir,
                ),
            }.get(format_key(), lambda: _raise_unsupported())()

        def glue_serde():
            return {
                _AVRO: lambda: AvroGlueKafkaSerde(
                    glue_client=schema_registry_client,
                    registry_name=self.conn.schema_registry.registry_name,
                    schema_name=self.conn.schema_registry.schema_name,
                    avro_schema=self.schema,
                ),
                _JSON: lambda: JsonGlueKafkaSerde(
                    glue_client=schema_registry_client,
                    registry_name=self.conn.schema_registry.registry_name,
                    schema_name=self.conn.schema_registry.schema_name,
                    json_schema=self.schema,
                ),
                _PROTOBUF: lambda: ProtobufGlueKafkaSerde(
                    glue_client=schema_registry_client,
                    registry_name=self.conn.schema_registry.registry_name,
                    schema_name=self.conn.schema_registry.schema_name,
                    protobuf_schema=self.schema,
                    message_name=self.data_format.message_name,
                    working_dir=working_dir,
                ),
            }.get(format_key(), lambda: _raise_unsupported())()

        match schema_registry_client:
            case None:
                return non_sr_serde()
            case SchemaRegistryClient():
                return sr_serde()
            case BaseClient():
                return glue_serde()
            case _:
                raise ValueError(
                    f"Unsupported schema registry client: {schema_registry_client}"
                )
