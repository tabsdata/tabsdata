#
# Copyright 2026 Tabsdata Inc.
#

"""Redpanda broker. Domain == topic. No SASL in this setup."""

from __future__ import annotations

import os
import platform
import subprocess

from ...errors import DatagenError
from ...shell import docker_run, find_free_port
from .._container import AdminConn, ContainerBase, DomainResult
from ..base import DomainContext, LoadRequest, _build_identity


class RedpandaSystem(ContainerBase):
    slug = "redpanda"
    label = "Redpanda"
    domain_desc = "a topic"
    domain_flag = "topic"
    supports_sql = False
    kind = "kafka"

    image = (
        f"redpandadata/redpanda:{os.environ.get('REDPANDA_IMAGE_VERSION', 'v24.3.5')}"
    )
    image_platform = (
        "linux/arm64" if platform.machine() in ("arm64", "aarch64") else "linux/amd64"
    )
    internal_port = 9092
    # Redpanda serves a Confluent-compatible schema registry on 8081 by
    # default; we publish it on a host port so `data load --use-schema-registry`
    # can reach it from the host.
    schema_registry_internal_port = 8081
    system_user = ""
    password_env_name = ""
    extra_env: list[tuple[str, str]] = []
    extra_flags: list[str] = []
    # The consumer GROUP_ID is surfaced (exported) so a source connection built
    # from a domain descriptor can consume under a per-build group.
    exported_domain_vars = ("GROUP_ID",)

    _registry_port: int | None = None

    # ---- data plane: produce to kafka (overrides ContainerBase's SQL path) ----

    def load_data(self, ctx: DomainContext, req: LoadRequest) -> int:
        """One topic per entity (`<domain>.<entity>`), one message per row,
        serialized as `req.ser`; schema files written to `req.schemas_dir`."""
        from ... import kafka_producer
        from ...emit import read_structured

        if not req.ser:
            raise DatagenError(
                f"--ser is required for kafka domains "
                f"(one of {', '.join(kafka_producer.SER_FORMATS)})"
            )
        if not req.schemas_dir:
            raise DatagenError("--schemas DIR is required for a kafka domain")
        meta = ctx.meta
        structured = read_structured(req.data_dir)
        brokers = meta.get("KAFKA_BROKERS") or (
            f"{meta.get('HOST', 'localhost')}:{meta.get('PORT', '9092')}"
        )
        registry_url = meta.get("SCHEMA_REGISTRY_URL") or None
        print(
            f">>> producing to kafka {brokers} (ser={req.ser}, "
            f"schema-registry={'on' if req.use_schema_registry else 'off'}) ..."
        )
        counts = kafka_producer.produce_model(
            structured=structured,
            brokers=brokers,
            domain=meta.get("DB", ""),
            ser=req.ser,
            schemas_dir=req.schemas_dir,
            use_schema_registry=req.use_schema_registry,
            registry_url=registry_url,
        )
        for topic, n in counts.items():
            print(f"    {topic}: {n} message(s)")
        print(f">>> schema files written to {req.schemas_dir}")
        return sum(counts.values())

    def generate_password(self) -> str:
        return ""

    def _docker_run(self, container: str, host_port: int, password: str) -> str:
        # Redpanda needs `redpanda start` after the image AND --advertise-kafka-addr
        # baked at start time. We bind two listeners: internal:9092 (for rpk inside)
        # and external:HOST_PORT (for clients on the host). We also publish the
        # schema registry (container 8081) on a free host port (allocated at plan
        # time by `_system_extra_vars`, so it's in the descriptor before start).
        if self._registry_port is None:
            self._registry_port = find_free_port()
        args = [
            "-d",
            "--platform",
            self.image_platform,
            "-p",
            f"{host_port}:{host_port}",
            "-p",
            f"{self._registry_port}:{self.schema_registry_internal_port}",
            "--name",
            container,
            self.image,
            "redpanda",
            "start",
            "--smp",
            "1",
            "--memory",
            "1G",
            "--overprovisioned",
            "--kafka-addr",
            f"internal://0.0.0.0:9092,external://0.0.0.0:{host_port}",
            "--advertise-kafka-addr",
            f"internal://localhost:9092,external://localhost:{host_port}",
        ]
        return docker_run(args)

    def _system_extra_vars(self) -> dict[str, str]:
        # Baked into the system script + forwarded via its delegate export,
        # so `<sys> create` (which runs create_domain) can read it. Allocated
        # here (plan time) so the port lands in the descriptor before start.
        if self._registry_port is None:
            self._registry_port = find_free_port()
        port = self._registry_port
        return {
            "REGISTRY_PORT": str(port or ""),
            "SCHEMA_REGISTRY_URL": f"http://localhost:{port}" if port else "",
        }

    def _domain_extra_vars(self) -> dict[str, str]:
        # Surfaced in the domain's `meta` for the kafka loader: the bootstrap
        # broker(s) and the schema-registry endpoint (forwarded from the
        # system script's exported env). GROUP_ID is the per-build consumer
        # group id (the build identity, matching the DOMAIN_PREFIX).
        return {
            "KAFKA_BROKERS": f"localhost:{os.environ.get('PORT', '9092')}",
            "SCHEMA_REGISTRY_URL": os.environ.get("SCHEMA_REGISTRY_URL", ""),
            "GROUP_ID": _build_identity(),
        }

    def check_ready(self, container: str, password: str) -> bool:
        return (
            subprocess.run(
                ["docker", "exec", container, "rpk", "cluster", "info"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            ).returncode
            == 0
        )

    def _admin_client(self, admin: AdminConn):
        from confluent_kafka.admin import AdminClient

        return AdminClient({"bootstrap.servers": f"{admin.host}:{admin.port}"})

    def domain_result(self, admin: AdminConn, name: str, password: str) -> DomainResult:
        # Kafka topics carry no per-domain credentials, so the generated
        # `password` is unused.
        return DomainResult(
            db=name,
            user="",
            password="",
            sqlalchemy_uri=f"kafka://{admin.host}:{admin.port}/{name}",
        )

    def do_create(self, admin: AdminConn, result: DomainResult) -> None:
        from confluent_kafka.admin import NewTopic

        # Keep `client` referenced until the future resolves — if the
        # AdminClient is GC'd first, librdkafka tears the broker handle down
        # ("Handle is terminating") and the future fails.
        client = self._admin_client(admin)
        futures = client.create_topics([
            NewTopic(result.db, num_partitions=1, replication_factor=1)
        ])
        try:
            futures[result.db].result()  # block until created; raises on failure
        except Exception as exc:
            raise DatagenError(
                f"kafka create topic '{result.db}' failed: {exc}"
            ) from exc

    def do_delete(self, admin: AdminConn, name: str) -> None:
        # A domain seeds one topic per entity, named `<domain>.<entity>`, so drop
        # the domain topic AND every per-entity topic under it — otherwise destroy
        # / force-recreate leave orphaned `<domain>.*` topics and a re-seed hits
        # TOPIC_ALREADY_EXISTS.
        client = self._admin_client(admin)
        try:
            md = client.list_topics(timeout=10)
        except Exception as exc:
            raise DatagenError(f"kafka list topics failed: {exc}") from exc
        targets = [t for t in md.topics if t == name or t.startswith(f"{name}.")]
        if not targets:
            return
        futures = client.delete_topics(targets)
        errors = []
        for t in targets:
            try:
                futures[t].result()
            except Exception as exc:  # noqa: BLE001 - report all, don't stop at first
                errors.append(f"{t}: {exc}")
        if errors:
            raise DatagenError("kafka delete topic(s) failed: " + "; ".join(errors))

    def do_list(self, admin: AdminConn) -> list[str]:
        client = self._admin_client(admin)
        try:
            md = client.list_topics(timeout=10)
        except Exception as exc:
            raise DatagenError(f"kafka list topics failed: {exc}") from exc
        # Skip Kafka/Redpanda internal topics (e.g. _schemas, __consumer_offsets).
        return [t for t in md.topics if not t.startswith("_")]
