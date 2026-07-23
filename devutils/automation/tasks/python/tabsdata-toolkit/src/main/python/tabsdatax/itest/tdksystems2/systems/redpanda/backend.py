#
# Copyright 2026 Tabsdata Inc.
#

"""Redpanda backend — a docker broker; a domain is a Kafka topic.

The container needs a custom ``redpanda start`` command (internal + external
kafka listeners, and the Confluent-compatible schema registry published on a
host port), so ``provision_system`` is bespoke; readiness is ``rpk cluster
info``. A domain is a topic managed through the confluent-kafka AdminClient
(there is no per-topic auth). The data plane produces a model's rows as JSON
messages, one per row, to a ``<domain>.<entity>`` topic per entity (topics
auto-created on produce); ``modify-data`` is insert-only (a Kafka log only
appends). ``entities`` records the produced topics and their message counts.
"""

from __future__ import annotations

import json

from tabsdatax.itest.tdksystems2 import datagen, emit, naming, options
from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.shell import (
    container_exists,
    container_id,
    docker_pull,
    docker_rm,
    docker_run,
    require_docker,
)
from tabsdatax.itest.tdksystems2.systems._common import container_info
from tabsdatax.itest.tdksystems2.systems._common.sql import SqlContainerBackend

_REGISTRY_INTERNAL_PORT = 8081


class Redpanda(SqlContainerBackend):
    slug = "redpanda"
    label = "Redpanda"
    kind = "kafka"
    domain_word = "topic"
    max_entity_name_length = 249  # Kafka topic-name limit
    supports_changes = True  # insert-only (a Kafka log only appends)

    image = container_info.REDPANDA.ref
    image_platform = container_info.REDPANDA.platform
    internal_port = 9092

    def generate_password(self) -> str:
        return ""  # no SASL in this setup

    def options(self) -> tuple[options.Option, ...]:
        return (options.Option("PORT", "port", options.PORT, "a free host port"),)

    # ---- custom container lifecycle ------------------------------------

    def plan_system(
        self, name: str, overrides: dict[str, str] | None = None
    ) -> dict[str, str]:
        raw = (overrides or {}).get("PORT")
        kport = int(raw) if raw else self.next_free_port()
        reg = self.next_free_port()
        while reg == kport:
            reg = self.next_free_port()
        return {
            "CONTAINER": self.container_name(name),
            "IMAGE": self.image,
            "HOST": "localhost",
            "PORT": str(kport),
            "REGISTRY_PORT": str(reg),
            "SCHEMA_REGISTRY_URL": f"http://localhost:{reg}",
            "KAFKA_BROKERS": f"localhost:{kport}",
            "USER": "",
            "PASSWORD": "",
        }

    def provision_system(
        self, meta: dict[str, str], *, force: bool = False
    ) -> dict[str, str]:
        require_docker()
        container, port, reg = meta["CONTAINER"], meta["PORT"], meta["REGISTRY_PORT"]
        if container_exists(container):
            if force:
                docker_rm(container, force=True)
                print(f">>> --force: removed existing container {container}")
            else:
                raise TdxError(f"container already exists: {container} (use --force)")
        print(f">>> pulling docker image {self.image} …", flush=True)
        docker_pull(self.image, self.image_platform)
        print(f">>> starting container {container} from image {self.image}")
        docker_run([
            "-d",
            "--platform",
            self.image_platform,
            "-p",
            f"{port}:{port}",
            "-p",
            f"{reg}:{_REGISTRY_INTERNAL_PORT}",
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
            f"internal://0.0.0.0:9092,external://0.0.0.0:{port}",
            "--advertise-kafka-addr",
            f"internal://localhost:9092,external://localhost:{port}",
        ])
        print(f">>> waiting for {self.label} to become ready …")
        self._wait_ready(container, "", self.ready_timeout)
        cid = container_id(container)
        return {"CONTAINER_ID": cid} if cid else {}

    def check_ready(self, container: str, password: str) -> bool:
        from tabsdatax.itest.tdksystems2.shell import docker_exec_check

        return docker_exec_check(container, "rpk", "cluster", "info")

    # ---- topic domain plane (confluent-kafka AdminClient) --------------

    def _admin_client(self, meta: dict[str, str]):
        from confluent_kafka.admin import AdminClient

        return AdminClient({
            "bootstrap.servers": f"{meta.get('HOST', 'localhost')}:{meta['PORT']}"
        })

    def _topics(self, client) -> list[str]:
        try:
            return list(client.list_topics(timeout=10).topics)
        except Exception as exc:  # noqa: BLE001
            raise TdxError(f"kafka list topics failed: {exc}") from exc

    def _delete_topics(self, client, name: str, *, include_root: bool = True) -> None:
        targets = [
            t
            for t in self._topics(client)
            if (include_root and t == name) or t.startswith(f"{name}.")
        ]
        if not targets:
            return
        futures = client.delete_topics(targets)
        for t in targets:
            try:
                futures[t].result()
            except Exception:  # noqa: BLE001 - best-effort; report nothing
                pass

    def domain_meta(
        self, sys_meta: dict[str, str], entity_name: str, password: str
    ) -> dict[str, str]:
        brokers = f"{sys_meta['HOST']}:{sys_meta['PORT']}"
        return {
            "HOST": sys_meta["HOST"],
            "PORT": sys_meta["PORT"],
            "USER": "",  # redpanda has no per-topic auth
            "PASSWORD": "",
            "DB": entity_name,
            "IMAGE": sys_meta.get("IMAGE", ""),
            "KAFKA_BROKERS": brokers,
            "SCHEMA_REGISTRY_URL": sys_meta.get("SCHEMA_REGISTRY_URL", ""),
            "GROUP_ID": naming.build_identity(),
            "URI": f"kafka://{brokers}/{entity_name}",
        }

    def provision_domain(
        self,
        sys_meta: dict[str, str],
        entity_name: str,
        domain_meta: dict[str, str],
        *,
        force: bool = False,
    ) -> None:
        from confluent_kafka.admin import NewTopic

        client = self._admin_client(sys_meta)
        if entity_name in self._topics(client):
            if not force:
                raise TdxError(f"topic '{entity_name}' already exists (use --force)")
            self._delete_topics(client, entity_name)
        futures = client.create_topics([
            NewTopic(entity_name, num_partitions=1, replication_factor=1)
        ])
        try:
            futures[entity_name].result()
        except Exception as exc:  # noqa: BLE001
            raise TdxError(f"kafka create topic '{entity_name}' failed: {exc}") from exc

    def destroy_domain(self, domain_meta: dict[str, str]) -> None:
        self._delete_topics(self._admin_client(domain_meta), domain_meta["DB"])

    def list_domain_entities(self, sys_meta: dict[str, str]) -> list[str]:
        # A domain is one dot-free topic; per-entity `<domain>.<table>` topics go
        # with it, and Kafka internals (_schemas, __consumer_offsets) are skipped.
        return [
            t
            for t in self._topics(self._admin_client(sys_meta))
            if not t.startswith("_") and "." not in t
        ]

    def purge_domain_entity(self, sys_meta: dict[str, str], name: str) -> None:
        self._delete_topics(self._admin_client(sys_meta), name)

    # ---- data plane: produce a model's rows as JSON messages -----------

    def has_model(self, model: str) -> bool:
        return model in datagen.list_models()  # any model — no per-system schema

    def available_models(self) -> list[str]:
        return datagen.list_models()

    def data_plane_uri(self, meta: dict[str, str]) -> str:
        return meta["URI"]  # kafka://<brokers>/<domain-topic-prefix>

    @staticmethod
    def _brokers_prefix(uri: str) -> tuple[str, str]:
        brokers, _, prefix = uri.split("://", 1)[-1].partition("/")
        return brokers, prefix

    def install_schema(
        self, meta: dict[str, str], model: str, *, force: bool = False
    ) -> None:
        # topics are auto-created on produce; on reload clear the per-entity
        # topics (keep the domain topic itself) so messages don't duplicate.
        if force:
            self._delete_topics(
                self._admin_client(meta), meta.get("DB", ""), include_root=False
            )
        print(">>> kafka domain: topics auto-created on produce")

    def _produce(
        self, brokers: str, prefix: str, tables: list[emit.Table]
    ) -> dict[str, int]:
        from confluent_kafka import Producer

        producer = Producer({"bootstrap.servers": brokers})
        counts: dict[str, int] = {}
        for tbl in tables:
            topic = f"{prefix}.{tbl.name}"
            cols = [c for c in tbl.columns if not c.auto]
            for row in tbl.rows:
                record = {c.name: row.get(c.name) for c in cols}
                producer.produce(
                    topic,
                    json.dumps(record, default=str, separators=(",", ":")).encode(),
                )
                producer.poll(0)
            counts[topic] = len(tbl.rows)
        producer.flush()
        return counts

    def load_tables(self, uri: str, tables: list[emit.Table]) -> dict[str, int]:
        brokers, prefix = self._brokers_prefix(uri)
        rows = sum(len(t.rows) for t in tables)
        print(
            f">>> producing {rows} JSON message(s) to {len(tables)} topic(s) "
            f"on {brokers} …"
        )
        return self._produce(brokers, prefix, tables)

    def apply_changes(
        self, uri: str, transactions: list, before: dict[str, int]
    ) -> dict[str, int]:
        changes = [c for txn in transactions for c in txn]
        if any(c.kind is not datagen.OpKind.INSERT for c in changes):
            raise TdxError(
                f"{self.slug} topics are append-only for modify-data "
                f"(a change is a new message) — use --ops Insert"
            )
        by_table: dict[str, tuple[list, list]] = {}
        for c in changes:
            by_table.setdefault(c.table, (c.columns, []))[1].append(c.row)
        tables = [
            emit.Table(name=t, columns=cols, rows=rows)
            for t, (cols, rows) in by_table.items()
        ]
        brokers, prefix = self._brokers_prefix(uri)
        print(f">>> producing {len(changes)} JSON message(s) to {brokers} …")
        produced = self._produce(brokers, prefix, tables)
        new = dict(before)
        for topic, n in produced.items():
            new[topic] = new.get(topic, 0) + n  # kafka log appends
        return new

    def drop_all_tables(self, uri: str) -> None:
        from confluent_kafka.admin import AdminClient

        brokers, prefix = self._brokers_prefix(uri)
        client = AdminClient({"bootstrap.servers": brokers})
        self._delete_topics(client, prefix, include_root=False)
