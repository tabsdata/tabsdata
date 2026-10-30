#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import importlib
import json
import shutil
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
import yaml

from tabsdatak.common._archive.bundle import build_bundle
from tabsdatak.server._tdrunner.function.commands import (
    begin_execution,
    get_bundle,
    prep_localize_state,
    prep_upload_raw,
    report_completion,
    run_transporter,
    stamp_inputs,
)
from tabsdatak.server._tdrunner.function.commands.runners import (
    destination_plugin as run_destination_plugin,
    function as run_function,
    source_plugin as run_source_plugin,
)

from ..conftest import info_payload

PUBLISHER = "publisher"
TRANSFORMER = "transformer"
SUBSCRIBER = "subscriber"

STORAGE_MOUNT = "ROOT"

SHARED_FOLDERS = (
    "request",
    "fn/in",
    "fn/out",
    "in",
    "rich",
    "out",
    "state",
    "response",
    "shuttle",
)


@dataclass
class Run:
    outputs: dict[str, pl.DataFrame]
    response: dict
    callbacks: list[dict]
    delivered: pl.DataFrame | None = None
    root: Path = field(default_factory=Path)


@dataclass
class Function:
    kind: str
    project: Path
    package: str
    module: str
    name: str

    def load(self):
        module = importlib.import_module(f"{self.package}.{self.module}")
        return getattr(module, self.name)


class Driver:
    def __init__(self, root: Path, monkeypatch, responder_url: str, posts: list):
        self.root = root
        self.monkeypatch = monkeypatch
        self.responder_url = responder_url
        self.posts = posts
        self.shared = root / "shared"
        for folder in SHARED_FOLDERS:
            (self.shared / folder).mkdir(parents=True, exist_ok=True)
        self.storage = root / "storage.yaml"
        self.storage.write_text(
            yaml.safe_dump({
                "storage": {
                    "mounts": [
                        {"id": STORAGE_MOUNT, "uri": root.as_uri(), "options": {}},
                    ],
                },
            })
        )
        self.store = root / "store"
        self.store.mkdir()

    def path(self, relative: str) -> Path:
        return self.shared / relative

    @property
    def request(self) -> Path:
        return self.path("request/request.yaml")

    def step(self, command, name: str, **flags: Path | str) -> None:
        arguments = [name]
        for flag, value in flags.items():
            arguments += [f"--{flag.replace('_', '-')}", str(value)]
        arguments += [
            "--liveness",
            str(self.path("liveness")),
            "--journal-file",
            str(self.path("journal.yaml")),
            "--responder-url",
            self.responder_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer function-api",
        ]
        self.monkeypatch.setattr(sys, "argv", arguments)
        exit_code = command.main()
        assert exit_code in (0, None), (
            f"Command {name} exited with code {exit_code}; "
            f"last callback: {self.posts[-1:]}"
        )

    def transport(self, request: str, report: str) -> None:
        self.step(
            run_transporter,
            "run-transporter",
            request=self.path(request),
            report=self.path(report),
        )

    def write_request(self, bundle: Path, body: dict) -> None:
        info = info_payload(bundle_uri=bundle.as_uri(), **body.pop("info", {}))
        self.request.write_text(yaml.safe_dump({"Function": {"info": info, **body}}))

    def output_table(self, name: str, position: int) -> dict:
        return {
            "Table": {
                "collection_id": 1,
                "collection": "c",
                "table_id": f"T-{name}",
                "table": name,
                "table_version_id": f"TV-{name}",
                "logical_table_data_version_id": f"LDV-{name}",
                "physical_table_data_version_id": f"PDV-{name}",
                "location": {
                    "uri": (self.store / f"{name}.parquet").as_uri(),
                    "env_prefix": STORAGE_MOUNT,
                },
                "table_pos": position,
            }
        }

    def input_table(
        self,
        name: str,
        position: int,
        index: int,
        source: Path,
        version: int | None = None,
    ) -> dict:
        suffix = "" if version is None else f".{version}"
        return {
            "collection_id": 1,
            "collection": "c",
            "table_id": f"T-{name}",
            "table": name,
            "table_version_id": f"TV-{name}",
            "input_idx": index,
            "table_pos": position,
            "version_pos": 0 if version is None else version,
            "execution_id": 2,
            "transaction_id": 3,
            "triggered_on": 1780000000100,
            "logical_table_data_version_id": f"LDV-{name}{suffix}",
            "physical_table_data_version_id": f"PDV-{name}{suffix}",
            "location": {"uri": source.as_uri(), "env_prefix": STORAGE_MOUNT},
        }

    def inputs(self, inputs: dict[str, pl.DataFrame | list[pl.DataFrame]]) -> list:
        catalog = self.root / "catalog"
        catalog.mkdir()
        entries = []
        index = 0
        for position, (name, value) in enumerate(inputs.items()):
            if isinstance(value, pl.DataFrame):
                value.write_parquet(catalog / f"{name}.parquet")
                entries.append({
                    "Table": self.input_table(
                        name, position, index, catalog / f"{name}.parquet"
                    )
                })
                index += 1
                continue
            versions = []
            for version, frame in enumerate(value):
                source = catalog / f"{name}.{version}.parquet"
                frame.write_parquet(source)
                versions.append(
                    self.input_table(name, position, index, source, version)
                )
                index += 1
            entries.append({
                "TableVersions": {"is_empty": not versions, "versions": versions}
            })
        return entries

    def begin(self) -> None:
        self.step(begin_execution, "begin-execution")
        self.step(
            get_bundle,
            "get-bundle",
            request=self.request,
            bundle_folder=self.path("bundle"),
            bundle_archive=self.path("shuttle/bundle.tar.zst"),
        )
        self.step(
            prep_localize_state,
            "prep-localize-state",
            request=self.request,
            inputs_manifest=self.path("fn/in/inputs-manifest.yaml"),
        )

    def run_function(self, inputs_manifest: str) -> None:
        self.step(
            run_function,
            "run-function",
            request=self.request,
            bundle_folder=self.path("bundle"),
            inputs_manifest=self.path(inputs_manifest),
            output_folder=self.path("out"),
            outputs_manifest=self.path("fn/out/outputs-manifest.yaml"),
            state_folder=self.path("state"),
        )

    def complete(self) -> None:
        self.step(
            report_completion,
            "report-completion",
            request=self.request,
            outputs_manifest=self.path("fn/out/outputs-manifest.yaml"),
            response=self.path("response/response.yaml"),
        )

    def publisher(self, connection: Path) -> None:
        self.step(
            run_source_plugin,
            "run-source-plugin",
            request=self.request,
            bundle_folder=self.path("bundle"),
            output_folder=self.path("in"),
            inputs_manifest=self.path("fn/in/inputs-manifest.yaml"),
            state_folder=self.path("state"),
            connection_file=connection,
        )
        self.step(
            stamp_inputs,
            "stamp-inputs",
            request=self.request,
            inputs_manifest=self.path("fn/in/inputs-manifest.yaml"),
            enriched_folder=self.path("rich"),
            enriched_inputs_manifest=self.path("fn/in/enriched-inputs-manifest.yaml"),
        )
        self.step(
            prep_upload_raw,
            "prep-upload-raw",
            request=self.request,
            inputs_manifest=self.path("fn/in/enriched-inputs-manifest.yaml"),
            transporter_request=self.path("shuttle/landing-request.yaml"),
            storage_configuration=self.storage,
        )
        self.transport("shuttle/landing-request.yaml", "shuttle/landing-report.yaml")
        self.run_function("fn/in/enriched-inputs-manifest.yaml")

    def transformer(self) -> None:
        self.run_function("fn/in/inputs-manifest.yaml")

    def subscriber(self, connection: Path) -> None:
        self.run_function("fn/in/inputs-manifest.yaml")
        self.step(
            run_destination_plugin,
            "run-destination-plugin",
            request=self.request,
            bundle_folder=self.path("bundle"),
            outputs_manifest=self.path("fn/out/outputs-manifest.yaml"),
            connection_file=connection,
        )

    def run(
        self,
        function: Function,
        *,
        outputs: list[str],
        inputs: dict[str, pl.DataFrame | list[pl.DataFrame]] | None = None,
        source_files: dict[str, pl.DataFrame] | None = None,
        connection: Callable[[Path], dict] | None = None,
    ) -> Run:
        bundle = self.root / "bundle.tar.zst"
        build_bundle(function.load(), function.project, bundle)
        body: dict = {
            "info": {
                "function_landing": {
                    "uri": (self.root / "landing").as_uri(),
                    "env_prefix": STORAGE_MOUNT,
                }
            },
            "output": [
                self.output_table(name, position)
                for position, name in enumerate(outputs)
            ],
        }
        if inputs:
            body["input"] = self.inputs(inputs)
        self.write_request(bundle, body)

        connection_file = None
        if connection is not None:
            base_path = self.root / "connection-base"
            base_path.mkdir()
            for name, frame in (source_files or {}).items():
                frame.write_parquet(base_path / name)
            connection_file = self.root / "connection.yaml"
            connection_file.write_text(yaml.safe_dump(connection(base_path)))

        self.begin()
        if function.kind == PUBLISHER:
            self.publisher(connection_file)
        elif function.kind == TRANSFORMER:
            self.transformer()
        else:
            self.subscriber(connection_file)
        self.complete()

        stored = {
            name: pl.read_parquet(self.store / f"{name}.parquet")
            for name in outputs
            if (self.store / f"{name}.parquet").exists()
        }
        delivered = None
        if function.kind == SUBSCRIBER:
            delivered = pl.read_parquet(self.root / "connection-base" / "sink.parquet")
        return Run(
            outputs=stored,
            response=yaml.safe_load(self.path("response/response.yaml").read_text()),
            callbacks=[json.loads(post["body"]) for post in self.posts if post["body"]],
            delivered=delivered,
            root=self.root,
        )


def copy_project(resources: Path, root: Path, monkeypatch, name: str) -> Path:
    project = root / name
    shutil.copytree(
        resources / name, project, ignore=shutil.ignore_patterns("__pycache__")
    )
    monkeypatch.syspath_prepend(str(project))
    importlib.invalidate_caches()
    return project


def forget(package: str) -> None:
    for name in list(sys.modules):
        if name == package or name.startswith(f"{package}."):
            del sys.modules[name]
