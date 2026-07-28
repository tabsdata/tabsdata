#
# Copyright 2026 Tabsdata Inc.
#

# Bundled models. Each models/<name>/ ships entities.json (the entity names it
# generates) and, when the shared pipeline builds its data, datagenerator.py (a
# datagenerator() -> Model factory); a model whose system loads it directly
# (salesforce) has no datagenerator and so is absent from datagen.list_models().
# model.json is optional and carries the managed venv the model's data plane
# needs (see _venv). Each system's per-model assets — DDL, or a generation recipe
# — live under systems/<slug>/domain/models/<name>/.
