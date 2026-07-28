#
# Copyright 2026 Tabsdata Inc.
#

# Salesforce domain resources. `models/<model>/` holds each model's snowfakery
# recipe and CumulusCI load mapping — the org's analogue of the SQL systems'
# per-model DDL. The `models`, `create-model` and `create-data` verbs are
# overridden here: the org's objects already exist (nothing to install) and its
# records are generated and bulk-uploaded rather than built as Tables, so the
# shared implementations, which all go through the datagen pipeline, do not fit.
