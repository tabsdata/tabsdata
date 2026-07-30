# Copyright 2026 Tabsdata Inc.

Tabsdata Quickstart (AI)
========================

The same end-to-end dataflow as the `quickstart-dev` example, with the Python
left out. You get the input data and the two filesystem connections; an AI
agent writes the three functions -- a publisher, a transformer, and a
subscriber -- and wires them across three collections so that triggering the
publisher runs the whole flow.


What you get
------------

  persons.csv        100 rows with the columns first_name, last_name, age,
                     and nationality, in the input-data/ directory.
  conn-input.yaml    A tabsdatak.conn.localfile:LocalFileSrcConn reading from
                     the input-data/ directory.
  conn-output.yaml   A tabsdatak.conn.localfile:LocalFileDestConn writing to
                     the output-data/ directory.

Extraction already pointed each connection's `base_path` at the absolute
location of its data folder and created output-data/, so the connections are
ready to register as they stand.


What to ask the AI agent
-------------

  - Publish    A publisher reading persons.csv through the input connection
               and landing it as a `persons` table in the `input` collection.
  - Transform  A transformer reading `input/persons` and producing a `people`
               table in the `dataprod` collection, with a `full_name` column
               added and `age` dropped.
  - Subscribe  A subscriber writing `dataprod/people` as JSON Lines to
               people.jsonl through the output connection, in the `output`
               collection.

Before you begin
----------------

IMPORTANT: This example only works with a local Tabsdata installed with quickstart.

A Tabsdata server must be running on the same host as the CLI, and you must be
logged in:

  tdk login

Vibing It
---------

Using a Code agent, Claude or CodeX, with Tabsdata MCP installed ask the agent to
build the above steps for you.