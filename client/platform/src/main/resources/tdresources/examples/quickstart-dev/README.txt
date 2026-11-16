# Copyright 2026 Tabsdata Inc.

Tabsdata Quickstart (developer)
===============================

A small end-to-end dataflow over a CSV of people: publish it as a table,
reshape it, and write the result out as JSON Lines. Three functions -- a
publisher, a transformer, and a subscriber -- are wired across three
collections. In Tabsdata Enterprise, triggering the publisher runs the whole
flow; the open-source edition runs only the function you trigger, so there you
trigger each of the three in turn.

You'll build a small end-to-end dataflow across your three collections and run
it:

  - Publish    qs_pub reads CSV data and lands it as a table in the `input`
               collection.
  - Transform  qs_tfr reads that table and produces a new, reshaped table (a
               `full_name` column is added and `age` is dropped) in the
               `dataprod` collection.
  - Subscribe  qs_sub writes the transformed table as JSON Lines through the
               `output` collection.


Before you begin
----------------

IMPORTANT: This example only works with a local Tabsdata installed with quickstart.

A Tabsdata server must be running on the same host as the CLI, and you must be
logged in:

  tdk login


Run it
------

Create the project, add the three collections, register one function in each,
then trigger the publisher. Run these commands from the directory where this
README file is.

  # Create the project and make it the default for the command line

  tdk project create --name quickstart_dev
  tdk set --proj quickstart_dev


  # Collections: a source (input), a transformation (dataprod), a destination (output)

  tdk collection create --name input --group sources --conn-file conn-input.yaml
  tdk collection create --name dataprod --group default
  tdk collection create --name output --group destinations --conn-file conn-output.yaml


  # Register one function per collection

  tdk fn register --coll input --path input.py::qs_pub
  tdk fn register --coll dataprod --path transform.py::qs_tfr
  tdk fn register --coll output --path output.py::qs_sub


  # Run it

  tdk fn trigger --coll input --name qs_pub


  # Open-source edition only: run the transformer and the subscriber yourself
  # (Tabsdata Enterprise runs them after the publisher automatically)

  tdk fn trigger --coll dataprod --name qs_tfr
  tdk fn trigger --coll output --name qs_sub

When the flow finishes, the result is written to the output-data/ directory.
