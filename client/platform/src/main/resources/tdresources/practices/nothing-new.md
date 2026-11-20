<!--
Copyright 2026 Tabsdata Inc.
-->
---
title: Writing no rows versus keeping the data
summary: >-
  Return an empty frame when a run has no rows to write, and None only when
  the table's current data is still right: None keeps the previous rows, and
  an @NEW reader receives them again.
applies_to: [publisher, stream_publisher, transformer, subscriber, TableFrame,
  TableFrameSpec, TableFramesSpec, TableFrame.empty, TableFrame.clear, concat]
examples:
  - pub-postgres-incremental
  - pub-s3-parquet-glob
  - pub-localfile-csv
  - tfr-versions-head-new
  - tfr-multi-output-split
  - tfr-keep-last-good
  - tfr-accumulate-batches
  - tfr-incremental-batch
  - sub-bigquery
  - sub-localfile-parquet-append
---
Every run of a function writes a new version of each output table. What that
version holds depends on what the function returns for it:

- A frame, empty or not: the version holds those rows and those columns. An
  empty frame is a version with no rows; give it the table's columns, since
  `TableFrame.empty()` without a schema has none.
- `None`: the version holds no data of its own and stands for the previous
  one. A reader at `@HEAD` sees the previous rows, and a reader at `@NEW`
  receives them again, once for every version since its last run.

A version has data when the function returned a frame for it, even one with
no rows or no columns; only `None` gives a version without data, and `@HEAD`
is the latest version with data.

So a table that holds each run's batch, read downstream with `@NEW`, must
get an empty frame when a run has nothing new, or its last batch is
processed again. `None` is for a table whose current rows are still right.
A version without data still starts the functions that read its table.

## Publishers

A source that finds no new data hands an empty frame with the columns of the
last run that had data: run the usual logic on it and publish it. The version
has no rows, so a reader at `@NEW` gets no batch. A query that returns no
rows, a stream batch without data for a slot and a file path that takes no
new file all do this. A path takes only the files modified since the last
run when the source sets `initial_last_modified`; without it, every run
takes every file the path matches.

A file source (`LocalFileSrc`, `S3Src`, `GCSSrc`, `AzureBlobSrc`) also takes
`incremental`, which says how each run's data relates to what was read
before:

- `incremental=True`, the default: each run adds to, or modifies, what was
  read before. A path that takes no new file hands the empty frame above.
- `incremental=False`: each run replaces what was read before. A path that
  takes no new file hands nothing, meaning nothing changed; `concat` of it
  is `None`, and returning `None` keeps the table's data, which is still
  valid. Set `initial_last_modified` too, so a file that did not change is
  not taken again; a file rewritten in place counts as changed. It fits a
  table read whole, at `@HEAD`; a reader at `@NEW` would get that data
  again, so a table read that way keeps the default.

Until a run has had data for an input, the source may have no columns to
give it, and the input is then `None` (`[]` for a list slot); a query whose
result types its columns (SQL, a Salesforce sObject) hands an empty frame of
them from the first run. Each input follows the rule on its own: in one run
one can bring data while another brings none.

## Transformers

- A table input is `None` while its table has no version with data yet;
  returning `None` then keeps the output without data too.
- An `@NEW` input is `[]` when no version came since the last run, which
  happens only when something else started the run: another input, a table
  named in `trigger_by`, its schedule, or a person. A function started that
  way also reads the table at `@HEAD` and, when `concat` of the `@NEW` input
  is `None`, runs on `head.clear()`: an empty frame with the table's
  columns, so a batch output gets a version with no rows and its columns.
  While `head` is `None` too, it returns `None`.
- An output recomputed from whole inputs gets its result every run, empty or
  not; `None` would keep rows its inputs no longer hold.
- `None` fits an output whose current data is still right, such as the last
  good snapshot kept when a new one fails validation.
- A table that holds every batch adds the new ones to itself, read at
  `@HEAD`; with no new version it returns `None`, since all it holds is
  still right. A subscriber can then replace its destination with it. Its
  `@NEW` input must come from a table that gets an empty frame when nothing
  is new, never `None`, or the last batch is added again.
- With an `@NEW` input, returning `None` because another input has no data
  yet drops the batch: the next run does not get it again. Fail the run
  instead, or keep the batch in an output of its own.

## Subscribers

The version a subscriber reads matches how it writes:

- A destination appended to reads `@NEW`, so each version is written once,
  even when several commit between two runs. With no new version the input
  is `[]`, and `concat([])` is `None`. Its first run writes every version
  the table has.
- A destination replaced every run reads `@HEAD`.

`None` for a slot leaves its destination table as it is; `None` in every
slot writes nothing. An empty frame writes no rows, and with
`if_table_exists="replace"` it empties the destination table. A file
destination writes a file for an empty frame too, one without rows.
