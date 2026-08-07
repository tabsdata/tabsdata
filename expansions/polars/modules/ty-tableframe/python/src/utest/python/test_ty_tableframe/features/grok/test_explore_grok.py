#
# Copyright 2025 Tabsdata Inc.
#

import logging

import polars as pl

from tabsdatak.tableframe import TableFrame, datatypes as td_datatypes
from tabsdatak.tableframe.functions import col
from tabsdatak.tableframe.grok import ColumnDefinition

# noinspection PyProtectedMember
from tabsdata.expansions.tableframe.features.grok.api._handler import GrokParser
from tabsdata.expansions.tableframe.features.grok.engine import (
    grok_fields,
    grok_patterns,
)

logger = logging.getLogger(__name__)


def test_grok_patterns():
    patterns = grok_patterns()
    assert patterns

    import json

    _ = json.dumps(patterns, indent=4)


def test_grok():
    pl.Config.set_tbl_cols(1024)
    pl.Config.set_tbl_rows(1024)
    pl.Config.set_fmt_table_cell_list_len(1024)
    pl.Config.set_tbl_width_chars(4096)
    pl.Config.set_fmt_str_lengths(4096)

    pattern = (
        "%{WORD:word}-%{INT:year}-"
        "%{WORD:word}-%{INT:year}-"
        "%{WORD:word}-%{INT:year}-"
        "%{WORD:word}-%{INT:year}"
    )

    schema = {
        "word": ColumnDefinition("pare", td_datatypes.String),
        "word[1]": ColumnDefinition("mare", td_datatypes.String),
        "word[2]": ColumnDefinition("hereu", td_datatypes.String),
        "word[3]": ColumnDefinition("cabaler", td_datatypes.String),
        "year": ColumnDefinition("any pare", td_datatypes.Int64),
        "year[1]": ColumnDefinition("any mare", td_datatypes.Int64),
        "year[2]": ColumnDefinition("any hereu", td_datatypes.Int64),
        "year[3]": ColumnDefinition("any cabaler", td_datatypes.Int64),
    }

    _ = grok_fields(pattern)

    tf0 = TableFrame.from_dict(
        {"family": ["dimas-1968-gemma-1976-kai-2009-guiu-2012"]}
    )
    assert tf0
    _ = tf0._lf.collect()

    tf1 = tf0.grok("family", pattern, schema)
    assert tf1
    _tf1_len = tf1.schema.len()
    _tf1_schema = tf1.schema.names()
    _ = tf1._lf.collect()

    tf = tf1.select("pare", "any pare")
    lf = tf._lf
    _ = lf.collect()

    tf = tf1.select("mare", "any mare")
    lf = tf._lf
    _ = lf.collect()

    tf = tf1.select("hereu", "any hereu")
    lf = tf._lf
    _ = lf.collect()

    tf = tf1.select("cabaler", "any cabaler")
    lf = tf._lf
    _ = lf.collect()


def test_grok_experimental():
    pl.Config.set_tbl_cols(1024)
    pl.Config.set_tbl_rows(1024)
    pl.Config.set_fmt_table_cell_list_len(1024)
    pl.Config.set_tbl_width_chars(4096)
    pl.Config.set_fmt_str_lengths(4096)

    pattern = (
        "%{WORD:word}-%{INT:year}-"
        "%{WORD:word}-%{INT:year}-"
        "%{WORD:word}-%{INT:year}-"
        "%{WORD:word}-%{INT:year}"
    )

    schema = {
        "word": ColumnDefinition("pare", td_datatypes.String),
        "word[1]": ColumnDefinition("mare", td_datatypes.String),
        "word[2]": ColumnDefinition("hereu", td_datatypes.String),
        "word[3]": ColumnDefinition("cabaler", td_datatypes.String),
        "year": ColumnDefinition("any pare", td_datatypes.Int64),
        "year[1]": ColumnDefinition("any mare", td_datatypes.Int64),
        "year[2]": ColumnDefinition("any hereu", td_datatypes.Int64),
        "year[3]": ColumnDefinition("any cabaler", td_datatypes.Int64),
    }

    _ = grok_fields(pattern)

    grok = GrokParser(pattern, schema)

    tf0 = TableFrame.from_dict(
        {"family": ["dimas-1968-gemma-1976-kai-2009-guiu-2012"]}
    )
    assert tf0
    _ = tf0._lf.collect()

    tf1 = tf0.with_columns(grok.rust(col("family")._inner).alias("grok"))
    assert tf1
    _ = tf1._lf.collect()

    tf2 = tf0.grok("family", pattern, schema)
    assert tf2
    _ = tf2._lf.collect()
