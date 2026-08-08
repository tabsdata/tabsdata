#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import constants as const
from tabsdatak.conn.common import _config


# All ``_engine`` cfg keys must live under the shared ``tabsdata.``
# prefix so ``_config.remove_tabsdata_cfgs`` strips them before a
# connector forwards a cfg dict to the 3rd-party client library.


def test_sql_key_prefix_extends_tabsdata_prefix():
    assert const.SQL_KEY_PREFIX.startswith(_config.TABSDATA_CFG_PREFIX)


def test_read_keys_carry_the_sql_prefix():
    assert const.READ_ENGINE_KEY.startswith(_config.TABSDATA_CFG_PREFIX)
    assert const.AVAIL_READ_ENGINES_KEY.startswith(_config.TABSDATA_CFG_PREFIX)
    assert const.READ_CHUNK_SIZE_KEY.startswith(_config.TABSDATA_CFG_PREFIX)


def test_write_keys_carry_the_sql_prefix():
    assert const.WRITE_ENGINE_KEY.startswith(_config.TABSDATA_CFG_PREFIX)
    assert const.AVAIL_WRITE_ENGINES_KEY.startswith(_config.TABSDATA_CFG_PREFIX)
    assert const.WRITE_CHUNK_SIZE_KEY.startswith(_config.TABSDATA_CFG_PREFIX)


def test_cfg_keys_are_snake_case_under_a_single_sql_segment():
    # The keys are the user-facing src_cfg / dest_cfg spelling:
    # snake_case throughout, one ``sql.`` segment.
    assert const.READ_ENGINE_KEY == "tabsdata.sql.read_engine"
    assert const.WRITE_ENGINE_KEY == "tabsdata.sql.write_engine"
    assert const.AVAIL_READ_ENGINES_KEY == "tabsdata.sql.engine.avail_read_engines"
    assert const.AVAIL_WRITE_ENGINES_KEY == "tabsdata.sql.engine.avail_write_engines"
    assert const.READ_CHUNK_SIZE_KEY == "tabsdata.sql.chunk_size"
    assert const.WRITE_CHUNK_SIZE_KEY == "tabsdata.sql.chunk_size"


def test_engine_names_are_distinct():
    # Each engine name is a wire-format identifier; collisions
    # would route a read-engine pick to a write-engine class (or
    # vice versa). Same string for read vs write would also blur
    # which factory consumes which.
    names = {
        const.POLARS_SQL_ALCHEMY_READ_ENGINE,
        const.PANDAS_SQL_ALCHEMY_READ_ENGINE,
        const.POLARS_SQL_ALCHEMY_WRITE_ENGINE,
    }
    assert len(names) == 3


def test_chunk_size_defaults_are_positive_ints():
    assert isinstance(const.READ_CHUNK_SIZE_DEFAULT, int)
    assert const.READ_CHUNK_SIZE_DEFAULT > 0
    assert isinstance(const.WRITE_CHUNK_SIZE_DEFAULT, int)
    assert const.WRITE_CHUNK_SIZE_DEFAULT > 0


def test_per_loader_chunk_size_defaults_match_the_global_default():
    # The per-loader chunk-size defaults are explicit aliases of
    # the global default today; if that ever diverges (e.g. polars
    # wants a smaller batch), this test surfaces it.
    assert const.POLARS_SQL_ALCHEMY_READ_CHUNK_SIZE_DEFAULT == const.READ_CHUNK_SIZE_DEFAULT
    assert const.PANDAS_SQL_ALCHEMY_READ_CHUNK_SIZE_DEFAULT == const.READ_CHUNK_SIZE_DEFAULT
    assert const.POLARS_SQL_ALCHEMY_WRITE_CHUNK_SIZE_DEFAULT == const.WRITE_CHUNK_SIZE_DEFAULT
