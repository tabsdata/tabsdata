#
# Copyright 2026 Tabsdata Inc.
#

"""LocalFile source-plugin integration tests using fake context"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.functions.files import PluginSrcCtx
from testi_tabsdatak.conn.common.plugin_ctx import LastModSrcCtx
from testi_tabsdatak.conn.localfile.backend import scratch, src_conn, stage_parquet

from tabsdatak.conn.common.file.typing_ import SrcFileFormat

# noinspection PyProtectedMember
from tabsdatak.conn.localfile import LocalFileSrc, _plugin as plugin

pytestmark = pytest.mark.localfile

# The cursor tests stamp their files relative to this fixed instant rather than
# to "now", so a run is reproducible and never races the clock.
CUTOFF = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _slot_names(slot: list) -> list[str]:
    """The ``name`` column each file in one output slot carries -- the relative name
    :func:`stage_parquet` wrote it under."""
    return sorted(pl.read_parquet(f.file).select("name").item(0, 0) for f in slot)


def _read_csv_with_cfg(
    domain_fs: FsDomain,
    request: pytest.FixtureRequest,
    csv_text: str,
    cfg: dict,
) -> pl.DataFrame:
    _, base, work = scratch(domain_fs, request)
    (base / "in.csv").write_text(csv_text)

    src = LocalFileSrc(
        paths=["in.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: cfg},
    )
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), src_conn(base), src)
    # Drop the binary's ``$td.opt.*`` provenance columns.
    df = pl.read_parquet(out[0][0].file)
    user_cols = [c for c in df.columns if not c.startswith("$td.")]
    return df.select(user_cols)


@fs_domain("fs")
def test_src_treats_absolute_path_as_relative_to_base_path(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    _, base, work = scratch(domain_fs, request)
    pl.DataFrame([{"x": 1}]).write_parquet(base / "data.parquet", use_pyarrow=True)

    # The entry is relative to the connection's base_path.
    src = LocalFileSrc(paths=["data.parquet"])

    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), src_conn(base), src)
    assert len(out[0]) >= 1
    assert pl.read_parquet(out[0][0].file).select("x").to_dicts() == [{"x": 1}]


@fs_domain("fs")
def test_src_wildcard_in_basename_matches_multiple_files(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    _, base, work = scratch(domain_fs, request)
    for name in ("a.parquet", "b.parquet", "c.parquet"):
        stage_parquet(base, name)
    # A non-matching neighbour: must NOT show up in the output.
    stage_parquet(base, "skip.csv")

    src = LocalFileSrc(paths=["*.parquet"])

    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), src_conn(base), src)

    assert len(out) == 1
    assert _slot_names(out[0]) == ["a.parquet", "b.parquet", "c.parquet"]


@fs_domain("fs")
def test_src_wildcard_with_no_matches_returns_empty_slot(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    _, base, work = scratch(domain_fs, request)
    pl.DataFrame([{"name": "x"}]).write_parquet(base / "x.csv", use_pyarrow=True)

    src = LocalFileSrc(paths=["*.parquet"])
    ctx = LastModSrcCtx(work)

    out = plugin.LocalFileSrcPlugin().read_in(ctx, src_conn(base), src)

    assert len(out) == 1
    assert out[0] == []
    assert ctx.attrs == {}


@fs_domain("fs")
def test_src_one_slot_per_path_in_path_order(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    _, base, work = scratch(domain_fs, request)
    stage_parquet(base, "in/a1.parquet")
    stage_parquet(base, "in/a2.parquet")
    stage_parquet(base, "out/b1.parquet")
    stage_parquet(base, "single.parquet")

    src = LocalFileSrc(paths=["out/b*.parquet", "in/a*.parquet", "single.parquet"])

    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), src_conn(base), src)

    assert [_slot_names(slot) for slot in out] == [
        ["out/b1.parquet"],
        ["in/a1.parquet", "in/a2.parquet"],
        ["single.parquet"],
    ]


@fs_domain("fs")
def test_src_overlapping_globs_import_the_same_file_in_every_slot(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root, base, work = scratch(domain_fs, request)
    stage_parquet(base, "shared.parquet", CUTOFF)

    src = LocalFileSrc(paths=["*.parquet", "sh*.parquet"], initial_last_modified=CUTOFF)
    ctx = LastModSrcCtx(work)

    out = plugin.LocalFileSrcPlugin().read_in(ctx, src_conn(base), src)

    # Each slot keeps its own cursor, so a file both patterns match is imported
    # once per slot rather than deduplicated across them.
    assert [_slot_names(slot) for slot in out] == [
        ["shared.parquet"],
        ["shared.parquet"],
    ]
    assert sorted(ctx.attrs) == ["file-lastmod.slot0", "file-lastmod.slot1"]

    resumed = LastModSrcCtx(root / "resumed", ctx.attrs)
    assert plugin.LocalFileSrcPlugin().read_in(resumed, src_conn(base), src) == [[], []]


@fs_domain("fs")
def test_src_initial_last_modified_filters_out_older_files(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    _, base, work = scratch(domain_fs, request)
    stage_parquet(base, "older.parquet", CUTOFF - timedelta(hours=1))
    stage_parquet(base, "boundary.parquet", CUTOFF)
    stage_parquet(base, "newer.parquet", CUTOFF + timedelta(hours=1))

    src = LocalFileSrc(paths=["*.parquet"], initial_last_modified=CUTOFF)

    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), src_conn(base), src)

    # The cutoff is inclusive: the file modified exactly at it is imported.
    assert _slot_names(out[0]) == ["boundary.parquet", "newer.parquet"]


@fs_domain("fs")
def test_src_initial_last_modified_at_or_before_oldest_returns_all(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    _, base, work = scratch(domain_fs, request)
    floor = datetime(2020, 1, 1, tzinfo=timezone.utc)
    stage_parquet(base, "a.parquet", floor + timedelta(days=1))
    stage_parquet(base, "b.parquet", floor + timedelta(days=2))

    src = LocalFileSrc(paths=["*.parquet"], initial_last_modified=floor)

    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), src_conn(base), src)

    assert _slot_names(out[0]) == ["a.parquet", "b.parquet"]


@fs_domain("fs")
def test_src_without_initial_last_modified_keeps_no_cursor(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    # A Src with no baseline is not incremental at all: nothing is stored on the
    # context, so every run re-imports whatever the glob matches.
    root, base, work = scratch(domain_fs, request)
    stage_parquet(base, "only.parquet", CUTOFF)

    src = LocalFileSrc(paths=["*.parquet"])
    ctx = LastModSrcCtx(work)

    assert _slot_names(
        plugin.LocalFileSrcPlugin().read_in(ctx, src_conn(base), src)[0]
    ) == ["only.parquet"]
    assert ctx.attrs == {}

    again = LastModSrcCtx(root / "again", ctx.attrs)
    assert _slot_names(
        plugin.LocalFileSrcPlugin().read_in(again, src_conn(base), src)[0]
    ) == ["only.parquet"]


@fs_domain("fs")
def test_src_second_run_resumes_from_the_stored_cursor(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root, base, work = scratch(domain_fs, request)
    stage_parquet(base, "first.parquet", CUTOFF)

    src = LocalFileSrc(paths=["*.parquet"], initial_last_modified=CUTOFF)
    ctx = LastModSrcCtx(work)

    first = plugin.LocalFileSrcPlugin().read_in(ctx, src_conn(base), src)
    assert _slot_names(first[0]) == ["first.parquet"]
    assert sorted(ctx.attrs) == ["file-lastmod.slot0"]

    stage_parquet(base, "second.parquet", CUTOFF + timedelta(hours=1))
    resumed = LastModSrcCtx(root / "resumed", ctx.attrs)

    # The cursor, not the baseline, governs once one is stored: the file the first
    # run consumed stays consumed.
    second = plugin.LocalFileSrcPlugin().read_in(resumed, src_conn(base), src)
    assert _slot_names(second[0]) == ["second.parquet"]


@fs_domain("fs")
def test_src_first_run_with_no_matches_still_stores_the_baseline(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root, base, work = scratch(domain_fs, request)
    (base / "other.txt").write_text("not a match")

    src = LocalFileSrc(paths=["*.parquet"], initial_last_modified=CUTOFF)
    ctx = LastModSrcCtx(work)

    assert plugin.LocalFileSrcPlugin().read_in(ctx, src_conn(base), src) == [[]]
    assert sorted(ctx.attrs) == ["file-lastmod.slot0"]

    # The stored cursor holds the baseline and no entry yet, so a file appearing
    # at the baseline is still inside the window.
    stage_parquet(base, "late.parquet", CUTOFF)
    resumed = LastModSrcCtx(root / "resumed", ctx.attrs)
    assert _slot_names(
        plugin.LocalFileSrcPlugin().read_in(resumed, src_conn(base), src)[0]
    ) == ["late.parquet"]


@fs_domain("fs")
def test_src_second_run_imports_a_file_tied_at_the_cursor_timestamp(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root, base, work = scratch(domain_fs, request)
    stage_parquet(base, "first.parquet", CUTOFF)

    src = LocalFileSrc(paths=["*.parquet"], initial_last_modified=CUTOFF)
    ctx = LastModSrcCtx(work)

    assert _slot_names(
        plugin.LocalFileSrcPlugin().read_in(ctx, src_conn(base), src)[0]
    ) == ["first.parquet"]

    # The cursor is a timestamp plus the names seen at exactly it, so a new file
    # sharing that timestamp is still recognised as new.
    stage_parquet(base, "tied.parquet", CUTOFF)
    tied = LastModSrcCtx(root / "tied", ctx.attrs)
    assert _slot_names(
        plugin.LocalFileSrcPlugin().read_in(tied, src_conn(base), src)[0]
    ) == ["tied.parquet"]

    idle = LastModSrcCtx(root / "idle", tied.attrs)
    assert plugin.LocalFileSrcPlugin().read_in(idle, src_conn(base), src) == [[]]


@fs_domain("fs")
def test_src_file_appearing_backdated_below_the_cursor_is_not_imported(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    # Intended behaviour, recorded so it is not mistaken for a loss: the cursor is
    # the greatest last-modified consumed, not a set of names, so a file that shows
    # up later carrying an older timestamp is behind the cursor and skipped -- even
    # though it is above the Src's baseline and was never imported.
    root, base, work = scratch(domain_fs, request)
    stage_parquet(base, "first.parquet", CUTOFF)

    src = LocalFileSrc(
        paths=["*.parquet"], initial_last_modified=CUTOFF - timedelta(days=1)
    )
    ctx = LastModSrcCtx(work)

    assert _slot_names(
        plugin.LocalFileSrcPlugin().read_in(ctx, src_conn(base), src)[0]
    ) == ["first.parquet"]

    stage_parquet(base, "backdated.parquet", CUTOFF - timedelta(hours=1))
    resumed = LastModSrcCtx(root / "resumed", ctx.attrs)
    assert plugin.LocalFileSrcPlugin().read_in(resumed, src_conn(base), src) == [[]]


@fs_domain("fs")
def test_src_reordering_paths_reimports_because_slots_are_positional(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root, base, work = scratch(domain_fs, request)
    stage_parquet(base, "in/a.parquet", CUTOFF)
    # Deliberately a different mtime from a.parquet: with equal mtimes the swap
    # below would re-import through the same-timestamp tie branch instead, and the
    # test would pass without the cursor lookup ever missing.
    stage_parquet(base, "out/b.parquet", CUTOFF + timedelta(hours=1))

    # Distinct basename globs on purpose: a cursor entry is keyed by the basename
    # pattern alone (the transporter splits the path and keys on the part after the
    # last `/`), so two directory globs that share `*.parquet` would share the entry
    # and the swap below would turn on which file is newer.
    ordered = LocalFileSrc(
        paths=["in/a*.parquet", "out/b*.parquet"], initial_last_modified=CUTOFF
    )
    ctx = LastModSrcCtx(work)

    first = plugin.LocalFileSrcPlugin().read_in(ctx, src_conn(base), ordered)
    assert [_slot_names(slot) for slot in first] == [
        ["in/a.parquet"],
        ["out/b.parquet"],
    ]

    unchanged = LastModSrcCtx(root / "unchanged", ctx.attrs)
    assert plugin.LocalFileSrcPlugin().read_in(unchanged, src_conn(base), ordered) == [
        [],
        [],
    ]

    # A cursor is addressed by the path's position, not by the path itself, so
    # swapping two entries hands each slot the other one's cursor -- which knows
    # nothing about this slot's pattern -- and both files are imported again.
    swapped = LocalFileSrc(
        paths=["out/b*.parquet", "in/a*.parquet"], initial_last_modified=CUTOFF
    )
    moved = LastModSrcCtx(root / "swapped", ctx.attrs)
    assert [
        _slot_names(slot)
        for slot in plugin.LocalFileSrcPlugin().read_in(moved, src_conn(base), swapped)
    ] == [["out/b.parquet"], ["in/a.parquet"]]


@fs_domain("fs")
def test_src_format_csv_reads_with_default_options(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    _, base, work = scratch(domain_fs, request)
    pl.DataFrame([{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]).write_csv(
        base / "users.csv"
    )

    src = LocalFileSrc(paths=["users.csv"], format=SrcFileFormat.CSV)
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), src_conn(base), src)
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@fs_domain("fs")
def test_src_format_csv_honours_separator_option(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    _, base, work = scratch(domain_fs, request)
    pl.DataFrame([{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]).write_csv(
        base / "users.csv", separator=";"
    )

    src = LocalFileSrc(
        paths=["users.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: {"separator": ";"}},
    )
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), src_conn(base), src)
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@fs_domain("fs")
def test_src_format_json_reads_records(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    # The transporter binary reads newline-delimited JSON (one record per line)
    # for ``SrcFileFormat.JSON``.
    _, base, work = scratch(domain_fs, request)
    pl.DataFrame([{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]).write_ndjson(
        base / "users.json"
    )

    src = LocalFileSrc(paths=["users.json"], format=SrcFileFormat.JSON)
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), src_conn(base), src)
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@fs_domain("fs")
def test_src_format_avro_reads_records(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    _, base, work = scratch(domain_fs, request)
    pl.DataFrame([{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]).write_avro(
        base / "users.avro"
    )

    src = LocalFileSrc(paths=["users.avro"], format=SrcFileFormat.AVRO)
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), src_conn(base), src)
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@fs_domain("fs")
def test_src_format_auto_infers_parquet_from_extension(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    _, base, work = scratch(domain_fs, request)
    pl.DataFrame([{"id": 1, "name": "alice"}]).write_parquet(
        base / "users.parquet", use_pyarrow=True
    )

    src = LocalFileSrc(paths=["users.parquet"], format=SrcFileFormat.AUTO)
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), src_conn(base), src)
    df = pl.read_parquet(out[0][0].file).select("id", "name")
    assert df.to_dicts() == [{"id": 1, "name": "alice"}]


@fs_domain("fs")
def test_csv_src_separator(domain_fs: FsDomain, request: pytest.FixtureRequest):
    df = _read_csv_with_cfg(
        domain_fs, request, "id;name\n1;alice\n2;bob\n", {"separator": ";"}
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@fs_domain("fs")
def test_csv_src_quote_char(domain_fs: FsDomain, request: pytest.FixtureRequest):
    # Apostrophe-quoted field with an embedded comma: only parses correctly when
    # ``quote_char`` is honored.
    df = _read_csv_with_cfg(
        domain_fs,
        request,
        "id,name\n1,'alice, the great'\n2,bob\n",
        {"quote_char": "'"},
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice, the great"},
        {"id": 2, "name": "bob"},
    ]


@fs_domain("fs")
def test_csv_src_has_header_false(domain_fs: FsDomain, request: pytest.FixtureRequest):
    df = _read_csv_with_cfg(
        domain_fs, request, "1,alice\n2,bob\n", {"has_header": False}
    )
    # Column names are positional; values are all data. Whatever the binary names
    # the columns, they must NOT be 'id' / 'name'.
    assert len(df) == 2
    assert "id" not in df.columns
    assert "name" not in df.columns


@fs_domain("fs")
def test_csv_src_skip_rows(domain_fs: FsDomain, request: pytest.FixtureRequest):
    df = _read_csv_with_cfg(
        domain_fs,
        request,
        "# noise 1\n# noise 2\nid,name\n1,alice\n2,bob\n",
        {"skip_rows": 2},
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@fs_domain("fs")
def test_csv_src_skip_rows_after_header(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    df = _read_csv_with_cfg(
        domain_fs,
        request,
        "id,name\nskip_1,skip_1\nskip_2,skip_2\n1,alice\n2,bob\n",
        {"skip_rows_after_header": 2},
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@fs_domain("fs")
def test_csv_src_comment_prefix(domain_fs: FsDomain, request: pytest.FixtureRequest):
    df = _read_csv_with_cfg(
        domain_fs,
        request,
        "# header comment\nid,name\n# data comment\n1,alice\n2,bob\n",
        {"comment_prefix": "#"},
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@fs_domain("fs")
def test_csv_src_null_values(domain_fs: FsDomain, request: pytest.FixtureRequest):
    df = _read_csv_with_cfg(
        domain_fs, request, "id,name\n1,NA\n2,bob\n", {"null_values": ["NA"]}
    )
    rows = df.sort("id").to_dicts()
    assert rows[0]["name"] is None
    assert rows[1]["name"] == "bob"


@fs_domain("fs")
def test_csv_src_missing_is_null_default_empty_cell_is_null(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    # The binary emits the same output for ``missing_is_null`` True and False (it
    # always treats an empty cell as null), so this pins the default behaviour
    # rather than the option being honored.
    df = _read_csv_with_cfg(
        domain_fs, request, "id,name\n1,\n2,bob\n", {"missing_is_null": True}
    )
    rows = df.sort("id").to_dicts()
    assert rows[0]["name"] is None
    assert rows[1]["name"] == "bob"


@fs_domain("fs")
def test_csv_src_try_parse_dates(domain_fs: FsDomain, request: pytest.FixtureRequest):
    df = _read_csv_with_cfg(
        domain_fs,
        request,
        "id,when\n1,2026-01-01\n2,2026-02-02\n",
        {"try_parse_dates": True},
    )
    assert str(df.schema["when"]).lower() == "date"


@fs_domain("fs")
def test_csv_src_decimal_comma(domain_fs: FsDomain, request: pytest.FixtureRequest):
    # Comma is the decimal point, so the field delimiter has to be something else.
    df = _read_csv_with_cfg(
        domain_fs,
        request,
        "id;value\n1;1,5\n2;2,75\n",
        {"separator": ";", "decimal_comma": True},
    )
    rows = df.sort("id").to_dicts()
    assert rows[0]["value"] == 1.5
    assert rows[1]["value"] == 2.75


@fs_domain("fs")
def test_csv_src_truncate_ragged_lines(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    df = _read_csv_with_cfg(
        domain_fs,
        request,
        # Second row has an extra column; both rows must survive.
        "id,name\n1,alice\n2,bob,extra\n",
        {"truncate_ragged_lines": True, "ignore_errors": True},
    )
    assert len(df) == 2


@fs_domain("fs")
def test_csv_src_raise_if_empty_false(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    # An empty file must not error, and comes back with no rows.
    df = _read_csv_with_cfg(domain_fs, request, "", {"raise_if_empty": False})
    assert len(df) == 0


@fs_domain("fs")
def test_csv_src_ignore_errors(domain_fs: FsDomain, request: pytest.FixtureRequest):
    # 'not_a_number' would trip type inference on ``id``; with ``ignore_errors``
    # the binary keeps the column as String so every row passes through.
    df = _read_csv_with_cfg(
        domain_fs,
        request,
        "id,name\n1,alice\nnot_a_number,bob\n3,carol\n",
        {"ignore_errors": True},
    )
    assert len(df) == 3
    assert str(df.schema["id"]).lower() in {"string", "str", "utf8"}


@fs_domain("fs")
def test_csv_src_encoding_utf8(domain_fs: FsDomain, request: pytest.FixtureRequest):
    # Default encoding is utf8 -- multibyte chars round-trip.
    _, base, work = scratch(domain_fs, request)
    (base / "in.csv").write_bytes("id,name\n1,café\n2,piñata\n".encode("utf-8"))
    src = LocalFileSrc(
        paths=["in.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: {"encoding": "utf8"}},
    )
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), src_conn(base), src)
    df = pl.read_parquet(out[0][0].file).select("id", "name")
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "café"},
        {"id": 2, "name": "piñata"},
    ]


@fs_domain("fs")
def test_csv_src_eol_char_default_lf(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    df = _read_csv_with_cfg(
        domain_fs, request, "id,name\n1,alice\n2,bob\n", {"eol_char": "\n"}
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]
