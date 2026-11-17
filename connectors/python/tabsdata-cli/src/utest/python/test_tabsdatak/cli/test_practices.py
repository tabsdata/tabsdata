#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.cli._docs._catalog import build_catalog
from tabsdatak.cli._docs._practices import load_practices, parse_practice
from tabsdatak.cli._docs._render import markdown
from tabsdatak.cli._examples import examples_root, load_examples

PRACTICES = load_practices()


@pytest.fixture(scope="module")
def catalog():
    return build_catalog()


def test_practices_are_bundled():
    slugs = {practice.slug for practice in PRACTICES}
    assert "function-granularity" in slugs


@pytest.mark.parametrize("practice", PRACTICES, ids=lambda p: p.slug)
def test_every_practice_names_documented_symbols(catalog, practice):
    assert practice.applies_to
    for symbol in practice.applies_to:
        catalog.resolve(symbol)


@pytest.mark.parametrize("practice", PRACTICES, ids=lambda p: p.slug)
def test_every_practice_names_bundled_examples(practice):
    names = {example.name for example in load_examples(examples_root())}
    assert set(practice.examples) <= names


def test_a_decorator_page_links_its_practices():
    transformer = build_catalog(["api"]).resolve("transformer")
    assert "tdk docs show --name function-granularity" in transformer.see_also


def test_a_practice_page_shows_its_title_body_and_links(catalog):
    page = markdown(catalog.resolve("function-granularity"))
    assert page.startswith("# function-granularity  (practices · practice)\n")
    assert "**How much one function should do**" in page
    assert "- `tdk docs show --name transformer`" in page
    assert "- `tdk examples show --name tfr-join-two-tables`" in page


def test_a_page_without_front_matter_is_refused(tmp_path):
    page = tmp_path / "bad.md"
    page.write_text("# No front matter\n", encoding="utf-8")
    with pytest.raises(ValueError, match="no front matter"):
        parse_practice(page)


def test_a_page_missing_a_key_is_refused(tmp_path):
    page = tmp_path / "bad.md"
    page.write_text("---\ntitle: t\nsummary: s\n---\nbody\n", encoding="utf-8")
    with pytest.raises(ValueError, match="applies_to, examples"):
        parse_practice(page)
