#
# Copyright 2026 Tabsdata Inc.
#

import polars as pl
import pytest

import tabsdatak.dataquality as dq
from tabsdatak.common._actions.dataquality.engine import (
    CLASSIFIER_DICTIONARY,
    ByteSentinel,
    DataQualityEngine,
)
from tabsdatak.common._actions.dataquality.processor import DataQualityProcessor
from tabsdatak.dataquality._error import (
    DataQualityErrorCode,
    DataQualityProcessingError,
    ParameterValidationError,
)
from tabsdatak.tableframe._core.hooks import tableframe_of

pytestmark = pytest.mark.dq


def orders():
    return tableframe_of(
        pl.LazyFrame({
            "order_id": [1, 2, 3, 4],
            "discount_pct": [10.0, 25.0, 60.0, 0.0],
            "amount": [100.0, -5.0, 50.0, 20.0],
            "region": ["EU", "US", "EU", "APAC"],
        })
    )


def typed():
    return tableframe_of(pl.LazyFrame({"n": [1.0, -2.0], "s": ["x", "y"]}))


def between(columns, max_val: float, **kwargs) -> dq.IsBetween:
    kwargs.setdefault("prefix", "")
    return dq.IsBetween(columns, min_val=0.0, max_val=max_val, **kwargs)


def action(classifiers, operators=None) -> dq.DataQuality:
    return dq.DataQuality(
        table="orders",
        classifiers=classifiers,
        operators=operators if operators is not None else [dq.Summary()],
    )


def engine_with(calls, frame=None) -> DataQualityEngine:
    engine = DataQualityEngine(frame if frame is not None else orders())
    for method, classifier in calls:
        engine = getattr(engine, method)(classifier)
    return engine


# --- Registration time: collisions decidable from the decorator literals ----


def test_one_classifier_repeating_a_source_column_is_rejected():
    with pytest.raises(ParameterValidationError) as exc:
        action([between(["discount_pct", "discount_pct"], 15.0)])
    assert exc.value.api_error is DataQualityErrorCode.DQC_202


def test_one_classifier_repeating_an_alias_is_rejected():
    with pytest.raises(ParameterValidationError) as exc:
        action([between([("amount", "v"), ("discount_pct", "v")], 15.0)])
    assert exc.value.api_error is DataQualityErrorCode.DQC_202


def test_classifiers_of_different_types_sharing_an_alias_are_rejected():
    with pytest.raises(ParameterValidationError) as exc:
        action([
            between([("amount", "v")], 15.0),
            dq.IsPositive([("region", "v")], prefix=""),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQC_202


def test_an_explicit_none_alias_still_infers_and_collides():
    with pytest.raises(ParameterValidationError) as exc:
        action([
            between([("discount_pct", None)], 15.0),
            between([("discount_pct", None)], 40.0),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQC_202


def test_a_collision_is_reported_across_three_classifiers():
    with pytest.raises(ParameterValidationError) as exc:
        action([
            between([("amount", "v")], 15.0),
            between([("amount", "v")], 40.0),
            between([("amount", "v")], 90.0),
        ])
    assert "3 classifiers" in str(exc.value)


def test_mixing_bare_and_aliased_entries_in_one_classifier_is_allowed():
    declared = action([between(["discount_pct", ("amount", "amount_ok")], 15.0)])
    assert len(declared.classifiers) == 1


@pytest.mark.parametrize(
    "name",
    ["$td.id", "$td.ver.version", "$td.row"],
)
def test_reserved_prefixes_are_rejected_at_registration(name: str):
    with pytest.raises(ParameterValidationError) as exc:
        action([between([("discount_pct", name)], 15.0, prefix="")])
    assert exc.value.api_error is DataQualityErrorCode.DQC_203


@pytest.mark.parametrize(
    "name",
    ["@td.origin.file.name", "@td.dq.discount", "@td.x"],
)
def test_the_connector_namespace_is_not_reserved_for_verdict_columns(name: str):
    declared = action([between([("discount_pct", name)], 15.0, prefix="")])
    assert len(declared.classifiers) == 1


@pytest.mark.parametrize("name", ["$td", "@td", "td.x", "@TD.x", "not_reserved"])
def test_names_that_only_resemble_a_reserved_prefix_are_allowed(name: str):
    declared = action([between([("discount_pct", name)], 15.0)])
    assert len(declared.classifiers) == 1


def test_a_verdict_column_named_after_its_own_source_is_rejected():
    with pytest.raises(ParameterValidationError) as exc:
        action([between([("discount_pct", "discount_pct")], 15.0, prefix="")])
    assert exc.value.api_error is DataQualityErrorCode.DQC_204


def test_a_wildcard_and_a_literal_of_one_type_are_allowed_at_registration():
    declared = action([between(None, 15.0), between(["discount_pct"], 40.0)])
    assert len(declared.classifiers) == 2


# --- Execution time: collisions that need the schema ------------------------


def test_two_wildcards_of_one_type_collide_on_every_column():
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([
            ("is_between", between(None, 15.0)),
            ("is_between", between(None, 40.0)),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQPE_17


def test_a_wildcard_colliding_with_a_literal_of_one_type_is_rejected():
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([
            ("is_between", between(None, 15.0)),
            ("is_between", between(["discount_pct"], 40.0)),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQPE_17


def test_an_alias_colliding_with_an_inferred_name_is_rejected():
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([
            ("is_between", between(["discount_pct"], 15.0)),
            (
                "is_not_between",
                dq.IsNotBetween(
                    [("amount", "discount_pct_is_between")],
                    min_val=0.0,
                    max_val=1.0,
                    prefix="",
                ),
            ),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQPE_17


def test_the_error_names_both_classifiers_and_both_source_columns():
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([
            ("is_between", between(["discount_pct"], 15.0)),
            (
                "is_not_between",
                dq.IsNotBetween(
                    [("amount", "discount_pct_is_between")],
                    min_val=0.0,
                    max_val=1.0,
                    prefix="",
                ),
            ),
        ])
    message = str(exc.value)
    assert "IsBetween" in message and "IsNotBetween" in message
    assert "'discount_pct'" in message and "'amount'" in message


@pytest.mark.parametrize("victim", ["order_id", "region", "amount"])
def test_a_verdict_column_overwriting_a_user_column_is_rejected(victim: str):
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([
            ("is_between", between([("discount_pct", victim)], 15.0, prefix=""))
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQPE_18
    assert "user" in str(exc.value)


def test_a_verdict_column_overwriting_a_system_column_is_rejected():
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([
            ("is_between", between([("discount_pct", "$td.id")], 15.0, prefix=""))
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQPE_19


def test_a_categorizer_colliding_with_itself_is_rejected():
    scale = dq.LinearScale(0.0, 100.0, bins=4)
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([
            ("categorizer", dq.ScaleCategorizer(["amount"], scale=scale, prefix="")),
            ("categorizer", dq.ScaleCategorizer(["amount"], scale=scale, prefix="")),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQPE_17


def test_a_categorizer_and_a_boolean_classifier_coexist_on_one_column():
    engine = engine_with([
        (
            "categorizer",
            dq.ScaleCategorizer(
                ["amount"], scale=dq.LinearScale(0.0, 100.0, bins=4), prefix=""
            ),
        ),
        ("is_positive", dq.IsPositive(["amount"], prefix="")),
    ])
    assert set(engine._cols) == {"amount_category", "amount_is_positive"}


def test_tags_do_not_mask_a_collision():
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([
            ("is_between", between(["discount_pct"], 15.0, tags=["strict"])),
            ("is_between", between(["discount_pct"], 40.0, tags=["lenient"])),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQPE_17


# --- Pruning must not turn into a false positive ----------------------------


def test_a_pruned_classifier_frees_its_alias_for_another_classifier():
    engine = engine_with(
        [
            ("is_positive", dq.IsPositive([("s", "v")], prefix="")),
            ("is_positive", dq.IsPositive([("n", "v")], prefix="")),
        ],
        frame=typed(),
    )
    assert set(engine._cols) == {"v"}
    assert engine._cols["v"].column == "n"


def test_a_pruned_classifier_frees_its_alias_whatever_the_order():
    engine = engine_with(
        [
            ("is_positive", dq.IsPositive([("n", "v")], prefix="")),
            ("is_positive", dq.IsPositive([("s", "v")], prefix="")),
        ],
        frame=typed(),
    )
    assert set(engine._cols) == {"v"}


def test_a_missing_column_frees_its_alias_for_another_classifier():
    engine = engine_with(
        [
            ("is_positive", dq.IsPositive([("absent", "v")], prefix="")),
            ("is_positive", dq.IsPositive([("n", "v")], prefix="")),
        ],
        frame=typed(),
    )
    assert set(engine._cols) == {"v"}


def test_an_alias_naming_a_column_absent_from_the_table_is_allowed():
    engine = engine_with(
        [("is_positive", dq.IsPositive([("n", "absent_elsewhere")], prefix=""))],
        frame=typed(),
    )
    assert set(engine._cols) == {"absent_elsewhere"}


# --- Chained engine state ---------------------------------------------------


def test_a_collision_is_detected_across_separately_chained_calls():
    engine = engine_with([("is_between", between(["discount_pct"], 15.0))])
    with pytest.raises(DataQualityProcessingError) as exc:
        engine.is_between(between(["discount_pct"], 40.0))
    assert exc.value.api_error is DataQualityErrorCode.DQPE_17


def test_a_rejected_classifier_leaves_the_engine_state_untouched():
    engine = engine_with([("is_between", between([("discount_pct", "d15")], 15.0))])
    with pytest.raises(DataQualityProcessingError):
        engine.is_between(between([("amount", "d15")], 40.0))
    assert set(engine._cols) == {"d15"}
    assert engine._cols["d15"].column == "discount_pct"


# --- Full actions: many classifiers and chained operators -------------------


def compute(declared):
    return DataQualityProcessor.compute([orders()], declared)


def harvest(declared, operator):
    df, schema, cols, tags = compute(declared)
    return DataQualityProcessor.harvest(df, schema, cols, tags, None, operator)


def test_a_full_action_keeps_every_verdict_column_across_four_operators():
    declared = action(
        [
            between([("discount_pct", "disc_within_15")], 15.0, tags=["strict"]),
            between([("discount_pct", "disc_within_40")], 40.0, tags=["lenient"]),
            dq.IsPositive(["amount"], tags=["strict"], prefix=""),
            dq.IsIn(["region"], values={"EU", "US"}, tags=["lenient"], prefix=""),
            dq.ScaleCategorizer(
                [("amount", "amount_band")],
                scale=dq.LinearScale(0.0, 100.0, bins=4),
                prefix="",
            ),
        ],
        [
            dq.Enrich(),
            dq.Summary(to_table="orders_summary"),
            dq.Filter(
                dq.AllOk(tags=["strict"]),
                to_table="orders_bad",
                include_quality_columns="all",
            ),
            dq.Select(
                dq.AnyFailed(tags=["lenient"]),
                to_table="orders_lenient_fail",
                include_quality_columns="criteria",
            ),
        ],
    )
    df, _schema, cols, tags = compute(declared)
    assert set(cols) == {
        "disc_within_15",
        "disc_within_40",
        "amount_is_positive",
        "region_is_in",
        "amount_band",
    }
    assert set(tags["strict"]) == {"disc_within_15", "amount_is_positive"}
    assert set(tags["lenient"]) == {"disc_within_40", "region_is_in"}
    columns = df.derived_tf._lf.collect().columns
    for column in cols:
        assert column in columns


def test_both_acceptance_bands_survive_and_disagree_where_they_should():
    declared = action(
        [
            between([("discount_pct", "disc_within_15")], 15.0),
            between([("discount_pct", "disc_within_40")], 40.0),
        ],
        [dq.Enrich()],
    )
    df, _schema, _cols, _tags = compute(declared)
    result = df.derived_tf._lf.collect()
    assert result["disc_within_15"].to_list() == [
        ByteSentinel.TRUE,
        ByteSentinel.FALSE,
        ByteSentinel.FALSE,
        ByteSentinel.TRUE,
    ]
    assert result["disc_within_40"].to_list() == [
        ByteSentinel.TRUE,
        ByteSentinel.TRUE,
        ByteSentinel.FALSE,
        ByteSentinel.TRUE,
    ]


def test_the_summary_reports_one_row_per_declared_classifier():
    summary_operator = dq.Summary(to_table="orders_summary")
    declared = action(
        [
            between([("discount_pct", "disc_within_15")], 15.0),
            between([("discount_pct", "disc_within_40")], 40.0),
            dq.IsPositive(["amount"], prefix=""),
        ],
        [summary_operator],
    )
    _df, _schema, cols, _tags = compute(declared)
    result = harvest(declared, summary_operator)
    summary = (result.derived or result.output).derived_tf._lf.collect()
    assert len(summary) == len(cols) == 3
    assert set(summary["dq.classifier"].to_list()) == set(cols)


def test_a_collision_inside_a_full_action_fails_the_whole_action():
    declared = dq.DataQuality(
        table="orders",
        classifiers=[
            between([("discount_pct", "verdict")], 15.0),
            dq.IsPositive([("amount", "amount_ok")], prefix=""),
        ],
        operators=[dq.Enrich(), dq.Summary(to_table="orders_summary")],
    )
    object.__setattr__(
        declared.classifiers[1],
        "column_names",
        [("amount", "verdict")],
    )
    with pytest.raises(DataQualityProcessingError) as exc:
        compute(declared)
    assert exc.value.api_error is DataQualityErrorCode.DQPE_17


def test_a_fail_operator_sees_the_strict_band_that_used_to_be_lost():
    strict_only = dq.Fail(dq.AnyFailed(tags=["strict"]), dq.RowCountThreshold(0))
    declared = action(
        [
            between([("discount_pct", "disc_within_15")], 15.0, tags=["strict"]),
            between([("discount_pct", "disc_within_40")], 40.0, tags=["lenient"]),
        ],
        [strict_only],
    )
    with pytest.raises(DataQualityProcessingError) as exc:
        harvest(declared, strict_only)
    assert exc.value.api_error is DataQualityErrorCode.DQPE_14


def test_a_fail_operator_passes_when_only_the_lenient_band_is_checked():
    lenient_only = dq.Fail(dq.AnyFailed(tags=["lenient"]), dq.RowCountThreshold(1))
    declared = action(
        [
            between([("discount_pct", "disc_within_15")], 15.0, tags=["strict"]),
            between([("discount_pct", "disc_within_40")], 40.0, tags=["lenient"]),
        ],
        [lenient_only],
    )
    assert harvest(declared, lenient_only) is not None


def test_deserializing_an_action_revalidates_the_verdict_columns():
    declared = action([
        between([("discount_pct", "disc_within_15")], 15.0),
        between([("discount_pct", "disc_within_40")], 40.0),
    ])
    payload = declared.to_json().replace("disc_within_40", "disc_within_15")
    with pytest.raises(ParameterValidationError) as exc:
        dq.DataQuality.from_json(payload)
    assert exc.value.api_error is DataQualityErrorCode.DQC_202


# --- Classifying a verdict column -------------------------------------------


def test_classifying_an_aliased_verdict_column_is_rejected_at_registration():
    with pytest.raises(ParameterValidationError) as exc:
        action([
            between([("discount_pct", "disc_flag")], 15.0, prefix=""),
            dq.IsTrue(["disc_flag"], prefix=""),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQC_205
    assert "discount_pct" in str(exc.value)


def test_classifying_a_verdict_column_is_rejected_whatever_the_order():
    with pytest.raises(ParameterValidationError) as exc:
        action([
            dq.IsTrue(["disc_flag"], prefix=""),
            between([("discount_pct", "disc_flag")], 15.0, prefix=""),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQC_205


def test_consuming_a_verdict_column_as_an_aliased_source_is_rejected():
    with pytest.raises(ParameterValidationError) as exc:
        action([
            between([("discount_pct", "disc_flag")], 15.0, prefix=""),
            dq.IsTrue([("disc_flag", "flag_ok")], prefix=""),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQC_205


def test_a_verdict_column_nobody_classifies_is_allowed():
    declared = action([
        between([("discount_pct", "disc_flag")], 15.0),
        between([("amount", "amount_flag")], 40.0),
    ])
    assert len(declared.classifiers) == 2


def test_two_classifiers_on_one_source_with_distinct_aliases_are_allowed():
    declared = action([
        between([("discount_pct", "disc_within_15")], 15.0),
        between([("discount_pct", "disc_within_40")], 40.0),
    ])
    assert len(declared.classifiers) == 2


def test_classifying_an_inferred_verdict_column_fails_at_run_time():
    declared = action([
        between(["discount_pct"], 15.0),
        dq.IsTrue(["discount_pct_is_between"], on_missing_column="fail", prefix=""),
    ])
    with pytest.raises(DataQualityProcessingError) as exc:
        compute(declared)
    assert exc.value.api_error is DataQualityErrorCode.DQPE_11


def test_a_wildcard_classifier_never_sweeps_in_a_verdict_column():
    engine = engine_with([
        ("is_between", between(["discount_pct"], 15.0)),
        ("is_positive", dq.IsPositive(None, prefix="")),
    ])
    assert set(engine._cols) == {
        "discount_pct_is_between",
        "order_id_is_positive",
        "discount_pct_is_positive",
        "amount_is_positive",
    }


def test_classifiers_all_read_the_table_as_it_was_before_the_action():
    engine = engine_with([("is_between", between(["discount_pct"], 15.0))])
    assert "discount_pct_is_between" in engine.tf().schema.names()
    assert "discount_pct_is_between" not in engine._schema.names()


# --- Prefixes: the three supported values -----------------------------------


def verdict_names(*classifiers, frame=None):
    engine = DataQualityEngine(frame if frame is not None else orders())
    names = []
    for classifier in classifiers:
        metadata = CLASSIFIER_DICTIONARY[type(classifier)]
        for mapped in engine._cols_spec_to_cols_list(
            classifier, classifier.column_names
        ):
            names.append(
                engine._name(
                    metadata.column_suffix,
                    mapped.data_column,
                    mapped.dq_column,
                    classifier.prefix,
                )
            )
    return names


def test_an_omitted_prefix_namespaces_an_inferred_name():
    assert verdict_names(dq.IsBetween(["discount_pct"], min_val=0.0, max_val=15.0)) == [
        "td.dq.discount_pct_is_between"
    ]


def test_an_omitted_prefix_namespaces_an_explicit_alias():
    assert verdict_names(
        dq.IsBetween([("discount_pct", "disc_ok")], min_val=0.0, max_val=15.0)
    ) == ["td.dq.disc_ok"]


def test_an_omitted_prefix_namespaces_every_wildcard_column():
    assert verdict_names(dq.IsPositive(None)) == [
        "td.dq.amount_is_positive",
        "td.dq.discount_pct_is_positive",
        "td.dq.order_id_is_positive",
    ]


def test_an_empty_prefix_leaves_an_inferred_name_bare():
    assert verdict_names(between(["discount_pct"], 15.0)) == ["discount_pct_is_between"]


def test_an_empty_prefix_leaves_an_explicit_alias_bare():
    assert verdict_names(between([("discount_pct", "disc_ok")], 15.0)) == ["disc_ok"]


def test_a_custom_prefix_is_prepended_verbatim_to_an_inferred_name():
    assert verdict_names(between(["discount_pct"], 15.0, prefix="strict.")) == [
        "strict.discount_pct_is_between"
    ]


def test_a_custom_prefix_is_prepended_verbatim_to_an_explicit_alias():
    assert verdict_names(
        between([("discount_pct", "disc_ok")], 15.0, prefix="strict.")
    ) == ["strict.disc_ok"]


@pytest.mark.parametrize(
    "prefix", ["strict.", "strict_", "STRICT.", "a.b.c.", "x", "0.", "-", "@td.dq."]
)
def test_a_custom_prefix_is_not_interpreted(prefix: str):
    assert verdict_names(between([("discount_pct", "v")], 15.0, prefix=prefix)) == [
        f"{prefix}v"
    ]


def test_a_custom_prefix_applies_to_every_wildcard_column():
    assert verdict_names(between(None, 15.0, prefix="s.")) == [
        "s.amount_is_between",
        "s.discount_pct_is_between",
        "s.order_id_is_between",
        "s.region_is_between",
    ]


def test_prefixes_are_per_classifier():
    assert verdict_names(
        between([("discount_pct", "v")], 15.0, prefix="s."),
        between([("amount", "v")], 40.0, prefix="l."),
        dq.IsBetween([("region", "v")], min_val=0.0, max_val=90.0),
    ) == ["s.v", "l.v", "td.dq.v"]


# --- Prefixes and collision detection ---------------------------------------


def test_different_prefixes_resolve_a_shared_alias():
    declared = action([
        between([("discount_pct", "v")], 15.0, prefix="strict."),
        between([("discount_pct", "v")], 40.0, prefix="lenient."),
    ])
    assert len(declared.classifiers) == 2


def test_different_prefixes_resolve_a_shared_inferred_name():
    declared = action([
        between(["discount_pct"], 15.0, prefix="strict."),
        between(["discount_pct"], 40.0, prefix="lenient."),
    ])
    assert len(declared.classifiers) == 2


def test_the_same_prefix_does_not_hide_a_shared_alias():
    with pytest.raises(ParameterValidationError) as exc:
        action([
            between([("discount_pct", "v")], 15.0, prefix="strict."),
            between([("discount_pct", "v")], 40.0, prefix="strict."),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQC_202
    assert "'strict.v'" in str(exc.value)


def test_the_same_prefix_does_not_hide_a_shared_inferred_name():
    with pytest.raises(ParameterValidationError) as exc:
        action([
            between(["discount_pct"], 15.0, prefix="strict."),
            between(["discount_pct"], 40.0, prefix="strict."),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQC_202
    assert "strict." in str(exc.value)


def test_a_prefix_can_make_two_distinct_aliases_collide():
    with pytest.raises(ParameterValidationError) as exc:
        action([
            between([("discount_pct", "b.v")], 15.0, prefix="a."),
            between([("amount", "v")], 40.0, prefix="a.b."),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQC_202
    assert "'a.b.v'" in str(exc.value)


def test_the_default_prefix_protects_an_alias_that_shadows_a_user_column():
    declared = action([between([("discount_pct", "order_id")], 15.0, prefix=None)])
    assert len(declared.classifiers) == 1
    assert verdict_names(declared.classifiers[0]) == ["td.dq.order_id"]


def test_two_wildcards_of_one_type_are_separated_by_their_prefixes():
    engine = engine_with([
        ("is_between", between(None, 15.0, prefix="strict.")),
        ("is_between", between(None, 40.0, prefix="lenient.")),
    ])
    assert len(engine._cols) == 8
    assert engine._cols["strict.amount_is_between"].column == "amount"
    assert engine._cols["lenient.amount_is_between"].column == "amount"


def test_two_wildcards_sharing_a_prefix_still_collide_at_run_time():
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([
            ("is_between", between(None, 15.0, prefix="strict.")),
            ("is_between", between(None, 40.0, prefix="strict.")),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQPE_17
    assert "strict." in str(exc.value)


def test_a_prefixed_verdict_column_overwriting_a_user_column_is_rejected():
    frame = tableframe_of(pl.LazyFrame({"a": [1.0], "p.a_is_between": [2.0]}))
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([("is_between", between(["a"], 15.0, prefix="p."))], frame=frame)
    assert exc.value.api_error is DataQualityErrorCode.DQPE_18


@pytest.mark.parametrize("prefix", ["$td.", "$td.ver."])
def test_a_prefix_in_the_reserved_namespace_is_rejected(prefix: str):
    with pytest.raises(ParameterValidationError) as exc:
        action([between(["discount_pct"], 15.0, prefix=prefix)])
    assert exc.value.api_error is DataQualityErrorCode.DQC_203


@pytest.mark.parametrize("prefix", ["@td.", "@td.dq.", "@td.origin."])
def test_a_prefix_in_the_connector_namespace_is_allowed(prefix: str):
    assert verdict_names(between(["discount_pct"], 15.0, prefix=prefix)) == [
        f"{prefix}discount_pct_is_between"
    ]


def test_a_doubled_connector_namespace_is_allowed():
    assert verdict_names(
        between([("discount_pct", "@td.dq.a")], 15.0, prefix="@td.dq.")
    ) == ["@td.dq.@td.dq.a"]


def test_prefixes_carry_through_a_full_action():
    declared = action(
        [
            between([("discount_pct", "band")], 15.0, prefix="strict."),
            between([("discount_pct", "band")], 40.0, prefix="lenient."),
        ],
        [dq.Enrich()],
    )
    df, _schema, cols, _tags = compute(declared)
    assert set(cols) == {"strict.band", "lenient.band"}
    result = df.derived_tf._lf.collect()
    assert result["strict.band"].to_list() == [
        ByteSentinel.TRUE,
        ByteSentinel.FALSE,
        ByteSentinel.FALSE,
        ByteSentinel.TRUE,
    ]
    assert result["lenient.band"].to_list() == [
        ByteSentinel.TRUE,
        ByteSentinel.TRUE,
        ByteSentinel.FALSE,
        ByteSentinel.TRUE,
    ]


def test_a_prefix_survives_a_round_trip():
    declared = action([between(["discount_pct"], 15.0, prefix="strict.")])
    back = dq.DataQuality.from_json(declared.to_json())
    assert back.classifiers[0].prefix == "strict."
    assert back == declared


def test_an_empty_prefix_survives_a_round_trip():
    declared = action([between(["discount_pct"], 15.0, prefix="")])
    assert dq.DataQuality.from_json(declared.to_json()).classifiers[0].prefix == ""
